import { trace, SpanKind, SpanStatusCode } from "@opentelemetry/api";
import type { AxiosError, AxiosResponse } from "axios";
import type * as ClientTypes from "../types.js";
import authenticate from "./authenticate.js";
import Request from "../../request/index.js";
import type BaseClient from "../index.js";
import type {
  RequestConfig,
  RequestDoneData,
  RequestMetadata,
  RequestRetryData,
} from "../../request/types.js";
import {
  redactBody,
  redactConfig,
  redactHeaders,
  redactUrl,
  sanitizeError,
} from "../../utils/redact.js";
import {
  NoResponseError,
  RequestAbortedError,
  RequestCostExceedsBudgetError,
  RequestTimeoutError,
} from "../../errors.js";

const tracer = trace.getTracer("@dianemo/core");

/**
 * How long to spend announcing an abandonment. Short, because the caller is already
 * being failed — but still awaited, so a caller waits its own budget plus this one.
 * That is deliberate: the announcement releases the queue entry and any slot, so
 * doing it first keeps a retry from racing its predecessor's cleanup.
 */
const ABANDONMENT_PUBLISH_TIMEOUT_MS = 2000;

async function handleRequest(this: BaseClient, config: RequestConfig) {
  return tracer.startActiveSpan(
    `dianemo ${config.requestName}`,
    {
      kind: SpanKind.CLIENT,
      attributes: {
        "dianemo.client_name": this.name,
        "dianemo.request_name": config.requestName,
        "dianemo.priority": config.priority ?? 1,
        "dianemo.cost": config.cost ?? 1,
        ...(config.grantId ? { "dianemo.grant_id": config.grantId } : {}),
      },
    },
    async (span) => {
      // A cost above the ceiling can never be admitted, so reject it here — to
      // the caller — rather than letting it occupy the queue until it times out
      // on whichever replica happens to be the controller.
      await this.assertReadyToQueue();
      // Constructed before the ceiling test, because its constructor validates the
      // shape of `cost`, `priority` and `grantId`. Testing the ceiling first makes
      // the error a caller sees depend on whether their malformed value also
      // exceeded the budget, reporting `cost: Infinity` as "budget too small".
      const request = new Request(
        this.name,
        config,
        this.instanceId,
        this.requestOptions
      );
      const ceiling = this.getCostCeiling();
      if (ceiling !== undefined && (config.cost ?? 1) > ceiling) {
        throw new RequestCostExceedsBudgetError(
          this.name,
          config.cost ?? 1,
          ceiling
        );
      }
      span.setAttribute("dianemo.request_id", request.id);
      this.logger.debug(`Request ID: ${request.id} | Waiting...`);
      // Counted for the shutdown drain, which otherwise cannot see a fast-path
      // request — it has no queue entry, but it may hold a concurrency slot.
      this.trackRequestStarted();
      try {
        do {
          request.setStatus("inQueue");
          // Everything in here runs after admission has already claimed a slot
          // or spent tokens, so a throw has to hand those back explicitly.
          try {
            await handlePreRequest.bind(this)(request);
          } catch (error) {
            await releaseAbandonedRequest.bind(this)(request, error);
            throw error;
          }
          let res;
          try {
            res = await this.http.request(request.config);
          } catch (error: unknown) {
            // handleError throws when retry is exhausted; otherwise returns
            // retryData and we loop. The catch below records it on the span.
            const axiosErr = error as AxiosError;
            const retryData = await handleError.bind(this)(request, axiosErr);
            const status = axiosErr.response?.status;
            if (retryData.isRateLimited) {
              span.addEvent("rate_limited", {
                "dianemo.wait_ms": retryData.waitTime,
                ...(status ? { "http.response.status_code": status } : {}),
              });
            }
            // Record per-attempt exception details so deep retry chains are
            // debuggable in a trace viewer without paging through logs.
            span.addEvent("retry", {
              "dianemo.attempt": request.retries,
              "exception.type": axiosErr.name || axiosErr.code || "Error",
              "exception.message": axiosErr.message,
              ...(status ? { "http.response.status_code": status } : {}),
            });
            // Always wait, even with a client-wide freeze on its way: that freeze
            // is applied by the controller after a pub/sub round trip while this
            // loop re-enters admission immediately, so deferring to it sends the
            // request the vendor just rejected straight back out. The freeze is the
            // fleet-wide gate; this is the local one.
            if (retryData.waitTime > 0) {
              await new Promise((resolve) =>
                setTimeout(resolve, retryData.waitTime)
              );
            }
            continue;
          }
          return await handleResponse.bind(this)(request, res);
        } while (request.retries <= this.retryOptions.maxRetries);
        throw new NoResponseError();
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        span.recordException(e);
        span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
        const axiosStatus = (err as AxiosError)?.response?.status;
        if (axiosStatus)
          span.setAttribute("http.response.status_code", axiosStatus);
        throw err;
      } finally {
        this.trackRequestFinished();
        span.setAttribute("dianemo.retries", request.retries);
        span.end();
      }
    }
  );
}

/** Hands back everything admission granted, for a request that will not run. */
async function releaseAbandonedRequest(
  this: BaseClient,
  request: Request,
  cause: unknown
) {
  // The admission timeout and the abort path already announced this one. Both
  // reject, and the rejection lands in the catch that calls this — so without
  // the guard every such request emits `requestDone` twice on a public channel.
  if (request.abandonmentPublished) return;
  request.abandonmentPublished = true;
  const metadata = request.getMetadata();
  // `inProgress` is the record that admission completed, and only then is there
  // anything to give back — refunding for a request that never reached admission
  // would mint budget. The announcement below covers a concurrency slot but not a
  // token bucket, whose `handleOwnTypeRequestDone` has nothing to release.
  if (metadata.status === "inProgress") {
    await this.releaseUnusedAdmission(metadata).catch((error) => {
      this.logger.error(
        { error },
        `Request ID: ${request.id} | failed to return admission after a pre-request failure`
      );
    });
  }
  const message = cause instanceof Error ? cause.message : String(cause);
  await publishAbandoned.bind(this)(
    request,
    `Request abandoned before dispatch: ${message}`
  );
}

/**
 * Announces that a request will not run, so the controller releases whatever
 * admission granted it.
 *
 * The controller owns the queue entry and the concurrency slot and releases both
 * only in response to this event — so abandoning a request means announcing it,
 * not just dropping it.
 */
async function publishAbandoned(
  this: BaseClient,
  request: Request,
  message: string
) {
  const data = request.getRequestDoneData({
    retry: false,
    message,
    isRateLimited: false,
    waitTime: 0,
    freezeClient: false,
  });
  // Cleanup is local and retryable; pub/sub below is notification, not the
  // mechanism that makes queue/slot state correct.
  void this.finalizeOwnedRequest(data).catch(() => undefined);
  try {
    // Bounded, because this runs on the path that has ALREADY decided to give
    // up. An unbounded await here held the caller's `RequestTimeoutError` behind
    // a publish that never settled — so the admission deadline fired correctly
    // and then could not escape, which is the whole point of having one. A Redis
    // that is up but not answering (saturated, swapping, paused) leaves the
    // command in ioredis's queue indefinitely rather than rejecting it, so only
    // a deadline gets the caller its answer.
    await withDeadline(
      this.backend.publish(
        `${this.handlerNamespace}:requestDone`,
        JSON.stringify(data)
      ),
      ABANDONMENT_PUBLISH_TIMEOUT_MS,
      request.id
    );
  } catch (err) {
    this.logger.error(
      { error: err },
      `Failed to announce abandoned request ${request.id}; its retained completion will be republished after role reconciliation`
    );
  }
}

async function handlePreRequest(this: BaseClient, request: Request) {
  await waitForRequestReady.bind(this)(request);
  if (this.requestOptions.requestInterceptor) {
    request.config = await this.requestOptions.requestInterceptor(
      request.config
    );
  }
  const authHeader = await authenticate.bind(this)(request);
  if (authHeader) {
    request.config = {
      ...request.config,
      headers: { ...request.config.headers, ...authHeader },
    };
  }
  const { method, baseURL, url } = request.config;
  // Some vendors want their key in the query string.
  this.logger.debug(
    `Request ID: ${request.id} | ${method} | ${redactUrl(baseURL) || ""}${
      redactUrl(url) || ""
    }${request.retries ? ` | Retry Attempt: ${request.retries}` : ""}`
  );
}

/**
 * Rejects with `RequestTimeoutError` if a backend call outlives the admission
 * budget, so a flapping backend cannot hold a caller forever.
 *
 * The underlying promise is left to settle on its own — there is no way to cancel
 * an in-flight Redis command, and abandoning it is what the caller wanted.
 */
async function withDeadline<T>(
  work: Promise<T>,
  timeoutMs: number,
  requestId: string
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new RequestTimeoutError(requestId, timeoutMs)),
          timeoutMs
        );
        timer.unref?.();
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function waitForRequestReady(this: BaseClient, request: Request) {
  const timeoutMs = this.requestOptions.cleanupTimeout || 60000;
  const metadata = request.getMetadata();

  // A retry re-enters on the same Request object and `fastPath` describes how
  // *this* attempt was admitted, so it must be cleared first. A stale `true` makes
  // `handleRequestDone` skip the queue removal, stranding an entry that keeps the
  // queue non-empty — which disables the fast path for good.
  request.fastPath = false;

  // Checked before the fast path, not after: the fast path spends, and a caller who
  // has already given up must not be charged a token or a slot for a request axios
  // will then refuse to send.
  const signal = request.config.signal;
  if (signal?.aborted) {
    throw new RequestAbortedError(request.id);
  }

  // With nothing contending the queue has no ordering to do, so the enqueue, the
  // admission pass, both broadcasts and the removal are avoidable. Atomic, so it
  // cannot overtake queued work or double-spend.
  //
  // Deadlined like the wait that follows: this is the first backend call a request
  // makes, and ioredis resets its retry counter on every successful reconnect, so
  // against a Redis that accepts connections and drops them a queued command is
  // never rejected and the caller would wait forever.
  if (
    await withDeadline(
      this.tryAdmitImmediately(metadata),
      timeoutMs,
      request.id
    )
  ) {
    if (signal?.aborted) {
      void this.releaseUnusedAdmission(metadata).catch((error) => {
        this.logger.error(
          { error },
          `Request ID: ${request.id} | failed to return admission after abort`
        );
      });
      throw new RequestAbortedError(request.id);
    }
    request.fastPath = true;
    request.setStatus("inProgress");
    return true;
  }

  // Re-read, because the awaited call above may have spanned the abort. An
  // already-aborted request must not take a place in the queue: it would be
  // admitted in turn, spend, then fail inside axios with real traffic behind it.
  if (signal?.aborted) {
    throw new RequestAbortedError(request.id);
  }

  const readyPromise = new Promise<boolean>((resolve, reject) => {
    // Lets shutdown fail this request with a reason, and clears the timer so
    // the event loop can drain.
    const cancel = (error: Error) => {
      clearTimeout(timeout);
      signal?.removeEventListener?.("abort", onAbort);
      this.emitter.off(`requestReady:${request.id}`, onReady);
      reject(error);
    };

    // Aborting while queued releases the place rather than holding it until the
    // budget arrives.
    const onAbort = () => {
      clearTimeout(timeout);
      this.unregisterWaitingRequest(request.id);
      this.emitter.off(`requestReady:${request.id}`, onReady);
      // Not awaited: an abort must return control promptly, and awaiting would
      // block up to ABANDONMENT_PUBLISH_TIMEOUT_MS on a wedged backend for a caller
      // who has stopped waiting. A request aborted while queued holds no slot, so
      // only the queue entry is released.
      //
      // The trailing catch is load-bearing: `publishAbandoned` swallows failures by
      // logging, and the logger is consumer code, so a throw from inside its catch
      // would reject this floating promise with nothing attached.
      void publishAbandoned
        .bind(this)(request, "Request aborted while queued")
        .catch(() => undefined);
      // Same reason as the timeout branch: this rejection lands in the catch
      // that calls `releaseAbandonedRequest`, which would announce it again.
      request.abandonmentPublished = true;
      reject(new RequestAbortedError(request.id));
    };

    const timeout = setTimeout(() => {
      this.unregisterWaitingRequest(request.id);
      signal?.removeEventListener?.("abort", onAbort);
      this.emitter.off(`requestReady:${request.id}`, onReady);
      // Same reasoning as the abort branch for the trailing catch: the logger
      // inside `publishAbandoned`'s own catch is consumer code, and a throw there
      // would leave this floating promise rejected with nothing attached.
      void publishAbandoned
        .bind(this)(request, "Request timed out waiting for admission")
        .catch(() => undefined);
      // Marked so the catch in `handlePreRequest` does not announce the same
      // abandonment a second time — `requestDone` is a public channel, and a
      // consumer metering completions would double-count every timeout.
      request.abandonmentPublished = true;
      reject(new RequestTimeoutError(request.id, timeoutMs));
    }, timeoutMs);

    const onReady = (ready: RequestMetadata) => {
      clearTimeout(timeout);
      signal?.removeEventListener?.("abort", onAbort);
      this.unregisterWaitingRequest(request.id);
      request.isThawRequest = ready.isThawRequest;
      request.setStatus("inProgress");
      resolve(true);
    };

    // The `?.` on the method is unreachable — the `Request` constructor rejects a
    // signal without these two — and kept deliberately, so this code does not
    // depend on that check for a signal to get abort handling at all.
    signal?.addEventListener?.("abort", onAbort, { once: true });
    this.emitter.once(`requestReady:${request.id}`, onReady);
    this.registerWaitingRequest(request.id, cancel);
  });

  // Nothing is attached to `readyPromise` until this function returns it, and the
  // enqueue below is awaited in between, so an abort landing in that window would
  // reject a promise with no handler — which under Node's default
  // `--unhandled-rejections=throw` terminates the host process. A sink rather than
  // registering the listener after the add, which would silently ignore an abort in
  // the same window; the promise returned below still rejects for the caller.
  void readyPromise.catch(() => undefined);

  // A retry re-enters with the same id and keeps its metadata. The add is
  // idempotent in the backend, so it needs no existence check first. Deadlined
  // for the reason given above the fast-path probe, and because nothing else
  // bounds it: `readyPromise`'s own timer cannot answer the caller while this
  // await still holds the function.
  const created = await withDeadline(
    this.addRequestToQueue({
      ...metadata,
      status: "pending",
    }),
    timeoutMs,
    request.id
  );

  // An idempotent add leaves an existing entry untouched, so a retry whose entry
  // survived the previous attempt has to be made claimable explicitly — here
  // rather than in `handleError`, so it lands after the listener above exists.
  //
  // Only when it already existed: a retry of a fast-path attempt has no entry, so
  // the add above created one already `pending`, and flipping it again would
  // un-claim a request the drain loop has since claimed, letting the next pass
  // spend a second token on it.
  if (request.retries > 0 && !created) {
    await this.updateRequestInQueue(request.id, { status: "pending" });
    await this.notifyRequestAdded();
  }

  return readyPromise;
}

/**
 * Announces a completion without letting the announcement change the outcome: the
 * vendor has already acted, so a backend hiccup must not fail a served request for a
 * caller who would then retry a non-idempotent operation, nor replace the vendor's
 * status. Cleanup is retained and retried by the request owner regardless.
 */
async function publishRequestDone(
  this: BaseClient,
  request: Request,
  payload: string
): Promise<void> {
  const data = JSON.parse(payload) as RequestDoneData;
  const attempts = 3;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      await this.backend.publish(
        `${this.handlerNamespace}:requestDone`,
        payload
      );
      break;
    } catch (error) {
      if (attempt === attempts) {
        this.logger.error(
          { error },
          `Client ${this.name} | could not announce completion of request ${request.id} after ${attempts} attempts; the retained completion will be republished after role reconciliation`
        );
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 50 * attempt));
    }
  }
  // Publish first so peers retain a fallback before the owner can disappear
  // mid-cleanup. Cleanup remains asynchronous so retry latency is governed by
  // the configured back-off, not by a slow freeze write; controller shutdown
  // observes the retained pending record and drains it before closing.
  void this.finalizeOwnedRequest(data).catch(() => undefined);
}

async function handleResponse(
  this: BaseClient,
  request: Request,
  res: AxiosResponse
) {
  await publishRequestDone.bind(this)(
    request,
    JSON.stringify(request.getRequestDoneData(undefined, res.status))
  );
  if (this.requestOptions?.responseInterceptor) {
    try {
      await this.requestOptions.responseInterceptor(request.config, res);
    } catch (error) {
      this.logger.error(
        { error },
        `Request ID: ${request.id} | responseInterceptor failed after the upstream request succeeded`
      );
    }
  }
  if (this.rateLimitChange) {
    try {
      const change = this.rateLimitChange as ClientTypes.RateLimitChange;
      const newLimit = await change(this.rateLimit, res);
      if (newLimit) await this.updateRateLimit(newLimit, "dynamic");
    } catch (error) {
      this.logger.error(
        { error },
        `Request ID: ${request.id} | rateLimitChange failed after the upstream request succeeded`
      );
    }
  }
  this.logger.debug(`Request ID: ${request.id} | Status: ${res.status}`);
  return res;
}

async function handleError(
  this: BaseClient,
  request: Request,
  res: AxiosError
) {
  // Sanitize before anything else touches the error. `retryHandler` is consumer
  // code and the likeliest place a credential gets logged, and it runs from
  // `handleRetry` below — long before the throw at the end of this function.
  sanitizeError(res, this.getSensitiveHeaderNames());

  const retryData = await handleRetry.bind(this)(request, res);
  handleLogError.bind(this)(request, res, retryData);

  if (retryData.retry) {
    // Only the retry count, which is what re-scores the entry. Deliberately not
    // `status: "pending"` — that would make the entry claimable before the owner
    // has re-installed its `requestReady` listener, and an admission landing in
    // that window emits into an EventEmitter nobody is listening to.
    // `waitForRequestReady` flips it to pending once the listener exists.
    await this.updateRequestInQueue(request.id, {
      retries: request.retries,
    });
  }

  // The controller handles freeze and concurrency release from this event, and
  // leaves a retry in the queue — it distinguishes them by `responseStatus`.
  await publishRequestDone.bind(this)(
    request,
    JSON.stringify(request.getRequestDoneData(retryData, res.response?.status))
  );

  if (retryData.retry) return retryData;

  throw res;
}

async function handleRetry(
  this: BaseClient,
  request: Request,
  error: AxiosError
) {
  const { retry429s, retry5xxs, retryStatusCodes } = this.retryOptions;
  const retryableCodes = ["ECONNRESET", "ETIMEDOUT", "ECONNABORTED"];
  const status = error?.response?.status;
  const data: RequestRetryData = {
    retry: true,
    message: "",
    isRateLimited: false,
    waitTime: 0,
    freezeClient: false,
  };
  // Classify the failure before deciding whether to retry. The freeze is a
  // fleet-wide statement about the upstream, and a 429 says the same thing on
  // the last permitted attempt as on the first — checking the retry budget
  // first meant `maxRetries: 0` never armed the back-off for anyone.
  const outOfRetries = request.retries >= this.retryOptions.maxRetries;

  if (status && status === 429 && retry429s) {
    data.message += "Rate Limited";
    data.isRateLimited = true;
    data.freezeClient = true;
  } else if (status && status >= 500 && retry5xxs) {
    data.message += "Server Error";
    data.freezeClient = true;
  } else if (status && retryStatusCodes.includes(status)) {
    data.message += "Client Wants Retry By Status";
  } else if (error.code && retryableCodes.includes(error.code)) {
    data.message += "Server Error";
    data.freezeClient = true;
  } else if (!outOfRetries && this.retryOptions?.retryHandler) {
    // This runs before the error is logged and before `requestDone` is
    // published, so a throw here would strand the slot and lose the upstream
    // failure. A broken retry policy means "do not retry", not "abort".
    try {
      const retry = await this.retryOptions.retryHandler(error);
      if (retry) data.message += "Client Wants Retry";
      else data.retry = false;
    } catch (handlerError) {
      this.logger.error(
        { error: handlerError },
        `Client ${this.name} | retryHandler threw; treating the request as non-retryable`
      );
      data.message += "Retry handler failed";
      data.retry = false;
    }
  } else data.retry = false;

  if (outOfRetries) {
    data.message += data.message
      ? " | Maximum number of retries reached."
      : "Maximum number of retries reached.";
    data.retry = false;
  }

  if (!data.retry) {
    // A freeze still needs a duration even though this request is finished
    // retrying: the freeze is about the upstream, not about this attempt, and
    // `handleRequestDone` ignores a freeze with no wait time — so the last
    // attempt's 429 armed nothing at all.
    if (data.freezeClient) handleBackoff.bind(this)(request, data);
    return data;
  }
  request.incrementRetries();
  return handleBackoff.bind(this)(request, data);
}

/** How long to wait before the next attempt. */
function handleBackoff(
  this: BaseClient,
  request: Request,
  data: RequestRetryData
) {
  const power = this.retryOptions.retryBackoffMethod === "exponential" ? 2 : 1;
  // At least one attempt's worth. `retries` is 0 on a first failure, and zero
  // times anything is a wait of zero — which reads as "no freeze" downstream.
  const attempt = Math.max(1, request.retries);
  const multiplier = Math.pow(attempt, power);
  data.waitTime = multiplier * this.getRetryBackoffBaseTime(data.isRateLimited);
  data.freezeTime = multiplier * this.getFreezeBaseTime();
  data.message += ` | Will retry...`;
  return data;
}

function handleLogError(
  this: BaseClient,
  request: Request,
  res: AxiosError,
  retryData: RequestRetryData
) {
  const status = res.response?.status;
  const shouldMute = status && this.httpStatusCodesToMute?.includes(status);
  const message = `Request ID: ${request.id} | Status: ${status} | Code: ${
    res.code
  }${retryData.message ? ` | ${retryData.message}` : ""}`;
  const extraHeaderNames = this.getSensitiveHeaderNames();
  const payload = {
    error: {
      message: res.message,
      stack: res.stack,
      code: res.code,
      config: redactConfig(res.config, extraHeaderNames),
      response: {
        status: res.response?.status,
        statusText: res.response?.statusText,
        headers: redactHeaders(
          res.response?.headers as Record<string, unknown> | undefined,
          extraHeaderNames
        ),
        data: redactBody(res.response?.data),
      },
    },
  };
  if (shouldMute) this.logger.debug(payload, message);
  else this.logger.error(payload, message);
}

export default handleRequest;
