import type { RequestOptions } from "../client/types.js";
import { ConfigurationError } from "../errors.js";
import type * as RequestTypes from "./types.js";
import crypto from "node:crypto";

/**
 * Renders a rejected value readably. `JSON.stringify(NaN)` is `"null"`, which
 * sends a caller looking for a null they never passed.
 */
function describe(value: unknown): string {
  if (typeof value === "number" && !Number.isFinite(value))
    return String(value);
  return JSON.stringify(value) ?? String(value);
}

/** Matches the priority band the backends clamp to when computing a queue score. */
const MAX_PRIORITY = 10;

/**
 * Rejects a per-request `priority`, `cost`, `grantId` or `signal` that cannot be
 * honoured, where it enters — so it cannot surface later as a different and wrong
 * diagnosis. A negative cost is the dangerous one: the ceiling guard is a `>` test,
 * so it sails through, and `tokens - cost` then *adds* tokens.
 */
function assertUsableRequest(
  priority: number,
  cost: number,
  grantId?: string,
  signal?: unknown
): void {
  // An empty string is falsy everywhere `grantId` is tested, so it quietly fell
  // back to the client's own credentials — the request went out under the wrong
  // identity with no error and no log line. `grantId: row.externalId ?? ""` is
  // an easy way to produce it.
  if (
    grantId !== undefined &&
    (typeof grantId !== "string" || !grantId.trim())
  ) {
    throw new ConfigurationError(
      "invalid_grant_id",
      `grantId must be a non-empty string when provided. Received ${JSON.stringify(
        grantId
      )}.`
    );
  }
  // `Number.EPSILON` is the floor, not zero: a positive value below it cannot be
  // subtracted from a balance of 1 or more in floating point — `tokens - cost`
  // returns `tokens` — so `cost: 5e-324` would spend nothing and buy a request.
  if (!Number.isFinite(cost) || cost < Number.EPSILON) {
    throw new ConfigurationError(
      "invalid_request_cost",
      `cost must be a finite number of at least ${Number.EPSILON}. Received ${describe(
        cost
      )}.`
    );
  }
  if (
    !Number.isFinite(priority) ||
    priority < 0 ||
    priority > MAX_PRIORITY ||
    !Number.isInteger(priority)
  ) {
    throw new ConfigurationError(
      "invalid_request_priority",
      `priority must be an integer between 0 and ${MAX_PRIORITY}. Received ${describe(
        priority
      )}.`
    );
  }
  // Axios types `signal` as `GenericAbortSignal`, whose `addEventListener` is
  // optional, but its Node adapter calls `addEventListener` and
  // `removeEventListener` unguarded — so a looser shape throws a `TypeError` out of
  // axios at dispatch, after this client has already spent a token or a slot on the
  // request, and `handleRetry` reports it as an unknown non-retryable failure.
  // Refused on arrival, where the diagnosis is still accurate.
  if (signal !== undefined && !isUsableSignal(signal)) {
    throw new ConfigurationError(
      "invalid_request_signal",
      `signal must support addEventListener and removeEventListener — axios's Node (http) adapter calls both unguarded, so a signal without them fails inside axios rather than cancelling anything. Pass an AbortController's signal. Received ${describe(
        signal
      )}.`
    );
  }
}

/** Whether both listener methods axios and the admission wait call are present. */
function isUsableSignal(signal: unknown): boolean {
  if (typeof signal !== "object" || signal === null) return false;
  const candidate = signal as {
    addEventListener?: unknown;
    removeEventListener?: unknown;
  };
  return (
    typeof candidate.addEventListener === "function" &&
    typeof candidate.removeEventListener === "function"
  );
}

export default class Request {
  public id: string;
  public config: RequestTypes.RequestConfig;
  public retries: number;
  private priority: number;
  private clientName: string;
  private timestamp: number;
  private cost: number;
  private explicitCost?: number;
  private requestName: string;
  private status: RequestTypes.RequestStatus = "inQueue";
  private ownerId: string;
  public isThawRequest: boolean = false;
  /** Admitted without queueing, so there is no queue entry to clean up. */
  public fastPath: boolean = false;
  /** Whether `requestDone` has already been emitted for an abandonment. */
  public abandonmentPublished: boolean = false;

  constructor(
    clientName: string,
    config: RequestTypes.RequestConfig,
    instanceId: string,
    requestOptions?: RequestOptions
  ) {
    this.id = crypto.randomUUID();
    this.timestamp = Date.now();
    // `??`, not `||`: 0 is the documented bottom of the priority range, and
    // coercing it to 1 put a backfill in the same band as default traffic.
    this.priority = config.priority ?? 1;
    this.cost = config.cost ?? 1;
    this.explicitCost = config.cost;
    assertUsableRequest(
      this.priority,
      this.cost,
      config.grantId,
      config.signal
    );
    this.requestName = config.requestName;
    this.retries = 0;
    this.config = { ...config, requestId: this.id };
    this.clientName = clientName;
    this.ownerId = instanceId;
    if (requestOptions?.defaults) {
      const { headers, baseURL, params } = requestOptions.defaults;
      this.config = {
        ...this.config,
        baseURL: this.config.baseURL || baseURL,
        headers: { ...(headers || {}), ...this.config.headers },
        params: { ...(params || {}), ...this.config.params },
      };
    }
  }

  incrementRetries() {
    this.retries++;
  }

  setStatus(status: RequestTypes.RequestStatus) {
    this.status = status;
  }

  getMetadata(): RequestTypes.RequestMetadata {
    return {
      requestId: this.id,
      status: this.status,
      clientName: this.clientName,
      timestamp: this.timestamp,
      priority: this.priority,
      cost: this.cost,
      explicitCost: this.explicitCost,
      requestName: this.requestName,
      retries: this.retries,
      grantId: this.config.grantId,
      isThawRequest: this.isThawRequest,
      fastPath: this.fastPath,
      ownerId: this.ownerId,
    };
  }

  getRequestDoneData(
    retryData?: RequestTypes.RequestRetryData,
    httpStatus?: number
  ): RequestTypes.RequestDoneData {
    let responseStatus: "success" | "failure" | "retry" = "success";
    if (retryData) responseStatus = retryData.retry ? "retry" : "failure";
    return {
      ...this.getMetadata(),
      responseStatus,
      waitTime: retryData?.waitTime || 0,
      freezeTime: retryData?.freezeTime,
      isRateLimited: retryData?.isRateLimited || false,
      freezeClient: retryData?.freezeClient || false,
      completedAt: Date.now(),
      httpStatus,
    };
  }
}
