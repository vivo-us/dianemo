import type { RequestDoneData, RequestMetadata } from "../../request/types.js";
import { ConfigurationError } from "../../errors.js";
import BaseClient from "../index.js";
import type {
  ClientConstructorData,
  ConcurrencyLimitClientOptions,
  RateLimitStats,
  RateLimitUpdatedData,
} from "../types.js";

/** N requests in flight at once. See docs/rate-limits/concurrency-limit.md. */
class ConcurrencyLimitClient extends BaseClient {
  protected rateLimit: ConcurrencyLimitClientOptions;
  /**
   * Crash recovery only, and deliberately not `cleanupTimeout` — the two want
   * opposite values. Too short reaps slots from requests still in flight and the cap
   * stops holding, so tune `concurrencySlotTtl` against the slowest legitimate
   * response.
   */
  private requestTtl: number = 120000;
  constructor(
    data: ClientConstructorData,
    rateLimit: ConcurrencyLimitClientOptions
  ) {
    super(data, data.client.name);
    assertUsableConcurrency(rateLimit, data.client.name);
    this.rateLimit = rateLimit;
    const slotTtl = data.client.requestOptions?.concurrencySlotTtl;
    if (slotTtl && Number.isFinite(slotTtl) && slotTtl > 0) {
      this.requestTtl = slotTtl;
    }
  }

  public handleRateLimitUpdated(data: RateLimitUpdatedData) {
    if (data.rateLimit.type !== "concurrencyLimit") return;
    // Keep the last workable value rather than applying a broken one.
    try {
      assertUsableConcurrency(data.rateLimit, this.name);
    } catch (error) {
      this.logger.error(
        { error, rateLimit: data.rateLimit },
        `Client ${this.name} | ignoring an unusable rate limit update`
      );
      return;
    }
    this.rateLimit = data.rateLimit;

    // Poked on any change, in either direction. A reduction can make an
    // already-queued cost impossible, which the drain loop discovers and reports.
    // An increase creates capacity that no existing booking covers, because a
    // declined request's only wake-up was computed from the token maths at decline
    // time. Unconditional, since `processRequests` coalesces through
    // `processingLock` and returns immediately with nothing to admit.
    {
      this.processRequests().catch((error) => {
        this.logger.error(
          { error },
          `Client ${this.name} | processRequests failed after a concurrency reduction`
        );
      });
    }
  }

  protected async getRateLimitStats(): Promise<RateLimitStats> {
    return this.rateLimit;
  }

  protected async handleOwnTypeRequestDone(data: RequestDoneData) {
    const key = this.getConcurrencyKey(data.grantId);
    // A retry's payload is built after `incrementRetries`, so `data.retries` names
    // the next attempt while the slot being given back belongs to the one that just
    // ran. `retry` and the increment are biconditional, so the adjustment applies
    // exactly when it should.
    const attempt =
      data.responseStatus === "retry"
        ? { ...data, retries: Math.max(0, data.retries - 1) }
        : data;
    // Abandonment is the one caller that reports `retry: false` on a request whose
    // counter was already incremented, so it releases a slot id for an attempt that
    // never reached admission. Harmless only because the ledger is a set and release
    // is delete-by-member: anything that makes release arithmetic rather than
    // idempotent must pass the abandoned attempt's true retry count.
    await this.backend.releaseConcurrency(key, this.getSlotId(attempt));
    this.logger.debug(
      `Request ${data.requestId} released concurrency slot for ${this.name}`
    );
    // A freed slot is the only thing a queued request is waiting for, and
    // nothing else would tell the admission loop it exists.
    this.processRequests().catch((error) => {
      this.logger.error(
        { error },
        `Client ${this.name} | processRequests failed after releasing a slot`
      );
    });
  }

  protected handleFreezeOwnTypeRequests(_grantId?: string): void {
    // Nothing to reset: slots are released on completion, not on freeze.
  }

  protected getRetryBackoffBaseTime(): number {
    return this.retryOptions.retryBackoffBaseTime;
  }

  /**
   * Takes a slot directly when nothing is queued and one is free, sparing the
   * request a trip through the queue for a slot it could have had immediately.
   *
   * Grant-scoped requests are excluded: they track slots per grant, so the
   * single-key check here would be wrong.
   */
  protected async tryAdmitImmediately(
    request: RequestMetadata
  ): Promise<boolean> {
    if (request.grantId) return false;
    return this.backend.tryAdmitConcurrency(
      this.getQueueKey(),
      this.getConcurrencyKey(),
      this.getFreezeStateKey(),
      request.cost ?? 1,
      this.getSlotId(request),
      {
        maxConcurrency: this.rateLimit.maxConcurrency,
        requestTtl: this.requestTtl,
      }
    );
  }

  /**
   * Claims the slot as part of the admission decision, so a request without one is
   * simply not admitted and the loop moves on. Waiting here would block every
   * request behind it however many slots were free.
   */
  protected async canProcessNextRequest(request: RequestMetadata): Promise<{
    canProcess: boolean;
    isThawRequest: boolean;
    frozenUntil?: number;
  }> {
    const freeze = await super.canProcessNextRequest(request);
    if (!freeze.canProcess) return freeze;

    const key = this.getConcurrencyKey(request.grantId);
    const config = {
      maxConcurrency: this.rateLimit.maxConcurrency,
      requestTtl: this.requestTtl,
    };
    const acquireQueued = this.backend.acquireQueuedConcurrency;
    const result = acquireQueued
      ? await acquireQueued.call(
          this.backend,
          key,
          this.getRequestMetadataKey(request.requestId),
          request.cost ?? 1,
          this.getSlotId(request),
          config
        )
      : await this.backend.acquireConcurrency(
          key,
          request.cost ?? 1,
          this.getSlotId(request),
          config
        );
    return { canProcess: result.acquired, isThawRequest: freeze.isThawRequest };
  }

  protected claimsOnAdmission(): boolean {
    return true;
  }

  /** Hands back the slot claimed in `canProcessNextRequest`. */
  protected async releaseAdmission(request: RequestMetadata): Promise<void> {
    await this.backend.releaseConcurrency(
      this.getConcurrencyKey(request.grantId),
      this.getSlotId(request)
    );
  }

  private getSlotId(request: Pick<RequestMetadata, "requestId" | "retries">) {
    return request.retries === 0
      ? request.requestId
      : `${request.requestId}:retry:${request.retries}`;
  }

  /** Grant-isolated clients track slots per grant. */
  private getConcurrencyKey(grantId?: string): string {
    const baseKey = `${this.namespace}:concurrency`;

    if (!grantId) return baseKey;

    if (this.usesGrantIsolation()) {
      this.knownGrantIds.add(grantId);
      return `${this.namespace}:grant:${grantId}:concurrency`;
    }

    return baseKey;
  }

  /**
   * Clears the slot ledger only on final removal. The key is fleet-wide while
   * `destroy()` is local, so clearing it on a rebuild would delete slots held by
   * peers' in-flight requests and admit a fresh `maxConcurrency` on top of them.
   */
  protected handleDestroy(_finalRemoval: boolean): void {
    // Never clear a fleet-wide ledger from one replica. Even a final template
    // removal can race requests already running on peers; deleting their slots
    // would admit over the cap if the name is re-added before they finish. The
    // keys already expire, and ordinary completion/slot TTL cleanup reclaims
    // them safely.
    this.knownGrantIds.clear();
  }

  /** Grants this client has actually claimed a slot for, so destroy can clean up. */
  private knownGrantIds = new Set<string>();
}

/**
 * Rejects a concurrency ceiling that cannot admit anything.
 *
 * `NaN` is the dangerous value: `used + cost <= NaN` is false in JavaScript,
 * which removes the cap, and nil-ish in Lua, which refuses everything. The usual
 * source is a `rateLimitChange` reading a header that was absent once.
 */
function assertUsableConcurrency(
  rateLimit: ConcurrencyLimitClientOptions,
  clientName: string
): void {
  const { maxConcurrency } = rateLimit;
  if (Number.isFinite(maxConcurrency) && maxConcurrency > 0) return;
  throw new ConfigurationError(
    "invalid_rate_limit",
    `Client "${clientName}" has an unusable concurrencyLimit: maxConcurrency must be a finite number greater than 0. Received ${JSON.stringify(
      maxConcurrency
    )}.`
  );
}

export default ConcurrencyLimitClient;
