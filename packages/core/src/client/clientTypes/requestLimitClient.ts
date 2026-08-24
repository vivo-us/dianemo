import type { RequestMetadata } from "../../request/types.js";
import BaseClient from "../index.js";
import {
  ClientFrozenError,
  ConfigurationError,
  RequestCostExceedsBudgetError,
} from "../../errors.js";
import type {
  AcquireTurnResult,
  ClientConstructorData,
  RateLimitStats,
  RateLimitUpdatedData,
  RequestLimitClientOptions,
  RequestLimitClientStats,
} from "../types.js";

/** Token bucket. See docs/rate-limits/request-limit.md. */
class RequestLimitClient extends BaseClient {
  protected rateLimit: RequestLimitClientOptions;

  constructor(
    data: ClientConstructorData,
    rateLimit: RequestLimitClientOptions
  ) {
    super(data, data.client.name);
    assertUsableBudget(rateLimit, data.client.name);
    this.rateLimit = rateLimit;
  }

  public handleRateLimitUpdated(data: RateLimitUpdatedData) {
    if (data.rateLimit.type !== "requestLimit") return;
    // A dynamic update arrives from `rateLimitChange` — often parsed out of a
    // vendor header — so it is the likeliest source of a budget that can never
    // refill. Refusing it keeps the last workable limit instead of wedging the
    // client, which is what an unusable one does.
    try {
      assertUsableBudget(data.rateLimit, this.name);
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
          `Client ${this.name} | processRequests failed after a budget reduction`
        );
      });
    }
  }

  protected async getRateLimitStats(): Promise<RateLimitStats> {
    const state = await this.backend.getTokenBucketState(
      this.getRateLimitKey(),
      {
        maxTokens: this.rateLimit.maxTokens,
        tokensToAdd: this.rateLimit.tokensToAdd,
        interval: this.rateLimit.interval,
      }
    );

    const stats: RequestLimitClientStats = {
      ...this.rateLimit,
      tokens: state.tokens,
    };
    return stats;
  }

  protected handleOwnTypeRequestDone(): void {}

  protected async handleFreezeOwnTypeRequests(grantId?: string): Promise<void> {
    // Keyed by grant so an isolated grant's freeze empties its own bucket
    // rather than the client-level one.
    const key = this.getRateLimitKey(grantId);
    await this.backend.hset(key, {
      tokens: 0,
      lastUpdate: await this.backend.now(),
    });
  }

  /**
   * The refill interval floors this, but only for an actual rate limit: after a 429
   * the bucket is empty, so retrying sooner is pointless. A 5xx leaves the bucket
   * untouched, and flooring there would stop the fleet for a window over one bad
   * response.
   */
  protected getRetryBackoffBaseTime(isRateLimited = false): number {
    const base = this.retryOptions.retryBackoffBaseTime;
    return isRateLimited ? Math.max(this.rateLimit.interval, base) : base;
  }

  /**
   * A freeze empties the bucket, so it is not over until the bucket can refill —
   * the interval floors it even where the back-off above honours something shorter.
   *
   * Through `super`, never `this.retryOptions`: `Math.max(interval, NaN)` is NaN,
   * which serialises as `null` and reads as falsy at the arming gate, so a raw read
   * arms no freeze at all for an unset `Number(process.env.BACKOFF)`.
   */
  protected getFreezeBaseTime(): number {
    return Math.max(this.rateLimit.interval, super.getFreezeBaseTime());
  }

  /**
   * Grant-scoped requests are excluded: they draw on per-grant buckets with
   * their own freeze state, so the single-bucket check here would be wrong.
   */
  protected async tryAdmitImmediately(
    request: RequestMetadata
  ): Promise<boolean> {
    if (request.grantId) return false;
    return this.backend.tryAdmitImmediately(
      this.getQueueKey(),
      this.getRateLimitKey(),
      this.getFreezeStateKey(),
      request.cost ?? 1,
      {
        maxTokens: this.rateLimit.maxTokens,
        tokensToAdd: this.rateLimit.tokensToAdd,
        interval: this.rateLimit.interval,
      }
    );
  }

  /**
   * The ceiling is re-read here, not checked once up front: a `rateLimitChange` can
   * shrink `maxTokens` below the cost of an already-queued request, which then
   * reports "not acquired, wait 0ms" forever.
   */
  protected async tryAcquireTurn(
    request: RequestMetadata
  ): Promise<AcquireTurnResult> {
    const { cost, grantId } = request;

    // Re-checked on the controller, not just at origination: `Request` validates
    // cost where the call is made, but the controller reads it back out of the shared
    // queue, where a replica running older code or anything else with keyspace write
    // access can present a negative cost that `tokens - cost` would add to the
    // bucket. The origination check is policy; this one guards the arithmetic.
    if (!Number.isFinite(cost) || cost < Number.EPSILON) {
      throw new ConfigurationError(
        "invalid_request_cost",
        `Client ${this.name} refused a queued request whose cost is not spendable: ${String(
          cost
        )}.`
      );
    }

    if (cost > this.rateLimit.maxTokens) {
      throw new RequestCostExceedsBudgetError(
        this.name,
        cost,
        this.rateLimit.maxTokens
      );
    }

    // A 429 can land on another request between admission and here, and sending
    // this one would walk into a door that just closed.
    if (await this.isFrozen(grantId)) {
      throw new ClientFrozenError(this.name);
    }

    const result = await this.backend.acquireTokens(
      this.getRateLimitKey(grantId),
      cost,
      {
        maxTokens: this.rateLimit.maxTokens,
        tokensToAdd: this.rateLimit.tokensToAdd,
        interval: this.rateLimit.interval,
      }
    );

    if (result.error) {
      throw new ConfigurationError(
        "unusable_rate_limit",
        `Client ${this.name} cannot acquire tokens: ${result.error}`
      );
    }

    if (!result.acquired) {
      return { acquired: false, waitTime: result.waitTime ?? 0 };
    }

    // The freeze may have landed during the acquire, tokens already spent.
    if (await this.isFrozen(grantId)) {
      throw new ClientFrozenError(this.name);
    }

    return { acquired: true };
  }

  protected async refundUnusedAdmission(
    request: RequestMetadata
  ): Promise<void> {
    if (!this.backend.refundTokens) return;
    await this.backend.refundTokens(
      this.getRateLimitKey(request.grantId),
      request.cost ?? 1,
      this.rateLimit.maxTokens,
      this.getFreezeStateKey(request.grantId)
    );
  }

  protected handleDestroy(): void {}
}

/**
 * Rejects a token-bucket budget that can never hand out a token.
 *
 * `tokensToAdd <= 0` is the dangerous one: the bucket drains, nothing refills
 * it, and every backend reports "not acquired, wait 0ms" forever. A config typo
 * or a `rateLimitChange` that parses a header into 0 or NaN both land here, so
 * this is the only thing standing between either and the acquire loop.
 */
function assertUsableBudget(
  rateLimit: RequestLimitClientOptions,
  clientName: string
): void {
  const problems: string[] = [];
  if (!Number.isFinite(rateLimit.tokensToAdd) || rateLimit.tokensToAdd <= 0) {
    problems.push(`tokensToAdd must be greater than 0`);
  }
  if (!Number.isFinite(rateLimit.interval) || rateLimit.interval <= 0) {
    problems.push(`interval must be greater than 0`);
  }
  if (!Number.isFinite(rateLimit.maxTokens) || rateLimit.maxTokens <= 0) {
    problems.push(`maxTokens must be greater than 0`);
  }
  if (!problems.length) return;
  throw new ConfigurationError(
    "invalid_rate_limit",
    `Client "${clientName}" has an unusable requestLimit: ${problems.join(
      "; "
    )}. Received ${JSON.stringify({
      maxTokens: rateLimit.maxTokens,
      tokensToAdd: rateLimit.tokensToAdd,
      interval: rateLimit.interval,
    })}.`
  );
}

export default RequestLimitClient;
