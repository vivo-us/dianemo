import type { RequestDoneData } from "../../request/types.js";
import { ClientUnavailableError } from "../../errors.js";
import BaseClient from "../index.js";
import type {
  ClientConstructorData,
  SharedLimitClientOptions,
  RateLimitStats,
  RateLimitUpdatedData,
} from "../types.js";

/**
 * Draws on another client's budget. See docs/rate-limits/shared-limit.md.
 */
class SharedLimitClient extends BaseClient {
  protected rateLimit: SharedLimitClientOptions;

  constructor(
    data: ClientConstructorData,
    rateLimit: SharedLimitClientOptions
  ) {
    // Constructing with the parent's name is the entire mechanism: queue,
    // bucket, freeze state and channels all resolve to the parent's keys.
    super(data, rateLimit.clientName);
    this.rateLimit = rateLimit;
  }

  public handleRateLimitUpdated(data: RateLimitUpdatedData) {
    if (data.rateLimit.type !== "sharedLimit") return;
    this.rateLimit = data.rateLimit;
  }

  /**
   * Refuses the request when the budget owner is gone. `createClient` checks this at
   * registration, but removing the parent afterwards forces this child to `worker`,
   * so nobody drains its queue and every request waits out its admission timeout.
   */
  protected async assertReadyToQueue(): Promise<void> {
    if (this.getParentRateLimit?.() === undefined) {
      throw new ClientUnavailableError(
        "shared_limit_parent_missing",
        `Client "${this.sourceClientData.name}" shares the rate limit of "${this.rateLimit.clientName}", which is no longer registered. Nothing drains that budget's queue, so the request would wait out its admission timeout and fail.`
      );
    }
  }

  protected async getRateLimitStats(): Promise<RateLimitStats> {
    return this.rateLimit;
  }

  // Empty because completion, freezing and cleanup all belong to the parent, which
  // owns the budget these requests spent. `handleDestroy` below is empty for the
  // same reason, not because it is unreachable — a rebuild does call it.
  protected handleOwnTypeRequestDone(_data: RequestDoneData) {}

  protected handleFreezeOwnTypeRequests(_grantId?: string) {}

  protected getRetryBackoffBaseTime() {
    return this.retryOptions.retryBackoffBaseTime;
  }

  /**
   * Floored at the owner's refill interval, matching what the parent would write:
   * the freeze goes to shared state, so answering from this client's own back-off
   * would give one upstream two recovery windows. Only the freeze is floored —
   * `getRetryBackoffBaseTime` still paces this request's own retries.
   */
  protected getFreezeBaseTime(): number {
    // Delegated, not re-read: the base class sanitises a zero, negative or NaN
    // back-off into a usable floor, and a raw `retryOptions` read here arms no freeze
    // at all for a child with `retryBackoffBaseTime: 0`.
    const base = super.getFreezeBaseTime();
    const parent = this.getParentRateLimit?.();
    return parent?.type === "requestLimit"
      ? Math.max(parent.interval, base)
      : base;
  }

  protected handleDestroy() {}
}

export default SharedLimitClient;
