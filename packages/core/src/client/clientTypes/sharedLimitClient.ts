import type { RequestDoneData } from "../../request/types.js";
import { ClientUnavailableError } from "../../errors.js";
import BaseClient from "../index.js";
import {
  normalizeRateLimit,
  shortestRefillInterval,
} from "../../utils/rateLimit.js";
import type {
  ClientConstructorData,
  NamedRateLimitData,
  NamedRateLimitStats,
  RateLimitUpdatedData,
  SharedLimitClientOptions,
} from "../types.js";

/**
 * Draws on another client's budget. See docs/rate-limits/shared-limit.md.
 */
class SharedLimitClient extends BaseClient {
  protected rateLimit: NamedRateLimitData[];
  /** The one entry such a client may declare; see `assertUsableRateLimits`. */
  private shared: SharedLimitClientOptions;

  constructor(data: ClientConstructorData, rateLimit: NamedRateLimitData[]) {
    const shared = rateLimit[0] as SharedLimitClientOptions;
    // Constructing with the parent's name is the entire mechanism: queue,
    // bucket, freeze state and channels all resolve to the parent's keys.
    super(data, shared.clientName);
    this.rateLimit = rateLimit;
    this.shared = shared;
  }

  public handleRateLimitUpdated(data: RateLimitUpdatedData) {
    // Anything else would mean a different client class, which a broadcast
    // cannot change; the rebuild path is what swaps one client for another.
    if (!Array.isArray(data.rateLimit)) return;
    const limits = normalizeRateLimit(data.rateLimit);
    const shared = limits[0];
    if (limits.length !== 1 || shared.type !== "sharedLimit") return;
    this.rateLimit = limits;
    this.shared = shared;
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
        `Client "${this.sourceClientData.name}" shares the rate limit of "${this.shared.clientName}", which is no longer registered. Nothing drains that budget's queue, so the request would wait out its admission timeout and fail.`
      );
    }
  }

  protected async getRateLimitStats(): Promise<NamedRateLimitStats[]> {
    // From the narrowed field, not `rateLimit`: the array's element type admits a
    // `requestLimit`, which would owe a token balance this client cannot report.
    return [{ ...this.shared, name: this.rateLimit[0].name }];
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
    // The shortest of the owner's refill intervals when it declares several: the
    // longest would floor every freeze at the owner's slowest quota.
    const interval = parent ? shortestRefillInterval(parent) : undefined;
    return interval === undefined ? base : Math.max(interval, base);
  }

  protected handleDestroy() {}
}

export default SharedLimitClient;
