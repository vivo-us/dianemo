import type { RequestDoneData, RequestMetadata } from "../../request/types.js";
import BaseClient from "../index.js";
import type {
  ClientConstructorData,
  NoLimitClientOptions,
  RateLimitStats,
  RateLimitUpdatedData,
} from "../types.js";

class NoLimitClient extends BaseClient {
  protected rateLimit: NoLimitClientOptions;
  constructor(data: ClientConstructorData, rateLimit: NoLimitClientOptions) {
    super(data, data.client.name);
    this.rateLimit = rateLimit;
  }

  public handleRateLimitUpdated(data: RateLimitUpdatedData) {
    // An array would mean a different client class, which a broadcast cannot
    // change; the rebuild path is what swaps one client for another.
    if (Array.isArray(data.rateLimit)) return;
    if (data.rateLimit.type !== "noLimit") return;
    this.rateLimit = data.rateLimit;
  }

  protected async getRateLimitStats(): Promise<RateLimitStats> {
    return this.rateLimit;
  }

  protected handleOwnTypeRequestDone(_data: RequestDoneData) {
    return;
  }

  protected handleFreezeOwnTypeRequests(_grantId?: string) {
    return;
  }

  protected getRetryBackoffBaseTime() {
    return this.retryOptions.retryBackoffBaseTime;
  }

  /**
   * Immediately, unless the client is frozen or still probing recovery.
   *
   * With no budget there is nothing for the queue to arbitrate, so routing
   * through it costs several backend round-trips and buys nothing. The freeze
   * check is the one thing worth a round-trip: "no configured limit" is not
   * "ignore the limit the vendor just told us about".
   */
  protected async tryAdmitImmediately(
    request: RequestMetadata
  ): Promise<boolean> {
    // One atomic operation, not two reads: a peer enqueueing between a separate
    // "queue empty?" and "frozen?" would be overtaken by the arrival that read
    // first, which the backend contract forbids. A noLimit client does queue — a
    // 5xx freezes it, and that backlog is still draining when this path reopens.
    return this.backend.tryAdmitNoLimit(
      this.getQueueKey(),
      this.getFreezeStateKey(request.grantId)
    );
  }

  protected handleDestroy() {
    return;
  }
}

export default NoLimitClient;
