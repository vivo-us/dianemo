import type { RequestDoneData, RequestMetadata } from "../../request/types.js";
import type { MultiLimitSpec } from "../../backend/types.js";
import BaseClient from "../index.js";
import {
  assertUsableRateLimits,
  shortestRefillInterval,
} from "../../utils/rateLimit.js";
import {
  ClientFrozenError,
  ConfigurationError,
  RequestCostExceedsBudgetError,
} from "../../errors.js";
import type {
  AcquireTurnResult,
  ClientConstructorData,
  NamedRateLimitData,
  NamedRateLimitStats,
  RateLimitStats,
  RateLimitUpdatedData,
} from "../types.js";

/**
 * A client metered by several budgets at once — a per-second and a per-day cap,
 * or a request rate alongside a concurrency ceiling. A request is sent only when
 * every one of them admits it. See docs/rate-limits/multiple-limits.md.
 */
class MultiLimitClient extends BaseClient {
  protected rateLimit: NamedRateLimitData[];

  constructor(data: ClientConstructorData, rateLimit: NamedRateLimitData[]) {
    super(data, data.client.name);
    assertUsableRateLimits(rateLimit, data.client.name);
    assertBackendSupportsMultiLimit(data, rateLimit);
    this.rateLimit = rateLimit;
    const slotTtl = data.client.requestOptions?.concurrencySlotTtl;
    if (slotTtl && Number.isFinite(slotTtl) && slotTtl > 0) {
      this.requestTtl = slotTtl;
    }
  }

  /** Crash recovery for concurrency entries only. See the concurrency client's twin. */
  private requestTtl = 120000;

  public handleRateLimitUpdated(data: RateLimitUpdatedData) {
    // A single limit here would change the client class, which a broadcast
    // cannot do — the same rule the four single-limit clients apply.
    if (!Array.isArray(data.rateLimit)) return;
    try {
      assertUsableRateLimits(data.rateLimit, this.name);
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
    this.processRequests().catch((error) => {
      this.logger.error(
        { error },
        `Client ${this.name} | processRequests failed after a budget change`
      );
    });
  }

  // ------------------------------------------------------------ budget specs

  /**
   * Every budget a request must claim, in the order declared.
   *
   * Each is keyed by its own name under this client's namespace. A `noLimit`
   * entry contributes nothing, which is what makes it legal here — a client that
   * borrows another's budget declares `sharedLimit` and nothing else, and is a
   * different class entirely.
   */
  public getLimitSpecs(
    grantId: string | undefined,
    slotId: string
  ): MultiLimitSpec[] {
    const specs: MultiLimitSpec[] = [];
    for (const limit of this.rateLimit) {
      switch (limit.type) {
        case "requestLimit":
          specs.push({
            kind: "tokenBucket",
            key: `${this.getRateLimitKey(grantId)}:${limit.name}`,
            freezeKey: this.getFreezeStateKey(grantId),
            config: {
              maxTokens: limit.maxTokens,
              tokensToAdd: limit.tokensToAdd,
              interval: limit.interval,
            },
          });
          break;
        case "concurrencyLimit":
          specs.push({
            kind: "concurrency",
            key: `${this.getConcurrencyKey(grantId)}:${limit.name}`,
            freezeKey: this.getFreezeStateKey(grantId),
            config: {
              maxConcurrency: limit.maxConcurrency,
              requestTtl: this.requestTtl,
            },
            slotId,
          });
          break;
        case "noLimit":
          break;
      }
    }
    return dedupeByKey(specs);
  }

  // --------------------------------------------------------------- admission

  /**
   * Grant-scoped requests are excluded, as they are for every other client type:
   * the queue this path checks is client-level, and a grant-isolated tenant's
   * turn is decided by the scan in `processRequests`.
   */
  protected async tryAdmitImmediately(
    request: RequestMetadata
  ): Promise<boolean> {
    if (request.grantId) return false;
    const tryAdmit = this.backend.tryAdmitMultiLimit;
    if (!tryAdmit) return false;
    return tryAdmit.call(
      this.backend,
      this.getQueueKey(),
      [this.getFreezeStateKey()],
      this.getLimitSpecs(undefined, this.getSlotId(request)),
      request.cost ?? 1
    );
  }

  /**
   * Claims every budget or none of them.
   *
   * The all-or-nothing part is the backend's, in one operation: claiming them in
   * turn from here would leave the first spent when the second declines, and
   * nothing on the decline path hands back a budget the request never used.
   */
  protected async tryAcquireTurn(
    request: RequestMetadata
  ): Promise<AcquireTurnResult> {
    const { cost, grantId } = request;

    // Re-checked on the controller, not just at origination: `Request` validates
    // cost where the call is made, but the controller reads it back out of the
    // shared queue, where a replica running older code or anything else with
    // keyspace write access can present a negative cost that `tokens - cost`
    // would add to the bucket.
    if (!Number.isFinite(cost) || cost < Number.EPSILON) {
      throw new ConfigurationError(
        "invalid_request_cost",
        `Client ${this.name} refused a queued request whose cost is not spendable: ${String(
          cost
        )}.`
      );
    }

    // The ceiling is re-read here rather than checked once up front: a
    // `rateLimitChange` can shrink the tightest budget below the cost of an
    // already-queued request, which then reports "not acquired, wait 0ms" forever.
    const ceiling = this.getCostCeilingForQueue();
    if (ceiling !== undefined && cost > ceiling) {
      throw new RequestCostExceedsBudgetError(this.name, cost, ceiling);
    }

    if (await this.isFrozen(grantId)) {
      throw new ClientFrozenError(this.name);
    }

    const acquire = this.backend.acquireMultiLimit;
    if (!acquire) {
      throw new ConfigurationError(
        "backend_missing_multi_limit",
        `Client "${this.name}" declares several rate limits, which needs a backend implementing acquireMultiLimit.`
      );
    }
    const specs = this.getLimitSpecs(grantId, this.getSlotId(request));
    const result = await acquire.call(this.backend, specs, cost);

    if (result.error) {
      throw new ConfigurationError(
        "unusable_rate_limit",
        `Client ${this.name} cannot acquire its budgets: ${result.error}`
      );
    }

    if (!result.acquired) {
      return { acquired: false, waitTime: this.declineWait(result.waitTime) };
    }

    // A freeze may have landed during the acquire, with the budgets already
    // claimed. Handing them back is what stops a concurrency entry leaking a slot
    // no completion will ever release.
    if (await this.isFrozen(grantId)) {
      await this.backend
        .releaseMultiLimit?.(specs, cost)
        .catch((error: unknown) => {
          this.logger.error(
            { error },
            `Client ${this.name} | could not release budgets claimed just before a freeze`
          );
        });
      throw new ClientFrozenError(this.name);
    }

    return { acquired: true };
  }

  /**
   * How long before the decline is worth repeating.
   *
   * A concurrency entry declines with no deadline of its own — a slot is freed by
   * a completion, which pokes this loop directly — so the slot TTL stands in as
   * the latest moment capacity is guaranteed to exist. Without it the caller
   * books a 1ms wake-up and spins.
   */
  private declineWait(waitTime: number | undefined): number {
    if (waitTime !== undefined && waitTime > 0) return waitTime;
    const hasConcurrency = this.rateLimit.some(
      (limit) => limit.type === "concurrencyLimit"
    );
    return hasConcurrency ? this.requestTtl : (waitTime ?? 0);
  }

  /** Hands back everything a claimed-but-never-dispatched attempt took. */
  protected async refundUnusedAdmission(
    request: RequestMetadata
  ): Promise<void> {
    await this.backend.releaseMultiLimit?.(
      this.getLimitSpecs(request.grantId, this.getSlotId(request)),
      request.cost ?? 1
    );
  }

  // -------------------------------------------------------------- completion

  /**
   * Releases the concurrency entries. Tokens are not returned: they were spent on
   * a request that ran, which is what spending means.
   */
  protected async handleOwnTypeRequestDone(data: RequestDoneData) {
    // A retry's payload is built after `incrementRetries`, so `data.retries` names
    // the next attempt while the slot being given back belongs to the one that
    // just ran. See the concurrency client's twin for the abandonment case.
    const attempt =
      data.responseStatus === "retry"
        ? { ...data, retries: Math.max(0, data.retries - 1) }
        : data;
    const slotId = this.getSlotId(attempt);
    for (const spec of this.getLimitSpecs(data.grantId, slotId)) {
      if (spec.kind !== "concurrency") continue;
      await this.backend.releaseConcurrency(spec.key, spec.slotId);
    }
    // A freed slot is the only thing a queued request may be waiting for, and
    // nothing else would tell the admission loop it exists.
    this.processRequests().catch((error) => {
      this.logger.error(
        { error },
        `Client ${this.name} | processRequests failed after releasing a slot`
      );
    });
  }

  /**
   * Arms the freeze and empties nothing.
   *
   * A single `requestLimit` client zeroes its bucket here, because a 429 proves
   * the bucket's picture of the vendor was wrong. With several budgets the
   * response does not say which one was breached, and zeroing a per-day bucket
   * over a per-second breach would stand the client down until tomorrow. The
   * freeze window is the stand-down instead: floored at the shortest refill
   * interval below, and lengthened by each further 429.
   */
  protected handleFreezeOwnTypeRequests(_grantId?: string): void {}

  protected getRetryBackoffBaseTime(isRateLimited = false): number {
    const base = this.retryOptions.retryBackoffBaseTime;
    const interval = shortestRefillInterval(this.rateLimit);
    if (!isRateLimited || interval === undefined) return base;
    return Math.max(interval, base);
  }

  /**
   * The shortest refill interval floors the freeze: it is the soonest any budget
   * here can hand out anything at all. Through `super`, never `this.retryOptions`
   * — see the base implementation for what a raw read does to an unset back-off.
   */
  protected getFreezeBaseTime(): number {
    const base = super.getFreezeBaseTime();
    const interval = shortestRefillInterval(this.rateLimit);
    return interval === undefined ? base : Math.max(interval, base);
  }

  // ------------------------------------------------------------------- stats

  protected async getRateLimitStats(): Promise<RateLimitStats> {
    const limits: NamedRateLimitStats[] = [];
    for (const limit of this.rateLimit) {
      if (limit.type !== "requestLimit") {
        limits.push(limit);
        continue;
      }
      const state = await this.backend.getTokenBucketState(
        `${this.getRateLimitKey()}:${limit.name}`,
        {
          maxTokens: limit.maxTokens,
          tokensToAdd: limit.tokensToAdd,
          interval: limit.interval,
        }
      );
      limits.push({ ...limit, tokens: state.tokens });
    }
    return { type: "multiLimit", limits };
  }

  /**
   * Clears nothing fleet-wide, for the reason the concurrency client gives: a
   * slot ledger is shared, and a rebuild deleting it would admit a fresh ceiling
   * on top of requests still running on peers.
   */
  protected handleDestroy(): void {}
}

/**
 * Refuses to build a client whose budgets the backend cannot claim atomically.
 *
 * The three multi-limit methods are optional on `DianemoBackend` so a backend
 * written against an earlier version still satisfies the interface. Falling back
 * to claiming one budget at a time would be worse than refusing: every decline
 * would leak whatever the earlier budgets had already handed over.
 */
function assertBackendSupportsMultiLimit(
  data: ClientConstructorData,
  rateLimit: NamedRateLimitData[]
): void {
  const { backend } = data;
  if (backend.acquireMultiLimit && backend.releaseMultiLimit) return;
  throw new ConfigurationError(
    "backend_missing_multi_limit",
    `Client "${data.client.name}" declares ${rateLimit.length} rate limits, which needs a backend implementing acquireMultiLimit and releaseMultiLimit. This backend implements neither, and claiming the budgets one at a time would leak the ones claimed before a decline.`
  );
}

/**
 * Collapses budgets that resolve to one key, so a client naming the same balance
 * twice — its own limit and a `sharedLimit` entry pointing back at it — spends it
 * once rather than twice per request.
 */
function dedupeByKey(specs: MultiLimitSpec[]): MultiLimitSpec[] {
  const byKey = new Map<string, MultiLimitSpec>();
  for (const spec of specs) {
    if (!byKey.has(spec.key)) byKey.set(spec.key, spec);
  }
  return [...byKey.values()];
}

export default MultiLimitClient;
