import type { RequestDoneData, RequestMetadata } from "../../request/types.js";
import type { MultiLimitSpec } from "../../backend/types.js";
import BaseClient from "../index.js";
import {
  assertUsableRateLimits,
  hasMeteredBudget,
  normalizeRateLimit,
  shortestRefillInterval,
  soleRateLimit,
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
  RateLimitUpdatedData,
} from "../types.js";

/**
 * Every client that meters against budgets of its own — one of them or several.
 *
 * One class rather than one per limit type, because the only thing that varies
 * between them is which backend call claims the budget. A request is admitted
 * when every declared limit can take it, which for a single limit is the
 * ordinary token bucket or concurrency ceiling and for several is all of them at
 * once. See docs/rate-limits/multiple-limits.md.
 *
 * The exception is `sharedLimit`, which owns no budget and no queue and is
 * therefore its own class. It may not be combined with these.
 */
class MeteredClient extends BaseClient {
  protected rateLimit: NamedRateLimitData[];
  /**
   * The one limit this client declares, when it declares one.
   *
   * Present only so the single-budget backend calls can be used where they
   * apply: they are one round trip against one key, where the general path
   * encodes a spec list and runs a variadic script. The decision each makes is
   * identical — this is a shortcut, never a different rule.
   *
   * Derived rather than cached, here and below. A cached copy is a second
   * reading of `rateLimit` that a write can leave behind, and the two disagreeing
   * would have the claim spend against one budget while the ceiling was checked
   * against another.
   */
  private get sole(): NamedRateLimitData | undefined {
    return soleRateLimit(this.rateLimit);
  }

  /** False when nothing here meters anything, so admission needs no claim at all. */
  private get metered(): boolean {
    return hasMeteredBudget(this.rateLimit);
  }

  constructor(data: ClientConstructorData, rateLimit: NamedRateLimitData[]) {
    super(data, data.client.name);
    assertUsableRateLimits(rateLimit, data.client.name);
    this.rateLimit = rateLimit;
    assertBackendCanClaim(data, rateLimit, soleRateLimit(rateLimit));
    const slotTtl = data.client.requestOptions?.concurrencySlotTtl;
    if (slotTtl && Number.isFinite(slotTtl) && slotTtl > 0) {
      this.requestTtl = slotTtl;
    }
  }

  /**
   * Crash recovery only, for concurrency entries, and deliberately not
   * `cleanupTimeout` — the two want opposite values. Too short reaps slots from
   * requests still in flight and the cap stops holding, so tune
   * `concurrencySlotTtl` against the slowest legitimate response.
   */
  private requestTtl = 120000;

  public handleRateLimitUpdated(data: RateLimitUpdatedData) {
    // A config this class does not build would mean a different client class,
    // which a broadcast cannot change; the rebuild path swaps one for another.
    // Normalised on receipt, not just before publishing: an operator update or a
    // replica running older code can present the shape a caller writes.
    const limits = normalizeRateLimit(data.rateLimit);
    if (limits.some((limit) => limit.type === "sharedLimit")) return;
    try {
      assertUsableRateLimits(limits, this.name);
    } catch (error) {
      this.logger.error(
        { error, rateLimit: data.rateLimit },
        `Client ${this.name} | ignoring an unusable rate limit update`
      );
      return;
    }
    this.rateLimit = limits;

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
   * Every budget a request must claim, in the order declared, each keyed by its
   * own name under this client's namespace. A `noLimit` entry contributes
   * nothing, which is what makes it legal here.
   */
  public getLimitSpecs(
    grantId: string | undefined,
    slotId: string
  ): MultiLimitSpec[] {
    const specs: MultiLimitSpec[] = [];
    for (const limit of this.rateLimit) {
      if (limit.type === "requestLimit") {
        specs.push({
          kind: "tokenBucket",
          key: this.getBucketKey(limit.name, grantId),
          freezeKey: this.getFreezeStateKey(grantId),
          config: this.bucketConfig(limit),
        });
      } else if (limit.type === "concurrencyLimit") {
        specs.push({
          kind: "concurrency",
          key: this.getSlotKey(limit.name, grantId),
          freezeKey: this.getFreezeStateKey(grantId),
          config: {
            maxConcurrency: limit.maxConcurrency,
            requestTtl: this.requestTtl,
          },
          slotId,
        });
      }
    }
    return specs;
  }

  private getBucketKey(name: string, grantId?: string): string {
    return `${this.getRateLimitKey(grantId)}:${name}`;
  }

  private getSlotKey(name: string, grantId?: string): string {
    if (grantId && this.usesGrantIsolation()) this.knownGrantIds.add(grantId);
    return `${this.getConcurrencyKey(grantId)}:${name}`;
  }

  private bucketConfig(limit: NamedRateLimitData & { type: "requestLimit" }) {
    return {
      maxTokens: limit.maxTokens,
      tokensToAdd: limit.tokensToAdd,
      interval: limit.interval,
    };
  }

  // --------------------------------------------------------------- admission

  /**
   * Grant-scoped requests are excluded, as they are for every client type: the
   * queue this path checks is client-level, and a grant-isolated tenant's turn is
   * decided by the scan in `processRequests`.
   */
  protected async tryAdmitImmediately(
    request: RequestMetadata
  ): Promise<boolean> {
    if (request.grantId) return false;
    const cost = request.cost ?? 1;
    const sole = this.sole;

    if (!this.metered) {
      // One atomic operation, not two reads: a peer enqueueing between a separate
      // "queue empty?" and "frozen?" would be overtaken by the arrival that read
      // first, which the backend contract forbids. A client with no budget does
      // still queue — a 5xx freezes it, and that backlog is still draining when
      // this path reopens.
      return this.backend.tryAdmitNoLimit(
        this.getQueueKey(),
        this.getFreezeStateKey(request.grantId)
      );
    }

    if (sole?.type === "requestLimit") {
      return this.backend.tryAdmitImmediately(
        this.getQueueKey(),
        this.getBucketKey(sole.name),
        this.getFreezeStateKey(),
        cost,
        this.bucketConfig(sole)
      );
    }
    if (sole?.type === "concurrencyLimit") {
      return this.backend.tryAdmitConcurrency(
        this.getQueueKey(),
        this.getSlotKey(sole.name),
        this.getFreezeStateKey(),
        cost,
        this.getSlotId(request),
        { maxConcurrency: sole.maxConcurrency, requestTtl: this.requestTtl }
      );
    }

    const tryAdmit = this.backend.tryAdmitMultiLimit;
    if (!tryAdmit) return false;
    return tryAdmit.call(
      this.backend,
      this.getQueueKey(),
      [this.getFreezeStateKey()],
      this.getLimitSpecs(undefined, this.getSlotId(request)),
      cost
    );
  }

  /**
   * Claims every budget, or none of them.
   *
   * All-or-nothing is the backend's, in one operation: claiming them in turn from
   * here would leave the first spent when the second declines, and nothing on the
   * decline path hands back a budget the request never used.
   */
  protected async tryAcquireTurn(
    request: RequestMetadata
  ): Promise<AcquireTurnResult> {
    const { grantId } = request;
    // Nothing to claim, and nothing to check that `canProcessNextRequest` has not
    // already checked. Returning here is what keeps an unmetered client off the
    // backend entirely on its hot path.
    if (!this.metered) return { acquired: true };

    const cost = request.cost ?? 1;

    // Re-checked on the controller, not just at origination: `Request` validates
    // cost where the call is made, but the controller reads it back out of the
    // shared queue, where a replica running older code or anything else with
    // keyspace write access can present a negative cost that `tokens - cost`
    // would add to the bucket. The origination check is policy; this one guards
    // the arithmetic.
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

    // A 429 can land on another request between admission and here, and sending
    // this one would walk into a door that just closed.
    if (await this.isFrozen(grantId)) {
      throw new ClientFrozenError(this.name);
    }

    const claimed = await this.claim(request, cost);
    if (claimed.error) {
      throw new ConfigurationError(
        "unusable_rate_limit",
        `Client ${this.name} cannot acquire its budgets: ${claimed.error}`
      );
    }
    if (!claimed.acquired) {
      return { acquired: false, waitTime: this.declineWait(claimed.waitTime) };
    }

    // The freeze may have landed during the claim, budgets already spent. Handing
    // them back is what stops a concurrency entry leaking a slot no completion
    // will ever release.
    if (await this.isFrozen(grantId)) {
      await this.releaseClaim(request, cost).catch((error: unknown) => {
        this.logger.error(
          { error },
          `Client ${this.name} | could not release budgets claimed just before a freeze`
        );
      });
      throw new ClientFrozenError(this.name);
    }

    return { acquired: true };
  }

  /** The claim itself, by whichever backend call fits this client's budgets. */
  private async claim(
    request: RequestMetadata,
    cost: number
  ): Promise<{ acquired: boolean; waitTime?: number; error?: string }> {
    const sole = this.sole;
    const { grantId } = request;

    if (sole?.type === "requestLimit") {
      return this.backend.acquireTokens(
        this.getBucketKey(sole.name, grantId),
        cost,
        this.bucketConfig(sole)
      );
    }
    if (sole?.type === "concurrencyLimit") {
      const config = {
        maxConcurrency: sole.maxConcurrency,
        requestTtl: this.requestTtl,
      };
      const key = this.getSlotKey(sole.name, grantId);
      const slotId = this.getSlotId(request);
      // Claims only while the entry is still `inProgress`, in one operation. The
      // request can finish between the drain loop selecting it and this claim, and
      // admission excludes a request's own id from the occupancy sum — so a claim
      // for an id that just released would succeed, re-create its entry, and leave
      // nothing to release it until the slot TTL.
      const acquireQueued = this.backend.acquireQueuedConcurrency;
      return acquireQueued
        ? acquireQueued.call(
            this.backend,
            key,
            this.getRequestMetadataKey(request.requestId),
            cost,
            slotId,
            config
          )
        : this.backend.acquireConcurrency(key, cost, slotId, config);
    }

    const acquire = this.backend.acquireMultiLimit;
    if (!acquire) {
      throw new ConfigurationError(
        "backend_missing_multi_limit",
        `Client "${this.name}" declares several rate limits, which needs a backend implementing acquireMultiLimit.`
      );
    }
    return acquire.call(
      this.backend,
      this.getLimitSpecs(grantId, this.getSlotId(request)),
      cost
    );
  }

  /** Hands back a claim, by the call matching the one that made it. */
  private async releaseClaim(
    request: RequestMetadata,
    cost: number
  ): Promise<void> {
    const sole = this.sole;
    const { grantId } = request;

    if (sole?.type === "requestLimit") {
      if (!this.backend.refundTokens) return;
      await this.backend.refundTokens(
        this.getBucketKey(sole.name, grantId),
        cost,
        sole.maxTokens,
        this.getFreezeStateKey(grantId)
      );
      return;
    }
    if (sole?.type === "concurrencyLimit") {
      await this.backend.releaseConcurrency(
        this.getSlotKey(sole.name, grantId),
        this.getSlotId(request)
      );
      return;
    }
    await this.backend.releaseMultiLimit?.(
      this.getLimitSpecs(grantId, this.getSlotId(request)),
      cost
    );
  }

  /**
   * How long before a decline is worth repeating.
   *
   * A concurrency budget declines with no deadline of its own — a slot is freed by
   * a completion, which pokes this loop directly — so the slot TTL stands in as the
   * latest moment capacity is guaranteed to exist. Without it the caller books a
   * 1ms wake-up and spins.
   */
  private declineWait(waitTime: number | undefined): number {
    if (waitTime !== undefined && waitTime > 0) return waitTime;
    const hasConcurrency = this.rateLimit.some(
      (limit) => limit.type === "concurrencyLimit"
    );
    return hasConcurrency ? this.requestTtl : (waitTime ?? 0);
  }

  /** True while any budget here is a slot ledger rather than a spend. */
  protected claimsReleasableCapacity(): boolean {
    return this.rateLimit.some((limit) => limit.type === "concurrencyLimit");
  }

  /** Hands back everything a claimed-but-never-dispatched attempt took. */
  protected async refundUnusedAdmission(
    request: RequestMetadata
  ): Promise<void> {
    if (!this.metered) return;
    await this.releaseClaim(request, request.cost ?? 1);
  }

  // -------------------------------------------------------------- completion

  /**
   * Releases the concurrency budgets. Tokens are not returned: they were spent on
   * a request that ran, which is what spending means.
   */
  protected async handleOwnTypeRequestDone(data: RequestDoneData) {
    if (!this.rateLimit.some((limit) => limit.type === "concurrencyLimit")) {
      return;
    }
    // A retry's payload is built after `incrementRetries`, so `data.retries` names
    // the next attempt while the slot being given back belongs to the one that
    // just ran. `retry` and the increment are biconditional, so the adjustment
    // applies exactly when it should.
    //
    // Abandonment is the one caller that reports `retry: false` on a request whose
    // counter was already incremented, so it releases a slot id for an attempt
    // that never reached admission. Harmless only because the ledger is a set and
    // release is delete-by-member: anything that makes release arithmetic rather
    // than idempotent must pass the abandoned attempt's true retry count.
    const attempt =
      data.responseStatus === "retry"
        ? { ...data, retries: Math.max(0, data.retries - 1) }
        : data;
    const slotId = this.getSlotId(attempt);
    for (const spec of this.getLimitSpecs(data.grantId, slotId)) {
      if (spec.kind !== "concurrency") continue;
      await this.backend.releaseConcurrency(spec.key, spec.slotId);
    }
    this.logger.debug(
      `Request ${data.requestId} released concurrency slot for ${this.name}`
    );
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
   * Empties the bucket only when there is exactly one to empty.
   *
   * A 429 proves the client's picture of the vendor was wrong, and with one
   * `requestLimit` that says which bucket is wrong. With several the response does
   * not say which limit was breached, and zeroing a per-day bucket over a
   * per-second breach would stand the client down until tomorrow — so the freeze
   * window is the whole stand-down there, floored at the shortest refill interval
   * by `getFreezeBaseTime` below.
   */
  protected async handleFreezeOwnTypeRequests(grantId?: string): Promise<void> {
    const sole = this.sole;
    if (sole?.type !== "requestLimit") return;
    // Keyed by grant so an isolated grant's freeze empties its own bucket rather
    // than the client-level one.
    await this.backend.hset(this.getBucketKey(sole.name, grantId), {
      tokens: 0,
      lastUpdate: await this.backend.now(),
    });
  }

  /**
   * The refill interval floors this, but only for an actual rate limit: after a
   * 429 the buckets are empty, so retrying sooner is pointless. A 5xx leaves them
   * untouched, and flooring there would stop the fleet for a window over one bad
   * response.
   */
  protected getRetryBackoffBaseTime(isRateLimited = false): number {
    const base = this.retryOptions.retryBackoffBaseTime;
    const interval = shortestRefillInterval(this.rateLimit);
    if (!isRateLimited || interval === undefined) return base;
    return Math.max(interval, base);
  }

  /**
   * A freeze is not over until a budget can refill, so the shortest refill
   * interval floors it — the shortest because it is the soonest anything here can
   * hand out a token at all, where the longest would pause the fleet for a day
   * over a per-second refusal.
   *
   * Through `super`, never `this.retryOptions`: `Math.max(interval, NaN)` is NaN,
   * which serialises as `null` and reads as falsy at the arming gate, so a raw
   * read arms no freeze at all for an unset `Number(process.env.BACKOFF)`.
   */
  protected getFreezeBaseTime(): number {
    const base = super.getFreezeBaseTime();
    const interval = shortestRefillInterval(this.rateLimit);
    return interval === undefined ? base : Math.max(interval, base);
  }

  // ------------------------------------------------------------------- stats

  protected async getRateLimitStats(): Promise<NamedRateLimitStats[]> {
    const limits: NamedRateLimitStats[] = [];
    for (const limit of this.rateLimit) {
      if (limit.type !== "requestLimit") {
        limits.push(limit);
        continue;
      }
      const state = await this.backend.getTokenBucketState(
        this.getBucketKey(limit.name),
        this.bucketConfig(limit)
      );
      limits.push({ ...limit, tokens: state.tokens });
    }
    return limits;
  }

  /**
   * Clears no fleet-wide state. Even a final removal can race requests still
   * running on peers, and deleting their slots would admit over the cap if the
   * name were re-added before they finished. The keys already expire, and
   * ordinary completion and slot TTL cleanup reclaim them safely.
   */
  protected handleDestroy(): void {
    this.knownGrantIds.clear();
  }

  /** Grants this client has claimed a slot for, so destroy can clean up. */
  private knownGrantIds = new Set<string>();
}

/**
 * Refuses to build a client whose budgets the backend cannot claim atomically.
 *
 * Only a client with more than one budget needs the multi-limit methods; a single
 * budget uses the calls every backend has always had. They are optional on
 * `DianemoBackend` so a backend written against an earlier version still
 * satisfies the interface, and falling back to claiming one budget at a time
 * would be worse than refusing: every decline would leak whatever the earlier
 * budgets had already handed over.
 */
function assertBackendCanClaim(
  data: ClientConstructorData,
  rateLimit: NamedRateLimitData[],
  sole: NamedRateLimitData | undefined
): void {
  if (sole) return;
  const { backend } = data;
  if (backend.acquireMultiLimit && backend.releaseMultiLimit) return;
  throw new ConfigurationError(
    "backend_missing_multi_limit",
    `Client "${data.client.name}" declares ${rateLimit.length} rate limits, which needs a backend implementing acquireMultiLimit and releaseMultiLimit. This backend implements neither, and claiming the budgets one at a time would leak the ones claimed before a decline.`
  );
}

export default MeteredClient;
