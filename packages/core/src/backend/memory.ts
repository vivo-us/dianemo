import { normalizeTtlSeconds, REQUEST_TOMBSTONE_TTL_SECONDS } from "./ttl.js";
import { EventEmitter } from "node:events";
import {
  parseFiniteStored,
  parsePriority,
  parseStoredNumber,
} from "./parseStored.js";
import {
  calculateQueueScore,
  QUEUE_RETRY_BAND,
  QUEUE_SCORE_EPOCH_MS,
} from "./queueScore.js";
import type {
  AcquireConcurrencyResult,
  AcquireTokensResult,
  BatchOp,
  ConcurrencyConfig,
  DianemoBackend,
  FreezeState,
  MessageHandler,
  QueueStats,
  QueuedRequest,
  TokenBucketConfig,
} from "./types.js";

/**
 * In-process coordination, for a deployment that is exactly one process.
 *
 * **Every process running this gets its own private copy of every limit**, so two
 * processes send twice the agreed rate and nothing reports it. See
 * docs/backends/memory.md before choosing it.
 *
 * Every operation here is synchronous, which is what makes it atomic — a function
 * body runs to completion before anything else does — so there is no locking below.
 */

interface Entry {
  expiresAt?: number;
  hash?: Map<string, string>;
  set?: Set<string>;
  zset?: Map<string, number>;
  str?: string;
}

/**
 * Whether a concurrency ceiling can admit anything. `NaN` compares false against
 * everything, so an unguarded `used + cost > max` reads as "under the cap".
 */
function usableConcurrency(maxConcurrency: number): boolean {
  return Number.isFinite(maxConcurrency) && maxConcurrency > 0;
}

/**
 * Whether a cost can be measured against a ceiling.
 *
 * `NaN` slips through the same comparison `usableConcurrency` guards. A negative
 * one is worse than unenforced: it lowers the occupancy every later request is
 * measured against, so one parked slot raises the ceiling for everyone. Twin of
 * the concurrency Lua's `usableCost`.
 */
function usableCost(cost: number): boolean {
  return Number.isFinite(cost) && cost >= 0;
}

/**
 * Marks an id as removed, so a later add for it is refused rather than creating an
 * entry nobody awaits. Same layout as the Redis backend's.
 */
function tombstoneKey(metadataKeyPrefix: string, requestId: string): string {
  return `${metadataKeyPrefix}:${requestId}:removed`;
}

/**
 * A stored arrival timestamp held inside the version-1 score epoch.
 *
 * `calculateQueueScore` raises for an arrival past the retry band, which is right for
 * a constant a maintainer chose and wrong for a field read back from shared state:
 * the raise would fail the retry being re-scored. Clamping keeps the entry inside its
 * own band, which is the property ordering depends on. The Lua re-score clamps to
 * match.
 */
function clampArrival(timestamp: number): number {
  const arrival = timestamp - QUEUE_SCORE_EPOCH_MS;
  if (arrival < 0) return QUEUE_SCORE_EPOCH_MS;
  if (arrival > QUEUE_RETRY_BAND - 1) {
    return QUEUE_SCORE_EPOCH_MS + QUEUE_RETRY_BAND - 1;
  }
  return timestamp;
}

/**
 * A stored freeze deadline, or `undefined` when the field cannot serve as one.
 *
 * An unparseable or non-finite value counts as no freeze state at all, matching the
 * Lua's `nil` test: it otherwise survived every comparison and reported a client
 * frozen with no deadline a wake-up could be booked against.
 */
function usableDeadline(raw: string): number | undefined {
  // Through the shared parse, so an empty field reads as unusable rather than as 0.
  const parsed = parseStoredNumber(raw, NaN);
  return Number.isFinite(parsed) ? parsed : undefined;
}

/**
 * Why this budget cannot admit anything, or `undefined` if it can.
 *
 * Reported through `AcquireTokensResult.error` rather than thrown, so an unusable
 * configuration stays distinguishable from a merely empty bucket. Mirrors the guards
 * at the top of the `tokenBucket` Lua.
 */
function unusableBudget(
  cost: number,
  config: TokenBucketConfig
): string | undefined {
  if (!(config.tokensToAdd > 0)) return "tokensToAdd must be greater than 0";
  if (!Number.isFinite(config.maxTokens) || config.maxTokens <= 0) {
    return "maxTokens must be a finite number greater than 0";
  }
  if (!Number.isFinite(cost)) return "cost must be a finite number";
  // Guarded even though `refill` tolerates it — its `interval > 0` test is false
  // for a NaN, so the bucket simply never refills. The waiting caller is what
  // breaks: `waitTime` is derived from `interval` and clears the `>= 0` clamp.
  if (!Number.isFinite(config.interval)) {
    return "interval must be a finite number";
  }
  return undefined;
}

class MemoryBackend implements DianemoBackend {
  readonly kind = "memory";
  readonly distributed = false;

  private store = new Map<string, Entry>();
  private bus = new EventEmitter();
  /**
   * A Set, not one handler per channel: these channels are a documented extension
   * point, so a host observer must not displace the client's own subscription.
   */
  private subscribed = new Map<string, Set<MessageHandler>>();
  private busListening = false;
  private sweeper: NodeJS.Timeout | null = null;

  constructor() {
    this.bus.setMaxListeners(0);
    this.armSweeper();
  }

  // ------------------------------------------------------------- primitives

  /**
   * Starts the periodic sweep if nothing is sweeping yet.
   *
   * Reads expire lazily, which is enough for correctness. This only stops keys
   * that are never read again from pinning memory forever — so a backend still
   * written to after {@link close} needs it back rather than a permanent stop.
   */
  private armSweeper() {
    if (this.sweeper) return;
    this.sweeper = setInterval(() => this.sweep(), 60_000);
    this.sweeper.unref?.();
  }

  /** Every key creation funnels through here, so any write re-arms the sweep. */
  private put(key: string, entry: Entry) {
    this.armSweeper();
    this.store.set(key, entry);
  }

  private sweep() {
    const now = Date.now();
    for (const [key, entry] of this.store) {
      if (entry.expiresAt !== undefined && now >= entry.expiresAt) {
        this.store.delete(key);
      }
    }
  }

  /** Returns the live entry for `key`, dropping it first if it has expired. */
  private live(key: string): Entry | undefined {
    const entry = this.store.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt !== undefined && Date.now() >= entry.expiresAt) {
      this.store.delete(key);
      return undefined;
    }
    return entry;
  }

  private entry(key: string): Entry {
    const existing = this.live(key);
    if (existing) return existing;
    const created: Entry = {};
    this.put(key, created);
    return created;
  }

  private hash(key: string): Map<string, string> {
    const entry = this.entry(key);
    entry.hash ??= new Map();
    return entry.hash;
  }

  private zset(key: string): Map<string, number> {
    const entry = this.entry(key);
    entry.zset ??= new Map();
    return entry.zset;
  }

  private setOf(key: string): Set<string> {
    const entry = this.entry(key);
    entry.set ??= new Set();
    return entry.set;
  }

  private expire(key: string, ttlSeconds: number) {
    // Normalised through the same helper the Redis backend uses. A NaN deadline made
    // `live()`'s `now >= expiresAt` false forever, so the key never expired at all.
    const ttl = normalizeTtlSeconds(ttlSeconds);
    const entry = this.live(key);
    if (!entry) return;
    if (ttl <= 0) {
      this.store.delete(key);
      return;
    }
    entry.expiresAt = Date.now() + ttl * 1000;
  }

  /** Members of a sorted set, ordered by score then lexically, as Redis does. */
  private sorted(key: string): string[] {
    const z = this.live(key)?.zset;
    if (!z) return [];
    return [...z.entries()]
      .sort((a, b) => a[1] - b[1] || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
      .map(([member]) => member);
  }

  private hget(key: string, field: string): string | undefined {
    return this.live(key)?.hash?.get(field);
  }

  /**
   * Two states block the fast path: inside the freeze window, and afterwards while
   * probes are still owed. Only the queue path can arrange one-at-a-time probing,
   * so a fast-path admission would resume at full rate and never decrement the count.
   */
  private freezeBlocksAdmission(freezeKey: string, now: number): boolean {
    const raw = this.hget(freezeKey, "frozenUntil");
    if (raw === undefined) return false;
    const frozenUntil = usableDeadline(raw);
    if (frozenUntil === undefined) return false;
    if (now < frozenUntil) return true;
    return parseFiniteStored(this.hget(freezeKey, "thawRequestCount"), 0) > 0;
  }

  // ------------------------------------------------------------------ store

  async get(key: string): Promise<string | null> {
    return this.live(key)?.str ?? null;
  }

  /**
   * The body of {@link set}, synchronous so that {@link batch} can apply a group
   * without suspending. Every write op has one of these: `await` yields to the
   * microtask queue, so awaiting a sibling method mid-group is what let a reader
   * resume between two ops and observe the group half-applied.
   */
  private writeString(key: string, value: string, ttlSeconds?: number) {
    // Read before the write, so a TTL this backend cannot store leaves the key
    // untouched rather than set with a lifetime nobody chose.
    const ttl =
      ttlSeconds === undefined ? undefined : normalizeTtlSeconds(ttlSeconds);
    if (ttl !== undefined && ttl <= 0) {
      this.store.delete(key);
      return;
    }
    // A plain SET replaces whatever was there, type included.
    const entry: Entry = { str: value };
    if (ttl !== undefined) entry.expiresAt = Date.now() + ttl * 1000;
    this.put(key, entry);
  }

  /** The body of {@link hset}, synchronous for the reason {@link writeString} carries. */
  private writeHash(
    key: string,
    fields: Record<string, string | number>,
    ttlSeconds?: number
  ) {
    // Validated first: `expire` would otherwise raise with the fields already set.
    const ttl =
      ttlSeconds === undefined ? undefined : normalizeTtlSeconds(ttlSeconds);
    const hash = this.hash(key);
    for (const [field, value] of Object.entries(fields)) {
      hash.set(field, String(value));
    }
    if (ttl !== undefined) this.expire(key, ttl);
  }

  async set(key: string, value: string, ttlSeconds?: number): Promise<void> {
    this.writeString(key, value, ttlSeconds);
  }

  async del(...keys: string[]): Promise<void> {
    for (const key of keys) this.store.delete(key);
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    const hash = this.live(key)?.hash;
    if (!hash) return {};
    return Object.fromEntries(hash);
  }

  async hset(
    key: string,
    fields: Record<string, string | number>,
    ttlSeconds?: number
  ): Promise<void> {
    this.writeHash(key, fields, ttlSeconds);
  }

  async sadd(key: string, member: string): Promise<void> {
    this.setOf(key).add(member);
  }

  async srem(key: string, member: string): Promise<void> {
    this.live(key)?.set?.delete(member);
  }

  async smembers(key: string): Promise<string[]> {
    return [...(this.live(key)?.set ?? [])];
  }

  async batch(ops: BatchOp[]): Promise<void> {
    // Every TTL read before the first write, matching the Redis backend: otherwise
    // an unusable one raises partway and leaves the earlier ops applied.
    for (const op of ops) {
      if ("ttlSeconds" in op && op.ttlSeconds !== undefined) {
        normalizeTtlSeconds(op.ttlSeconds);
      }
    }
    // Every op body below is synchronous, and must stay that way: nothing can
    // interleave with a synchronous loop, which is the whole of this backend's
    // atomicity and so the whole of what `atomic` asks for. Calling the `async`
    // siblings here instead — `await this.set(...)` — reintroduces the tear, because
    // `await` yields to the microtask queue between two ops even when the promise is
    // already settled.
    for (const op of ops) {
      switch (op.op) {
        case "set":
          this.writeString(op.key, op.value, op.ttlSeconds);
          break;
        case "del":
          this.store.delete(op.key);
          break;
        case "expire":
          this.expire(op.key, op.ttlSeconds);
          break;
        case "sadd":
          this.setOf(op.key).add(op.member);
          break;
        case "srem":
          this.live(op.key)?.set?.delete(op.member);
          break;
        case "hset":
          this.writeHash(op.key, op.fields, op.ttlSeconds);
          break;
        case "publish":
          this.emit(op.channel, op.message);
          break;
      }
    }
  }

  // ------------------------------------------------------------------ locks

  async acquireLock(
    key: string,
    token: string,
    ttlMs: number
  ): Promise<boolean> {
    // The one TTL argument that does not reach `normalizeTtlSeconds`, so it is
    // checked here. Redis refuses the same values from `SET ... PX`: "invalid
    // expire time in 'set' command" for a non-positive one, "value is not an
    // integer or out of range" for a non-finite one.
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
      throw new RangeError(
        `ttlMs must be a finite number greater than 0, received ${String(ttlMs)}`
      );
    }
    if (this.live(key)) return false;
    this.put(key, { str: token, expiresAt: Date.now() + ttlMs });
    return true;
  }

  async releaseLock(key: string, token: string): Promise<boolean> {
    if (this.live(key)?.str !== token) return false;
    this.store.delete(key);
    return true;
  }

  // ---------------------------------------------------------------- pub/sub

  private emit(channel: string, message: string) {
    // Redis delivers to subscribers asynchronously, and callers here rely on
    // publish() returning before the handler runs — a synchronous emit would
    // re-enter the publisher mid-operation.
    queueMicrotask(() => this.bus.emit("message", channel, message));
  }

  async publish(channel: string, message: string): Promise<void> {
    this.emit(channel, message);
  }

  async subscribe(channels: string[], handler: MessageHandler): Promise<void> {
    for (const channel of channels) {
      let handlers = this.subscribed.get(channel);
      if (!handlers) {
        handlers = new Set();
        this.subscribed.set(channel, handlers);
      }
      handlers.add(handler);
    }
    // One bus listener for the lifetime of the backend, rather than one per
    // subscribe call — repeated subscription would otherwise grow the emitter
    // without bound and trip Node's max-listeners warning.
    if (this.busListening) return;
    this.busListening = true;
    this.bus.on("message", (channel: string, message: string) => {
      const handlers = this.subscribed.get(channel);
      if (!handlers) return;
      // Copied, so a handler that subscribes or unsubscribes while dispatching
      // cannot mutate the set being iterated.
      for (const h of [...handlers]) h(channel, message);
    });
  }

  // ------------------------------------------------------------ token bucket

  /**
   * Refills lazily, exactly as the Lua does: whole intervals only, so
   * `lastUpdate` advances in interval-sized steps rather than to `now`.
   */
  private refill(
    key: string,
    config: TokenBucketConfig,
    now: number
  ): { tokens: number; lastUpdate: number } {
    // Parseability, not absence: `Number("")` is 0 and `Number("abc")` is NaN, and a
    // NaN balance is written back on refusal, so one unparseable byte refused every
    // later request forever. The Lua defaults the same read.
    let tokens = parseStoredNumber(this.hget(key, "tokens"), config.maxTokens);
    let lastUpdate = parseStoredNumber(this.hget(key, "lastUpdate"), now);

    const elapsed = now - lastUpdate;
    if (elapsed > 0 && config.interval > 0) {
      const intervalsElapsed = Math.floor(elapsed / config.interval);
      if (intervalsElapsed > 0) {
        tokens = Math.min(
          tokens + intervalsElapsed * config.tokensToAdd,
          config.maxTokens
        );
        lastUpdate += intervalsElapsed * config.interval;
      }
    }
    // Clamped unconditionally, not only after a refill: `maxTokens` can shrink
    // underneath a stocked bucket, which is what `rateLimitChange` is for, and a
    // balance banked under the old ceiling would stay spendable for a whole interval
    // — a cut from 1000 to 10 releasing 1000 requests immediately.
    tokens = Math.min(tokens, config.maxTokens);
    return { tokens, lastUpdate };
  }

  private writeBucket(
    key: string,
    tokens: number,
    lastUpdate: number,
    ttlSeconds: number
  ) {
    const hash = this.hash(key);
    hash.set("tokens", String(tokens));
    hash.set("lastUpdate", String(lastUpdate));
    this.expire(key, ttlSeconds);
  }

  async acquireTokens(
    key: string,
    cost: number,
    config: TokenBucketConfig
  ): Promise<AcquireTokensResult> {
    const configError = unusableBudget(cost, config);
    if (configError) {
      return {
        acquired: false,
        waitTime: 0,
        remainingTokens: 0,
        error: configError,
      };
    }
    const now = Date.now();
    const { tokens, lastUpdate } = this.refill(key, config, now);

    if (tokens >= cost) {
      this.writeBucket(key, tokens - cost, lastUpdate, 86400);
      return { acquired: true, remainingTokens: tokens - cost };
    }

    // lastUpdate only moves in whole intervals, so the time already served
    // toward the next refill has to come off the wait — the caller sleeps on
    // this number directly.
    const needed = cost - tokens;
    const intervalsNeeded = Math.ceil(needed / config.tokensToAdd);
    const waitTime = Math.max(
      0,
      intervalsNeeded * config.interval - (now - lastUpdate)
    );
    this.writeBucket(key, tokens, lastUpdate, 86400);
    return { acquired: false, waitTime, remainingTokens: tokens };
  }

  async getTokenBucketState(
    key: string,
    config: TokenBucketConfig
  ): Promise<{ tokens: number; lastUpdate: number }> {
    return this.refill(key, config, Date.now());
  }

  async resetTokenBucket(key: string, maxTokens: number): Promise<void> {
    this.writeBucket(key, maxTokens, Date.now(), 86400);
  }

  async refundTokens(
    key: string,
    cost: number,
    maxTokens: number,
    freezeKey?: string
  ): Promise<void> {
    const frozenUntil = freezeKey
      ? parseStoredNumber(this.hget(freezeKey, "frozenUntil"), 0)
      : 0;
    if (frozenUntil > Date.now()) return;
    const hash = this.live(key)?.hash;
    if (!hash) return;
    // An absent field leaves the bucket alone: the Lua returns early rather than
    // creating a balance, because a bucket with no `tokens` already reads as full.
    if (hash.get("tokens") === undefined) return;
    const tokens = parseStoredNumber(hash.get("tokens"), 0);
    hash.set("tokens", String(Math.min(maxTokens, tokens + cost)));
  }

  async tryAdmitImmediately(
    queueKey: string,
    bucketKey: string,
    freezeKey: string,
    cost: number,
    config: TokenBucketConfig,
    ttl = 86400
  ): Promise<boolean> {
    if (unusableBudget(cost, config)) return false;
    // Anything already waiting outranks a new arrival.
    if (this.sorted(queueKey).length > 0) return false;

    const now = Date.now();
    if (this.freezeBlocksAdmission(freezeKey, now)) return false;

    const { tokens, lastUpdate } = this.refill(bucketKey, config, now);
    if (tokens < cost) {
      this.writeBucket(bucketKey, tokens, lastUpdate, ttl);
      return false;
    }
    this.writeBucket(bucketKey, tokens - cost, lastUpdate, ttl);
    return true;
  }

  async tryAdmitNoLimit(queueKey: string, freezeKey: string): Promise<boolean> {
    // One call, no suspension: the queue check and the freeze check cannot be
    // separated by another spender's write.
    if (this.sorted(queueKey).length > 0) return false;
    return !this.freezeBlocksAdmission(freezeKey, Date.now());
  }

  // ------------------------------------------------------------- concurrency

  /** Drops slots older than the TTL — the equivalent of a crashed holder. */
  private reapConcurrency(key: string, expireTime: number) {
    const z = this.live(key)?.zset;
    if (!z) return;
    const costs = this.live(`${key}:costs`)?.hash;
    for (const [member, score] of [...z.entries()]) {
      if (score <= expireTime) {
        z.delete(member);
        costs?.delete(member);
      }
    }
  }

  /** Total cost in flight, ignoring `exceptId` so a resubmit is not counted twice. */
  private concurrencyCost(key: string, exceptId?: string): number {
    const z = this.live(key)?.zset;
    if (!z) return 0;
    const costs = this.live(`${key}:costs`)?.hash;
    let total = 0;
    for (const member of z.keys()) {
      if (member === exceptId) continue;
      // This total feeds admission, not just stats: a NaN makes `total + cost > max`
      // false and the ceiling stops enforcing entirely. The Lua defaults to 1 here.
      total += parseStoredNumber(costs?.get(member), 1);
    }
    return total;
  }

  private claimSlot(
    key: string,
    requestId: string,
    cost: number,
    now: number,
    keyTtl: number
  ) {
    this.zset(key).set(requestId, now);
    this.hash(`${key}:costs`).set(requestId, String(cost));
    this.expire(key, keyTtl);
    this.expire(`${key}:costs`, keyTtl);
  }

  async acquireConcurrency(
    key: string,
    cost: number,
    requestId: string,
    config: ConcurrencyConfig
  ): Promise<AcquireConcurrencyResult> {
    const now = Date.now();
    if (!usableConcurrency(config.maxConcurrency)) {
      return {
        acquired: false,
        currentConcurrency: 0,
        error: "maxConcurrency must be a finite number greater than 0",
      };
    }
    if (!usableCost(cost)) {
      return {
        acquired: false,
        currentConcurrency: 0,
        error: "cost must be a finite number that is not negative",
      };
    }
    this.reapConcurrency(key, now - config.requestTtl);
    const currentCost = this.concurrencyCost(key, requestId);

    if (currentCost + cost > config.maxConcurrency) {
      return { acquired: false, currentConcurrency: currentCost };
    }
    this.claimSlot(key, requestId, cost, now, 86400);
    return { acquired: true, currentConcurrency: currentCost + cost };
  }

  async acquireQueuedConcurrency(
    key: string,
    metadataKey: string,
    cost: number,
    requestId: string,
    config: ConcurrencyConfig
  ): Promise<AcquireConcurrencyResult> {
    if (!usableConcurrency(config.maxConcurrency)) {
      return {
        acquired: false,
        currentConcurrency: 0,
        error: "maxConcurrency must be a finite number greater than 0",
      };
    }
    // Ahead of the status check, as the Lua twin orders it: an unusable cost is
    // the caller's bug either way, and reporting "not in progress" for it would
    // send them looking at the queue entry instead.
    if (!usableCost(cost)) {
      return {
        acquired: false,
        currentConcurrency: 0,
        error: "cost must be a finite number that is not negative",
      };
    }
    this.reapConcurrency(key, Date.now() - config.requestTtl);
    if (this.hget(metadataKey, "status") !== "inProgress") {
      // Excluding `requestId`, as the Lua's shared slot accounting does and as
      // `acquireConcurrency` does below: a resubmission of a request that still
      // holds a slot must not be counted against itself.
      return {
        acquired: false,
        currentConcurrency: this.concurrencyCost(key, requestId),
      };
    }
    return this.acquireConcurrency(key, cost, requestId, config);
  }

  async tryAdmitConcurrency(
    queueKey: string,
    concurrencyKey: string,
    freezeKey: string,
    cost: number,
    requestId: string,
    config: ConcurrencyConfig,
    ttl = 86400
  ): Promise<boolean> {
    if (this.sorted(queueKey).length > 0) return false;
    if (!usableConcurrency(config.maxConcurrency)) return false;
    if (!usableCost(cost)) return false;

    const now = Date.now();
    if (this.freezeBlocksAdmission(freezeKey, now)) return false;

    this.reapConcurrency(concurrencyKey, now - config.requestTtl);
    const currentCost = this.concurrencyCost(concurrencyKey, requestId);
    if (currentCost + cost > config.maxConcurrency) return false;

    this.claimSlot(concurrencyKey, requestId, cost, now, ttl);
    return true;
  }

  async releaseConcurrency(key: string, requestId: string): Promise<void> {
    this.live(key)?.zset?.delete(requestId);
    this.live(`${key}:costs`)?.hash?.delete(requestId);
  }

  async getConcurrencyState(
    key: string,
    requestTtl: number
  ): Promise<{ currentConcurrency: number; activeRequests: string[] }> {
    // Raises rather than reporting an empty client: a NaN cutoff loses every
    // `timestamp > cutoff`, so a monitor would read a full client as idle. The
    // Redis twin already fails here, from `ZRANGEBYSCORE '(nan'`.
    if (!Number.isFinite(requestTtl)) {
      throw new RangeError(
        `requestTtl must be a finite number, received ${String(requestTtl)}`
      );
    }
    const cutoff = Date.now() - requestTtl;
    const activeRequests = [...(this.live(key)?.zset?.entries() ?? [])]
      .filter(([, timestamp]) => timestamp > cutoff)
      // Score then member, as `sorted()` and Redis both order: slots claimed in the
      // same millisecond are the common case under load, and ZRANGEBYSCORE breaks
      // that tie lexically rather than by insertion.
      .sort((a, b) => a[1] - b[1] || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
      .map(([requestId]) => requestId);
    const costs = this.live(`${key}:costs`)?.hash;
    return {
      currentConcurrency: activeRequests.reduce(
        (total, requestId) =>
          total + parseStoredNumber(costs?.get(requestId), 1),
        0
      ),
      activeRequests,
    };
  }

  async clearConcurrency(key: string): Promise<void> {
    this.store.delete(key);
    this.store.delete(`${key}:costs`);
  }

  // ------------------------------------------------------------------- queue

  private toQueued(metadata: Record<string, string>): QueuedRequest | null {
    if (
      !metadata.requestId ||
      !metadata.clientName ||
      !metadata.requestName ||
      !metadata.status
    ) {
      return null;
    }
    return {
      requestId: metadata.requestId,
      clientName: metadata.clientName,
      requestName: metadata.requestName,
      status: metadata.status as "pending" | "inProgress",
      priority: parsePriority(metadata.priority),
      cost: parseFloat(metadata.cost) || 1,
      retries: parseInt(metadata.retries, 10) || 0,
      timestamp: parseInt(metadata.timestamp, 10) || 0,
      grantId: metadata.grantId || undefined,
      isThawRequest: metadata.isThawRequest === "true",
      ownerId: metadata.ownerId || "",
    };
  }

  async addRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    request: QueuedRequest,
    ttl = 86400
  ): Promise<boolean> {
    const metadataKey = `${metadataKeyPrefix}:${request.requestId}`;
    // Refused after removal, so an abandonment that overtook its own add cannot be
    // undone by the add landing afterwards. See the Lua twin.
    if (this.live(tombstoneKey(metadataKeyPrefix, request.requestId))) {
      return false;
    }
    // Already queued — re-adding would reset its position, so a retry of the
    // enqueue call must be a no-op rather than a queue-jump.
    if (this.live(metadataKey)?.hash) return false;

    // Both the score and the TTL are read before either write, so an unusable one
    // leaves nothing behind. A half-added entry is the orphan
    // docs/design-notes.md#two-backends-one-contract warns about: it has a live
    // owner nobody awaits, so cleanup spares it and it pins the queue non-empty.
    const score = calculateQueueScore(
      request.priority,
      request.retries,
      request.timestamp
    );
    const normalizedTtl = normalizeTtlSeconds(ttl);

    this.zset(queueKey).set(request.requestId, score);
    const hash = this.hash(metadataKey);
    hash.set("requestId", request.requestId);
    hash.set("clientName", request.clientName);
    hash.set("requestName", request.requestName);
    hash.set("status", request.status);
    hash.set("priority", String(request.priority));
    hash.set("cost", String(request.cost));
    hash.set("retries", String(request.retries));
    hash.set("timestamp", String(request.timestamp));
    hash.set("grantId", request.grantId || "");
    hash.set("isThawRequest", request.isThawRequest ? "true" : "false");
    hash.set("ownerId", request.ownerId);
    this.expire(queueKey, normalizedTtl);
    this.expire(metadataKey, normalizedTtl);
    return true;
  }

  async getQueueLength(queueKey: string): Promise<number> {
    // `sorted()` would materialise and sort every member to produce a number the
    // map already knows. The fast path asks this on every request, so at depth it
    // was the sort — not the count — that cost the time.
    return this.live(queueKey)?.zset?.size ?? 0;
  }

  async getNextRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    skipGrantIds: string[] = [],
    skipRequestIds: string[] = []
  ): Promise<QueuedRequest | null> {
    const skip = new Set(skipGrantIds);
    const skipIds = new Set(skipRequestIds);
    for (const requestId of this.sorted(queueKey)) {
      if (skipIds.has(requestId)) continue;
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;
      if (this.hget(metadataKey, "status") !== "pending") continue;
      const grantId = this.hget(metadataKey, "grantId") ?? "";
      if (grantId !== "" && skip.has(grantId)) continue;
      this.hash(metadataKey).set("status", "inProgress");
      return this.toQueued(await this.hgetall(metadataKey));
    }
    return null;
  }

  async getRequest(
    metadataKeyPrefix: string,
    requestId: string
  ): Promise<QueuedRequest | null> {
    return this.toQueued(
      await this.hgetall(`${metadataKeyPrefix}:${requestId}`)
    );
  }

  async updateRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    requestId: string,
    updates: Partial<QueuedRequest>
  ): Promise<void> {
    const metadataKey = `${metadataKeyPrefix}:${requestId}`;
    const entry = this.live(metadataKey);
    if (!entry?.hash) return;
    const hash = entry.hash;

    let needsScoreUpdate = false;
    if (updates.status !== undefined) hash.set("status", updates.status);
    // Skipped rather than stored, matching the Lua's `type(x) == 'number'`:
    // `JSON.stringify` writes null for both NaN and Infinity, so a non-finite
    // field never reaches that script at all.
    if (updates.retries !== undefined && Number.isFinite(updates.retries)) {
      hash.set("retries", String(updates.retries));
      needsScoreUpdate = true;
    }
    if (updates.priority !== undefined && Number.isFinite(updates.priority)) {
      hash.set("priority", String(updates.priority));
      needsScoreUpdate = true;
    }
    if (updates.isThawRequest !== undefined) {
      hash.set("isThawRequest", updates.isThawRequest ? "true" : "false");
    }

    if (!needsScoreUpdate) return;
    this.zset(queueKey).set(
      requestId,
      // Parsed exactly as the Lua re-score parses the same fields, which is the
      // only way the two agree about a corrupt one: `Number` alone reads an empty
      // field as 0 and an unparseable one as NaN, where `tonumber(...) or <d>`
      // reads both as missing.
      calculateQueueScore(
        parseStoredNumber(hash.get("priority"), 1),
        parseStoredNumber(hash.get("retries"), 0),
        // Clamped into the arrival band rather than left to raise: a stored
        // timestamp is not a constant a maintainer chose, and the Lua twin clamps.
        clampArrival(parseStoredNumber(hash.get("timestamp"), 0))
      )
    );
  }

  async removeRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    requestId: string,
    tombstoneTtlSeconds: number = REQUEST_TOMBSTONE_TTL_SECONDS
  ): Promise<{ wasThawRequest: boolean }> {
    const metadataKey = `${metadataKeyPrefix}:${requestId}`;
    const wasThawRequest = this.hget(metadataKey, "isThawRequest") === "true";
    this.live(queueKey)?.zset?.delete(requestId);
    this.store.delete(metadataKey);
    // Written even when there was nothing to remove, which is the case the marker
    // exists for. See the Lua twin.
    const tombstone = tombstoneKey(metadataKeyPrefix, requestId);
    this.put(tombstone, {
      str: "1",
      expiresAt: Date.now() + tombstoneTtlSeconds * 1000,
    });
    return { wasThawRequest };
  }

  async getQueueStats(
    queueKey: string,
    metadataKeyPrefix: string
  ): Promise<QueueStats> {
    let pending = 0;
    let inProgress = 0;
    let totalCost = 0;
    for (const requestId of this.sorted(queueKey)) {
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;
      const status = this.hget(metadataKey, "status");
      if (!status) continue;
      // parseStoredNumber, not Number: `Number("")` is 0 and `Number("abc")` is
      // NaN, and a single NaN here poisons totalCost for the whole queue while
      // the Redis twin's `tonumber(...) or 1` reports 1 for both.
      totalCost += parseStoredNumber(this.hget(metadataKey, "cost"), 1);
      if (status === "pending") pending++;
      else if (status === "inProgress") inProgress++;
    }
    return { pending, inProgress, totalCost };
  }

  async getAllRequests(
    queueKey: string,
    metadataKeyPrefix: string
  ): Promise<QueuedRequest[]> {
    const requests: QueuedRequest[] = [];
    for (const requestId of this.sorted(queueKey)) {
      // Read synchronously, as `getQueueStats` reads the same entries: `await`
      // yields to the microtask queue between two entries even for an already
      // settled promise, and `BatchOptions.atomic` promises a reader never sees
      // a group half-applied.
      const hash = this.live(`${metadataKeyPrefix}:${requestId}`)?.hash;
      if (!hash?.size) continue;
      const parsed = this.toQueued(Object.fromEntries(hash));
      if (parsed) requests.push(parsed);
    }
    return requests;
  }

  async cleanupOrphanedRequests(
    queueKey: string,
    metadataKeyPrefix: string,
    aliveInstanceIds: Set<string>,
    currentInstanceId: string
  ): Promise<number> {
    // A set the sweeper cannot find itself in was not read authoritatively, and
    // acting on it reaps live owners. See the Lua twin.
    if (!aliveInstanceIds.has(currentInstanceId)) return 0;

    let cleanedUp = 0;
    for (const requestId of this.sorted(queueKey)) {
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;
      const ownerId = this.hget(metadataKey, "ownerId");
      if (ownerId === undefined) {
        // Metadata already gone; drop the dangling queue entry.
        this.live(queueKey)?.zset?.delete(requestId);
        cleanedUp++;
      } else if (ownerId === "" || !aliveInstanceIds.has(ownerId)) {
        this.live(queueKey)?.zset?.delete(requestId);
        this.store.delete(metadataKey);
        cleanedUp++;
      }
    }
    return cleanedUp;
  }

  // ------------------------------------------------------------ freeze / thaw

  async setFreezeState(
    key: string,
    frozenUntil: number,
    thawRequestCount: number
  ): Promise<void> {
    this.writeFreezeState(key, frozenUntil, thawRequestCount, "set");
  }

  /** See the Redis twin: never shortens a standing freeze, and returns what won. */
  async armFreeze(
    key: string,
    frozenUntil: number,
    thawRequestCount: number
  ): Promise<FreezeState> {
    return this.writeFreezeState(key, frozenUntil, thawRequestCount, "max");
  }

  /**
   * No `await` between the read and the write: this backend's atomicity is a
   * property of the language, but only for code that does not suspend.
   */
  private writeFreezeState(
    key: string,
    frozenUntil: number,
    thawRequestCount: number,
    mode: "set" | "max"
  ): FreezeState {
    // Sanitised before the merge: a NaN compares false against the standing value,
    // so the merge kept it and cancelled the probe budget a 429 was owed.
    if (!Number.isFinite(frozenUntil)) frozenUntil = 0;
    if (!Number.isFinite(thawRequestCount)) thawRequestCount = 0;
    // Truncated toward zero, which is what the Lua's `string.format('%d', ...)`
    // does to both fields on Lua 5.1 — a deadline in milliseconds is integral and
    // a probe budget is a count, so the two backends must round them the same way
    // or a merge against a stored value compares different numbers.
    frozenUntil = Math.trunc(frozenUntil);
    thawRequestCount = Math.trunc(thawRequestCount);
    if (mode === "max") {
      const existingRaw = this.hget(key, "frozenUntil");
      if (existingRaw !== undefined) {
        const existingUntil = parseFiniteStored(existingRaw, 0);
        if (existingUntil > frozenUntil) frozenUntil = existingUntil;
        const existingThaw = parseFiniteStored(
          this.hget(key, "thawRequestCount"),
          0
        );
        if (existingThaw > thawRequestCount) thawRequestCount = existingThaw;
      }
    }
    // Clamped after the merge, so a deadline already stored past the ceiling is
    // brought back down rather than carried forward by "keep the larger" — which
    // is what lets a poisoned key heal. `Number.MAX_SAFE_INTEGER` is the 2^53-1
    // the Lua twin inlines, and it is also what holds the TTL below inside the
    // range `normalizeTtlSeconds` accepts.
    if (frozenUntil > Number.MAX_SAFE_INTEGER) {
      frozenUntil = Number.MAX_SAFE_INTEGER;
    }
    // Derived and validated before the fields are set, as `writeHash` validates
    // its own: `expire` raises, and would otherwise leave the freeze half-written.
    const ttl = normalizeTtlSeconds(
      Math.max(Math.ceil((frozenUntil - Date.now()) / 1000) + 60, 60)
    );
    const hash = this.hash(key);
    hash.set("frozenUntil", String(frozenUntil));
    hash.set("thawRequestCount", String(thawRequestCount));
    this.expire(key, ttl);
    return { frozenUntil, thawRequestCount };
  }

  /**
   * The body of {@link getFreezeState}, synchronous so that the two methods that
   * decide on it do not sample the clock a suspension later than the state. See
   * the Redis twin's one-script `readFreezeState`.
   */
  private readFreezeState(key: string): FreezeState | null {
    const raw = this.hget(key, "frozenUntil");
    if (raw === undefined) return null;
    const frozenUntil = usableDeadline(raw);
    if (frozenUntil === undefined) return null;
    const thawRequestCount = parseFiniteStored(
      this.hget(key, "thawRequestCount"),
      0
    );
    // Reports expiry without deleting, matching Redis. See the comment there.
    if (Date.now() >= frozenUntil && thawRequestCount <= 0) return null;
    return { frozenUntil, thawRequestCount };
  }

  async getFreezeState(key: string): Promise<FreezeState | null> {
    return this.readFreezeState(key);
  }

  async updateThawProgress(
    key: string,
    success: boolean,
    completionId?: string
  ): Promise<FreezeState | null> {
    // Read and write with no `await` between them: this backend's atomicity is
    // a property of the language, but only for code that does not suspend.
    const completionKey = `${key}:thawCompletions`;
    // Recorded before the freeze state is read, as the Lua twin records it: a
    // completion delivered while no freeze stands is still remembered, so a
    // redelivery of it cannot decrement a freeze armed afterwards. Checked through
    // `live`, not `setOf`, so the read does not create the set it is asking about.
    const alreadySeen =
      completionId !== undefined &&
      completionId !== "" &&
      this.live(completionKey)?.set?.has(completionId) === true;
    if (completionId && !alreadySeen) {
      this.setOf(completionKey).add(completionId);
      this.expire(completionKey, 86400);
    }

    const raw = this.hget(key, "frozenUntil");
    if (raw === undefined) return null;
    const frozenUntil = usableDeadline(raw);
    if (frozenUntil === undefined) return null;
    const thawRequestCount = parseFiniteStored(
      this.hget(key, "thawRequestCount"),
      0
    );
    if (alreadySeen) return { frozenUntil, thawRequestCount };

    if (Date.now() >= frozenUntil && thawRequestCount <= 0) {
      // The one place expiry deletes the key; a read must not.
      this.store.delete(key);
      return null;
    }
    if (!success) return { frozenUntil, thawRequestCount };

    const newCount = thawRequestCount - 1;
    if (newCount <= 0) {
      this.store.delete(key);
      return null;
    }
    this.hash(key).set("thawRequestCount", String(newCount));
    return { frozenUntil, thawRequestCount: newCount };
  }

  async clearFreezeState(key: string): Promise<void> {
    this.store.delete(key);
  }

  async isFrozen(key: string): Promise<boolean> {
    const state = this.readFreezeState(key);
    if (!state) return false;
    if (Date.now() < state.frozenUntil) return true;
    return state.thawRequestCount > 0;
  }

  async canProcessRequest(key: string): Promise<{
    canProcess: boolean;
    isThawRequest: boolean;
    frozenUntil?: number;
  }> {
    const state = this.readFreezeState(key);
    if (!state) return { canProcess: true, isThawRequest: false };
    // `frozenUntil` accompanies both outcomes so a caller can book a wake-up
    // against the deadline rather than a locally computed delay. Here the two
    // clocks are the same one; the Redis twin is where it matters.
    if (Date.now() < state.frozenUntil) {
      return {
        canProcess: false,
        isThawRequest: false,
        frozenUntil: state.frozenUntil,
      };
    }
    return {
      canProcess: true,
      isThawRequest: true,
      frozenUntil: state.frozenUntil,
    };
  }

  async hasThawRequestInProgress(
    queueKey: string,
    metadataKeyPrefix: string,
    grantId: string
  ): Promise<boolean> {
    for (const requestId of this.sorted(queueKey)) {
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;
      // Normalised exactly as `tryStartThawRequest` normalises it, and as the
      // Lua twin's `fields[2] or ''` does: a client-level probe carries no
      // `grantId` field, so an absent one has to match its own kind.
      const otherGrantId = this.hget(metadataKey, "grantId") ?? "";
      if (
        this.hget(metadataKey, "status") === "inProgress" &&
        otherGrantId === grantId &&
        this.hget(metadataKey, "isThawRequest") === "true"
      ) {
        return true;
      }
    }
    return false;
  }

  async tryStartThawRequest(
    frozenGrantsKey: string,
    queueKey: string,
    metadataKeyPrefix: string,
    requestId: string,
    grantId: string
  ): Promise<"started" | "exists"> {
    // A client-level thaw has no grant to track; the frozen-grants set exists
    // so getNextRequest can skip individual grants.
    if (grantId !== "") this.setOf(frozenGrantsKey).add(grantId);

    for (const otherId of this.sorted(queueKey)) {
      if (otherId === requestId) continue;
      const metadataKey = `${metadataKeyPrefix}:${otherId}`;
      const otherGrantId = this.hget(metadataKey, "grantId") ?? "";
      if (
        this.hget(metadataKey, "status") === "inProgress" &&
        otherGrantId === grantId &&
        this.hget(metadataKey, "isThawRequest") === "true"
      ) {
        return "exists";
      }
    }

    // Only marks an entry that still exists — `hash()` would otherwise create one
    // that no cleanup path can reach. See the Lua twin's EXISTS guard.
    const metadataKey = `${metadataKeyPrefix}:${requestId}`;
    if (!this.live(metadataKey)?.hash) return "exists";
    this.hash(metadataKey).set("isThawRequest", "true");
    return "started";
  }

  async cleanupStaleFrozenGrants(
    frozenGrantsKey: string,
    queueKey: string,
    metadataKeyPrefix: string,
    freezeStateKeyPrefix: string
  ): Promise<number> {
    const now = Date.now();
    const probing = new Set<string>();
    for (const requestId of this.sorted(queueKey)) {
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;
      const grantId = this.hget(metadataKey, "grantId");
      if (
        this.hget(metadataKey, "status") === "inProgress" &&
        this.hget(metadataKey, "isThawRequest") === "true" &&
        grantId !== undefined
      ) {
        probing.add(grantId);
      }
    }

    let cleaned = 0;
    for (const grantId of [...(this.live(frozenGrantsKey)?.set ?? [])]) {
      if (probing.has(grantId)) continue;
      const raw = this.hget(
        `${freezeStateKeyPrefix}${grantId}:freezeState`,
        "frozenUntil"
      );
      if (raw === undefined || now >= parseFiniteStored(raw, 0)) {
        this.live(frozenGrantsKey)?.set?.delete(grantId);
        cleaned++;
      }
    }
    return cleaned;
  }

  // --------------------------------------------------------------- instances

  async getInstances(
    instanceSetKey: string,
    instanceKeyPrefix: string,
    currentInstanceId: string
  ): Promise<Array<{ id: string; data: string }>> {
    const instances: Array<{ id: string; data: string }> = [];
    const set = this.live(instanceSetKey)?.set;
    if (!set) return instances;
    for (const id of [...set]) {
      if (id === currentInstanceId) continue;
      const data = this.live(`${instanceKeyPrefix}${id}`)?.str;
      if (data) instances.push({ id, data });
      else set.delete(id);
    }
    return instances;
  }

  // --------------------------------------------------------------- lifecycle

  async now(): Promise<number> {
    return Date.now();
  }

  async close(): Promise<void> {
    if (this.sweeper) clearInterval(this.sweeper);
    this.sweeper = null;
    this.bus.removeAllListeners();
    this.busListening = false;
    this.subscribed.clear();
    this.store.clear();
  }
}

/**
 * Coordinates in local memory — **single process only**.
 *
 * Each process gets its own copy of every rate limit, so running two of them
 * doubles what the vendor sees. Use {@link redisBackend} for anything that
 * scales past one process. See {@link MemoryBackend} for the full trade-offs.
 */
export function memoryBackend(): DianemoBackend {
  return new MemoryBackend();
}

export default MemoryBackend;
