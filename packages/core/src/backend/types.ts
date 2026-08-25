export interface TokenBucketConfig {
  maxTokens: number;
  tokensToAdd: number;
  /** ms between token additions */
  interval: number;
}

export interface ConcurrencyConfig {
  maxConcurrency: number;
  /** ms before a slot expires — reclaims slots held by a crashed process */
  requestTtl: number;
}

export interface AcquireTokensResult {
  acquired: boolean;
  remainingTokens?: number;
  /** ms to wait before retrying, when not acquired */
  waitTime?: number;
  /** set when the configuration itself is unusable */
  error?: string;
}

export interface AcquireConcurrencyResult {
  acquired: boolean;
  currentConcurrency?: number;
  /**
   * Set when the ceiling itself is unusable, as opposed to merely full.
   *
   * Without this the two cases were indistinguishable to the caller, so a
   * `maxConcurrency` of `NaN` or `0` looked exactly like a busy client and
   * wedged it silently. Mirrors `AcquireTokensResult.error`.
   */
  error?: string;
}

/**
 * One budget a request must claim, named by the key it lives under.
 *
 * A client with several limits presents the whole list to the backend at once,
 * because claiming them one at a time is not the same operation: a peer can
 * take the second budget between two calls, leaving the first spent on a
 * request that is then declined. See docs/rate-limits/multiple-limits.md.
 */
export type MultiLimitSpec = {
  /**
   * The freeze governing this budget, which is not always the spender's own: a
   * `sharedLimit` entry draws on another client's balance, and that client's
   * freeze is what says the balance may not be spent.
   */
  freezeKey?: string;
} & (
  | { kind: "tokenBucket"; key: string; config: TokenBucketConfig }
  | {
      kind: "concurrency";
      key: string;
      config: ConcurrencyConfig;
      /** Member id the slot is recorded under; released by the same id. */
      slotId: string;
    }
);

export interface AcquireMultiLimitResult {
  acquired: boolean;
  /** ms to wait before retrying — the longest any declining budget asked for. */
  waitTime?: number;
  /** Set when one of the configurations is unusable, as opposed to merely full. */
  error?: string;
  /** Key of the budget that declined, for logs. Unset when every one admitted. */
  blockedBy?: string;
}

export interface QueuedRequest {
  requestId: string;
  clientName: string;
  requestName: string;
  status: "pending" | "inProgress";
  priority: number;
  cost: number;
  retries: number;
  timestamp: number;
  grantId?: string;
  isThawRequest: boolean;
  ownerId: string;
}

export interface FreezeState {
  frozenUntil: number;
  thawRequestCount: number;
}

export interface QueueStats {
  pending: number;
  inProgress: number;
  totalCost: number;
}

/** A single write in a {@link DianemoBackend.batch} group. */
export type BatchOp =
  | { op: "set"; key: string; value: string; ttlSeconds?: number }
  | { op: "del"; key: string }
  | { op: "expire"; key: string; ttlSeconds: number }
  | { op: "sadd"; key: string; member: string }
  | { op: "srem"; key: string; member: string }
  | {
      op: "hset";
      key: string;
      fields: Record<string, string | number>;
      ttlSeconds?: number;
    }
  | { op: "publish"; channel: string; message: string };

export interface BatchOptions {
  /**
   * Apply the group without another client interleaving, so a reader observes it
   * either before or after but never mid-way. Costs a little more than a plain
   * batch, so it is opt-in — use it where a torn read would actually mislead a peer.
   *
   * Isolation, not rollback: Redis `MULTI`/`EXEC` runs the remaining commands after
   * one of them fails and keeps the effects of those that succeeded. Backends
   * therefore validate what they can before the first write, but an op that fails on
   * the server — a `WRONGTYPE` from a key another service owns — still leaves its
   * siblings applied. Group ops that must not tear behind one key you control.
   */
  atomic?: boolean;
}

/** Receives messages for channels passed to {@link DianemoBackend.subscribe}. */
export type MessageHandler = (channel: string, message: string) => void;

/**
 * The storage and coordination contract every dianemo backend implements.
 *
 * Domain operations rather than storage primitives, because each one must be atomic
 * with respect to other spenders and every backend arranges that differently. Keys
 * are built by the caller, so key layout stays the handler's business.
 *
 * Implementing one: docs/design-notes.md#two-backends-one-contract.
 */
export interface DianemoBackend {
  /** Names the implementation for a host to log or branch on, e.g. `"redis"`. */
  readonly kind: string;

  /**
   * Whether this backend shares state between processes. `false` means the budget it
   * enforces covers **this process only**, so N processes send N times the agreed rate.
   *
   * Declared for a host to inspect — nothing in dianemo reads it, so this is not a
   * guard against running a single-process backend across replicas.
   */
  readonly distributed: boolean;

  // ---------------------------------------------------------------- store

  get(key: string): Promise<string | null>;
  set(key: string, value: string, ttlSeconds?: number): Promise<void>;
  del(...keys: string[]): Promise<void>;
  hgetall(key: string): Promise<Record<string, string>>;
  hset(
    key: string,
    fields: Record<string, string | number>,
    ttlSeconds?: number
  ): Promise<void>;
  sadd(key: string, member: string): Promise<void>;
  srem(key: string, member: string): Promise<void>;
  smembers(key: string): Promise<string[]>;

  /** Applies several writes together — one round-trip where that is a concept. */
  batch(ops: BatchOp[], options?: BatchOptions): Promise<void>;

  // ---------------------------------------------------------------- locks

  /**
   * Takes `key` for `ttlMs` if it is free. `token` identifies the holder so
   * that only the holder can release it.
   */
  acquireLock(key: string, token: string, ttlMs: number): Promise<boolean>;
  /** Releases `key` only if `token` still holds it. */
  releaseLock(key: string, token: string): Promise<boolean>;

  // --------------------------------------------------------------- pub/sub

  publish(channel: string, message: string): Promise<void>;
  /**
   * Starts delivering messages on `channels` to `handler`. Called once during
   * handler startup; the backend owns whatever connection that needs.
   */
  subscribe(channels: string[], handler: MessageHandler): Promise<void>;

  // ---------------------------------------------------- admission (fast path)

  /**
   * Admits a request immediately, or declines — never queues, never blocks.
   *
   * Declining is not a failure; it means the caller should take the queued
   * path. Must refuse whenever anything is already queued, so a new arrival
   * can never overtake work that has been waiting.
   */
  tryAdmitImmediately(
    queueKey: string,
    bucketKey: string,
    freezeKey: string,
    cost: number,
    config: TokenBucketConfig,
    ttl?: number
  ): Promise<boolean>;

  /**
   * The same decision for a client with no budget: admit unless something is queued
   * or a freeze or probe stands. One call, because answering the two separately lets
   * a peer enqueue in between and be overtaken.
   */
  tryAdmitNoLimit(queueKey: string, freezeKey: string): Promise<boolean>;

  tryAdmitConcurrency(
    queueKey: string,
    concurrencyKey: string,
    freezeKey: string,
    cost: number,
    requestId: string,
    config: ConcurrencyConfig,
    ttl?: number
  ): Promise<boolean>;

  // ------------------------------------------------------------ token bucket

  acquireTokens(
    key: string,
    cost: number,
    config: TokenBucketConfig
  ): Promise<AcquireTokensResult>;
  getTokenBucketState(
    key: string,
    config: TokenBucketConfig
  ): Promise<{ tokens: number; lastUpdate: number }>;
  resetTokenBucket(key: string, maxTokens: number): Promise<void>;
  /** Returns budget claimed for a request that was never dispatched. */
  refundTokens?(
    key: string,
    cost: number,
    maxTokens: number,
    freezeKey?: string
  ): Promise<void>;

  // ------------------------------------------------------------ several budgets

  /**
   * Claims every listed budget, or none of them.
   *
   * Optional so a backend written against an earlier version still satisfies the
   * interface; a client declaring several limits refuses to start without it,
   * rather than silently metering against one budget. All-or-nothing is the whole
   * contract: a partial claim leaks budget that no completion path hands back.
   */
  acquireMultiLimit?(
    specs: MultiLimitSpec[],
    cost: number
  ): Promise<AcquireMultiLimitResult>;
  /**
   * Hands back what `acquireMultiLimit` claimed. Token buckets are credited and
   * concurrency slots released, so it is also the completion path for the slots.
   *
   * A spec's own `freezeKey` suppresses its refund while that freeze stands,
   * matching {@link refundTokens} — a freeze empties buckets on purpose.
   */
  releaseMultiLimit?(specs: MultiLimitSpec[], cost: number): Promise<void>;
  /**
   * The no-queue fast path for several budgets: admit only if nothing is queued,
   * none of `freezeKeys` stands, and every budget can be claimed at once.
   */
  tryAdmitMultiLimit?(
    queueKey: string,
    freezeKeys: string[],
    specs: MultiLimitSpec[],
    cost: number,
    ttl?: number
  ): Promise<boolean>;

  // ------------------------------------------------------------- concurrency

  acquireConcurrency(
    key: string,
    cost: number,
    requestId: string,
    config: ConcurrencyConfig
  ): Promise<AcquireConcurrencyResult>;
  /**
   * Claims capacity only while the selected queue entry still exists in the
   * in-progress state. The check and claim must be atomic.
   */
  acquireQueuedConcurrency?(
    key: string,
    metadataKey: string,
    cost: number,
    requestId: string,
    config: ConcurrencyConfig
  ): Promise<AcquireConcurrencyResult>;
  releaseConcurrency(key: string, requestId: string): Promise<void>;
  getConcurrencyState(
    key: string,
    requestTtl: number
  ): Promise<{ currentConcurrency: number; activeRequests: string[] }>;
  clearConcurrency(key: string): Promise<void>;

  // ------------------------------------------------------------------- queue

  /**
   * Returns false when `requestId` is already queued, so retries are safe, and false
   * when it has been removed recently — see {@link removeRequest}.
   *
   * That second refusal is what makes the ordering safe without trusting the
   * transport. An abandonment broadcast can be delivered before an outstanding add
   * commits, and the removal it triggers then finds nothing to remove; without the
   * refusal this call would go on to create an entry nobody awaits, which orphan
   * cleanup spares (its owner is alive) and which pins the queue non-empty, disabling
   * the fast path for the whole client.
   *
   * A retry is unaffected: it keeps its entry rather than removing and re-adding, so
   * a removal and a later add of one id never both happen.
   */
  addRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    request: QueuedRequest,
    ttl?: number
  ): Promise<boolean>;
  getNextRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    skipGrantIds?: string[],
    /**
     * Request ids to pass over. Lets the admission loop step past a head it has
     * already judged unsatisfiable and reach the work behind it, instead of
     * being handed the same entry every iteration.
     */
    skipRequestIds?: string[]
  ): Promise<QueuedRequest | null>;
  getRequest(
    metadataKeyPrefix: string,
    requestId: string
  ): Promise<QueuedRequest | null>;
  updateRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    requestId: string,
    updates: Partial<QueuedRequest>
  ): Promise<void>;
  /**
   * Removes the entry and marks the id as removed for `tombstoneTtlSeconds`, so a
   * later {@link addRequest} for it is refused.
   *
   * The mark is written even when there was no entry to remove — that is the case it
   * exists for.
   *
   * @param tombstoneTtlSeconds How long the mark stands, defaulting to
   * `REQUEST_TOMBSTONE_TTL_SECONDS`. It has to outlast the window in which an add
   * can still be outstanding, which is the caller's admission budget rather than a
   * constant, so a caller that lets a request wait longer must pass a longer value.
   */
  removeRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    requestId: string,
    tombstoneTtlSeconds?: number
  ): Promise<{ wasThawRequest: boolean }>;
  getQueueStats(
    queueKey: string,
    metadataKeyPrefix: string
  ): Promise<QueueStats>;
  /**
   * Depth without reading any metadata, unlike `getQueueStats`. For host
   * observability: the fast paths ask the same question inside their own scripts.
   */
  getQueueLength(queueKey: string): Promise<number>;
  getAllRequests(
    queueKey: string,
    metadataKeyPrefix: string
  ): Promise<QueuedRequest[]>;
  /**
   * Drops requests owned by instances that are no longer alive.
   *
   * Refused outright unless `aliveInstanceIds` contains `currentInstanceId`: an
   * instance registers itself on every heartbeat, so a set lacking the caller's
   * own id proves the read is not authoritative rather than proving everyone
   * died, and sweeping on it deletes live replicas' queued work. Why this
   * resolves the opposite way to absent state elsewhere:
   * docs/design-notes.md#an-alive-instance-set-that-omits-the-sweeper-is-not-evidence.
   */
  cleanupOrphanedRequests(
    queueKey: string,
    metadataKeyPrefix: string,
    aliveInstanceIds: Set<string>,
    currentInstanceId: string
  ): Promise<number>;

  // ------------------------------------------------------------ freeze / thaw

  /**
   * Writes the freeze state to exactly these values.
   *
   * The wrong operation for arming a freeze — see {@link armFreeze}. It exists
   * for callers that mean "the state is now this", such as a fixture moving a
   * freeze from live to lapsed.
   */
  setFreezeState(
    key: string,
    frozenUntil: number,
    thawRequestCount: number
  ): Promise<void>;
  /**
   * Arms a freeze, keeping the longer deadline and larger probe budget, and returns
   * whichever is now in force — schedule wake-ups against that, not the requested
   * value.
   *
   * Must be atomic: a read-then-write lets two concurrent arms read the same old
   * value and the loser still win. Aliasing this to `setFreezeState` is wrong in a
   * way no behaviour test will show; run the conformance suite.
   */
  armFreeze(
    key: string,
    frozenUntil: number,
    thawRequestCount: number
  ): Promise<FreezeState>;
  getFreezeState(key: string): Promise<FreezeState | null>;
  updateThawProgress(
    key: string,
    success: boolean,
    completionId?: string
  ): Promise<FreezeState | null>;
  clearFreezeState(key: string): Promise<void>;
  isFrozen(key: string): Promise<boolean>;
  /**
   * Whether the next request may go, and whether it would be the thaw probe.
   *
   * `frozenUntil` is reported whenever freeze state exists, on the BACKEND's
   * clock, so a caller declining a request can book its retry against the
   * deadline the fleet honours instead of subtracting a local clock from a
   * remote timestamp. Absent only when there is no freeze state at all.
   */
  canProcessRequest(key: string): Promise<{
    canProcess: boolean;
    isThawRequest: boolean;
    frozenUntil?: number;
  }>;
  hasThawRequestInProgress(
    queueKey: string,
    metadataKeyPrefix: string,
    grantId: string
  ): Promise<boolean>;
  /**
   * Nominates exactly one probe request per grant while frozen.
   *
   * Returns `"started"` to the single winner and `"exists"` to everyone else.
   * A backend that lets two callers win turns a recovering vendor back into a
   * thundering herd, so this has to be genuinely exclusive.
   */
  tryStartThawRequest(
    frozenGrantsKey: string,
    queueKey: string,
    metadataKeyPrefix: string,
    requestId: string,
    grantId: string
  ): Promise<"started" | "exists">;
  cleanupStaleFrozenGrants(
    frozenGrantsKey: string,
    queueKey: string,
    metadataKeyPrefix: string,
    freezeStateKeyPrefix: string
  ): Promise<number>;

  // --------------------------------------------------------------- instances

  getInstances(
    instanceSetKey: string,
    instanceKeyPrefix: string,
    currentInstanceId: string
  ): Promise<Array<{ id: string; data: string }>>;

  /**
   * The clock the backend's own atomic operations are measured against.
   *
   * Anything written into shared state as a timestamp — `lastUpdate`,
   * `frozenUntil`, a concurrency slot's score — has to come from here, because
   * every replica compares against this clock and not its own.
   */
  now(): Promise<number>;

  // --------------------------------------------------------------- lifecycle

  /**
   * Releases whatever the backend itself opened. It must not close a
   * connection handed in by the caller — that belongs to them.
   */
  close(): Promise<void>;
}
