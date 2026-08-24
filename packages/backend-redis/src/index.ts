import { createContext, type Ctx } from "./context.js";
import { concurrencyOps } from "./ops/concurrency.js";
import { tokenBucketOps } from "./ops/tokenBucket.js";
import { normalizeTtlSeconds } from "@dianemo/core";
import { instanceOps } from "./ops/instances.js";
import { freezeOps } from "./ops/freeze.js";
import { queueOps } from "./ops/queue.js";
import type { Redis } from "ioredis";
import type {
  BatchOp,
  BatchOptions,
  DianemoBackend,
  MessageHandler,
} from "@dianemo/core";

/**
 * Whole milliseconds for `SET ... PX`, which rejects a fractional argument.
 *
 * Rounds up for the reason `normalizeTtlSeconds` does, and throws on a value
 * Redis has no lifetime for rather than taking the lock forever: `PX 0` and a
 * negative are both `ERR invalid expire time`, and the memory twin throws too.
 */
function normalizeLockTtlMs(ttlMs: number): number {
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
    throw new RangeError(
      `ttlMs must be a finite number greater than 0, received ${String(ttlMs)}`
    );
  }
  return Math.ceil(ttlMs);
}

/**
 * Redis-backed coordination: one budget across every replica. See
 * docs/backends/redis.md.
 *
 * The connection belongs to the caller. This backend opens exactly one thing of its
 * own, a duplicate for pub/sub, and `close()` closes only that.
 *
 * `DianemoBackend` is a flat interface, so this class has to present every method
 * on one object — but the bodies do not have to live here. Each rate-limiting,
 * queue and freeze operation is implemented once in `ops/`, grouped the same way
 * the Lua in `lua/` is, and delegated to below by a method that takes its
 * parameters from the interface. Only the delegation is repeated.
 *
 * Those delegations are methods rather than arrow-function fields on purpose. A
 * field is an own property of the instance, and an own property shadows a
 * subclass's prototype method — so with fields, a subclass overriding
 * `addRequest` would be silently ignored while its override of `get` (a real
 * method) worked, which is a worse failure than none of them working.
 */
class RedisBackend implements DianemoBackend {
  readonly kind = "redis";
  readonly distributed = true;

  private redis: Redis;
  private ctx: Ctx;
  private listener: Redis | null = null;
  private subscribed = new Map<string, Set<MessageHandler>>();
  private ops: {
    tokenBucket: ReturnType<typeof tokenBucketOps>;
    concurrency: ReturnType<typeof concurrencyOps>;
    queue: ReturnType<typeof queueOps>;
    freeze: ReturnType<typeof freezeOps>;
    instances: ReturnType<typeof instanceOps>;
  };

  constructor(redis: Redis) {
    this.redis = redis;
    this.ctx = createContext(redis);
    this.ops = {
      tokenBucket: tokenBucketOps(this.ctx),
      concurrency: concurrencyOps(this.ctx),
      queue: queueOps(this.ctx),
      freeze: freezeOps(this.ctx),
      instances: instanceOps(this.ctx),
    };
  }

  // ---------------------------------------------------------------- store

  async get(key: string): Promise<string | null> {
    return this.redis.get(key);
  }

  async set(key: string, value: string, ttlSeconds?: number): Promise<void> {
    if (ttlSeconds === undefined) {
      await this.redis.set(key, value);
      return;
    }
    const ttl = normalizeTtlSeconds(ttlSeconds);
    // `SET key v EX 0` is not `EXPIRE key 0`: Redis rejects it as an invalid
    // expire time and leaves the old value in place, so a TTL the contract reads
    // as "expire immediately" is applied as a delete. See the memory twin.
    if (ttl <= 0) {
      await this.redis.del(key);
      return;
    }
    await this.redis.set(key, value, "EX", ttl);
  }

  async del(...keys: string[]): Promise<void> {
    if (keys.length) await this.redis.del(...keys);
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    return this.redis.hgetall(key);
  }

  async hset(
    key: string,
    fields: Record<string, string | number>,
    ttlSeconds?: number
  ): Promise<void> {
    // Validated before the write, not after: a pipeline is not a transaction, so a
    // TTL Redis rejects still leaves the `HSET` applied — and for a credential hash
    // that is an encrypted refresh token persisted with no expiry at all.
    const ttl =
      ttlSeconds === undefined ? undefined : normalizeTtlSeconds(ttlSeconds);
    // Only the `HSET` is skipped when there is nothing to write: `HSET key` with
    // no field/value pairs is an arity error rather than a no-op, and under
    // `batch({atomic: true})` that error aborts the whole MULTI. The expiry
    // still applies, because the memory twin moves an existing hash's TTL
    // whether or not the call also writes a field.
    const hasFields = Object.keys(fields).length > 0;
    if (ttl === undefined) {
      if (hasFields) await this.redis.hset(key, fields);
      return;
    }
    const pipeline = this.redis.pipeline();
    if (hasFields) pipeline.hset(key, fields);
    pipeline.expire(key, ttl);
    // `exec()` resolves even when individual commands failed — their errors are
    // per-entry, not thrown.
    const results = await pipeline.exec();
    const failed = results?.find(([error]) => error);
    if (failed?.[0]) throw failed[0];
  }

  async sadd(key: string, member: string): Promise<void> {
    await this.redis.sadd(key, member);
  }

  async srem(key: string, member: string): Promise<void> {
    await this.redis.srem(key, member);
  }

  async smembers(key: string): Promise<string[]> {
    return this.redis.smembers(key);
  }

  async batch(ops: BatchOp[], options: BatchOptions = {}): Promise<void> {
    if (!ops.length) return;
    // Every TTL is read before the first command is queued. `EXEC` does not roll
    // back a command that fails at execution time, so a TTL rejected halfway
    // through would leave the ops before it applied — the torn group `atomic` is
    // asked for precisely to prevent.
    const ttls = new Map<BatchOp, number>();
    for (const op of ops) {
      if ("ttlSeconds" in op && op.ttlSeconds !== undefined) {
        ttls.set(op, normalizeTtlSeconds(op.ttlSeconds));
      }
    }
    // MULTI when a torn read would mislead a peer, a plain pipeline otherwise.
    // Both are one round-trip; MULTI additionally blocks other clients from
    // interleaving, which is only worth asking for when it matters.
    const chain = options.atomic ? this.redis.multi() : this.redis.pipeline();
    for (const op of ops) {
      const ttl = ttls.get(op);
      switch (op.op) {
        case "set":
          // A non-positive TTL is a delete here for the reason `set` above
          // carries.
          if (ttl === undefined) chain.set(op.key, op.value);
          else if (ttl <= 0) chain.del(op.key);
          else chain.set(op.key, op.value, "EX", ttl);
          break;
        case "del":
          chain.del(op.key);
          break;
        case "expire":
          chain.expire(op.key, ttl ?? op.ttlSeconds);
          break;
        case "sadd":
          chain.sadd(op.key, op.member);
          break;
        case "srem":
          chain.srem(op.key, op.member);
          break;
        case "hset":
          // An empty `fields` skips the write but keeps the expiry, for the
          // reason `hset` above carries.
          if (Object.keys(op.fields).length) chain.hset(op.key, op.fields);
          if (ttl !== undefined) chain.expire(op.key, ttl);
          break;
        case "publish":
          chain.publish(op.channel, op.message);
          break;
      }
    }
    const results = await chain.exec();
    const failed = results?.find(([error]) => error);
    if (failed?.[0]) throw failed[0];
  }

  // ---------------------------------------------------------------- locks

  async acquireLock(
    key: string,
    token: string,
    ttlMs: number
  ): Promise<boolean> {
    const result = await this.redis.set(
      key,
      token,
      "PX",
      normalizeLockTtlMs(ttlMs),
      "NX"
    );
    return result === "OK";
  }

  async releaseLock(key: string, token: string): Promise<boolean> {
    // Checking then deleting from the client would let an expired lock be
    // deleted after someone else had already taken it.
    const script = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      else
        return 0
      end
    `;
    const result = await this.redis.eval(script, 1, key, token);
    return result === 1;
  }

  // --------------------------------------------------------------- pub/sub

  async publish(channel: string, message: string): Promise<void> {
    await this.redis.publish(channel, message);
  }

  async subscribe(channels: string[], handler: MessageHandler): Promise<void> {
    if (!this.listener) {
      // A subscribed connection cannot run ordinary commands, so pub/sub needs
      // its own. This duplicate is the only connection the backend owns.
      this.listener = this.redis.duplicate();
      // One `message` listener for the connection's lifetime, dispatching from
      // the map below: one connection delivers every message to every `message`
      // listener on it, so a listener per call would both re-deliver another
      // call's channels and grow the emitter without bound. Twin of the memory
      // backend's `subscribed` map.
      this.listener.on("message", (channel: string, message: string) => {
        // Copied, so a handler that subscribes while dispatching cannot mutate
        // the set being iterated.
        for (const h of [...(this.subscribed.get(channel) ?? [])]) {
          h(channel, message);
        }
      });
    }
    for (const channel of channels) {
      let handlers = this.subscribed.get(channel);
      if (!handlers) {
        handlers = new Set();
        this.subscribed.set(channel, handlers);
      }
      handlers.add(handler);
    }
    await this.listener.subscribe(...channels);
  }

  // ---------------------------------------------------------------- clock

  now() {
    return this.ctx.now();
  }

  // --------------------------------------------------------- token bucket

  tryAdmitImmediately(...a: Parameters<DianemoBackend["tryAdmitImmediately"]>) {
    return this.ops.tokenBucket.tryAdmitImmediately(...a);
  }
  acquireTokens(...a: Parameters<DianemoBackend["acquireTokens"]>) {
    return this.ops.tokenBucket.acquireTokens(...a);
  }
  getTokenBucketState(...a: Parameters<DianemoBackend["getTokenBucketState"]>) {
    return this.ops.tokenBucket.getTokenBucketState(...a);
  }
  resetTokenBucket(...a: Parameters<DianemoBackend["resetTokenBucket"]>) {
    return this.ops.tokenBucket.resetTokenBucket(...a);
  }
  // NonNullable because this member is OPTIONAL on the interface, so
  // `Parameters<DianemoBackend["refundTokens"]>` would resolve against
  // `undefined`. Same for acquireQueuedConcurrency.
  refundTokens(...a: Parameters<NonNullable<DianemoBackend["refundTokens"]>>) {
    return this.ops.tokenBucket.refundTokens(...a);
  }

  // ---------------------------------------------------------- concurrency

  tryAdmitConcurrency(...a: Parameters<DianemoBackend["tryAdmitConcurrency"]>) {
    return this.ops.concurrency.tryAdmitConcurrency(...a);
  }
  acquireConcurrency(...a: Parameters<DianemoBackend["acquireConcurrency"]>) {
    return this.ops.concurrency.acquireConcurrency(...a);
  }
  acquireQueuedConcurrency(
    ...a: Parameters<NonNullable<DianemoBackend["acquireQueuedConcurrency"]>>
  ) {
    return this.ops.concurrency.acquireQueuedConcurrency(...a);
  }
  releaseConcurrency(...a: Parameters<DianemoBackend["releaseConcurrency"]>) {
    return this.ops.concurrency.releaseConcurrency(...a);
  }
  getConcurrencyState(...a: Parameters<DianemoBackend["getConcurrencyState"]>) {
    return this.ops.concurrency.getConcurrencyState(...a);
  }
  clearConcurrency(...a: Parameters<DianemoBackend["clearConcurrency"]>) {
    return this.ops.concurrency.clearConcurrency(...a);
  }

  // ---------------------------------------------------------------- queue

  addRequest(...a: Parameters<DianemoBackend["addRequest"]>) {
    return this.ops.queue.addRequest(...a);
  }
  getQueueLength(...a: Parameters<DianemoBackend["getQueueLength"]>) {
    return this.ops.queue.getQueueLength(...a);
  }
  getNextRequest(...a: Parameters<DianemoBackend["getNextRequest"]>) {
    return this.ops.queue.getNextRequest(...a);
  }
  getRequest(...a: Parameters<DianemoBackend["getRequest"]>) {
    return this.ops.queue.getRequest(...a);
  }
  updateRequest(...a: Parameters<DianemoBackend["updateRequest"]>) {
    return this.ops.queue.updateRequest(...a);
  }
  removeRequest(...a: Parameters<DianemoBackend["removeRequest"]>) {
    return this.ops.queue.removeRequest(...a);
  }
  getQueueStats(...a: Parameters<DianemoBackend["getQueueStats"]>) {
    return this.ops.queue.getQueueStats(...a);
  }
  getAllRequests(...a: Parameters<DianemoBackend["getAllRequests"]>) {
    return this.ops.queue.getAllRequests(...a);
  }
  cleanupOrphanedRequests(
    ...a: Parameters<DianemoBackend["cleanupOrphanedRequests"]>
  ) {
    return this.ops.queue.cleanupOrphanedRequests(...a);
  }

  // -------------------------------------------------------- freeze / thaw

  tryAdmitNoLimit(...a: Parameters<DianemoBackend["tryAdmitNoLimit"]>) {
    return this.ops.freeze.tryAdmitNoLimit(...a);
  }
  setFreezeState(...a: Parameters<DianemoBackend["setFreezeState"]>) {
    return this.ops.freeze.setFreezeState(...a);
  }
  armFreeze(...a: Parameters<DianemoBackend["armFreeze"]>) {
    return this.ops.freeze.armFreeze(...a);
  }
  getFreezeState(...a: Parameters<DianemoBackend["getFreezeState"]>) {
    return this.ops.freeze.getFreezeState(...a);
  }
  updateThawProgress(...a: Parameters<DianemoBackend["updateThawProgress"]>) {
    return this.ops.freeze.updateThawProgress(...a);
  }
  clearFreezeState(...a: Parameters<DianemoBackend["clearFreezeState"]>) {
    return this.ops.freeze.clearFreezeState(...a);
  }
  isFrozen(...a: Parameters<DianemoBackend["isFrozen"]>) {
    return this.ops.freeze.isFrozen(...a);
  }
  canProcessRequest(...a: Parameters<DianemoBackend["canProcessRequest"]>) {
    return this.ops.freeze.canProcessRequest(...a);
  }
  hasThawRequestInProgress(
    ...a: Parameters<DianemoBackend["hasThawRequestInProgress"]>
  ) {
    return this.ops.freeze.hasThawRequestInProgress(...a);
  }
  tryStartThawRequest(...a: Parameters<DianemoBackend["tryStartThawRequest"]>) {
    return this.ops.freeze.tryStartThawRequest(...a);
  }
  cleanupStaleFrozenGrants(
    ...a: Parameters<DianemoBackend["cleanupStaleFrozenGrants"]>
  ) {
    return this.ops.freeze.cleanupStaleFrozenGrants(...a);
  }

  // ------------------------------------------------------------ instances

  getInstances(...a: Parameters<DianemoBackend["getInstances"]>) {
    return this.ops.instances.getInstances(...a);
  }

  // --------------------------------------------------------------- lifecycle

  async close(): Promise<void> {
    // Dropped with the connection they were reached through: a later
    // `subscribe` opens a fresh duplicate, and a handler stopped and replaced
    // on one backend must not deliver to its predecessor's handlers. The memory
    // twin clears its map here for the same reason.
    this.subscribed.clear();
    if (!this.listener) return;
    // Cleared first, so a second close cannot quit the same connection twice.
    const listener = this.listener;
    this.listener = null;
    // `quit()` sends QUIT and waits for the reply, which never arrives on a
    // connection that never reached the server — closing after a failed start hung
    // instead of releasing it. `disconnect()` tears down without a round trip.
    if (listener.status === "ready") {
      await listener.quit().catch(() => listener.disconnect());
      return;
    }
    listener.disconnect();
  }
}

/**
 * Coordinates through Redis — the right choice for anything running more than
 * one process, and a safe choice for one.
 *
 * @param redis an ioredis connection you own and will close yourself
 */
export function redisBackend(redis: Redis): DianemoBackend {
  return new RedisBackend(redis);
}

export default RedisBackend;
