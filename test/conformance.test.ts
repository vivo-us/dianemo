import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import type { DianemoBackend } from "../packages/core/src/backend/types.js";
import { harnesses as clientTypeHarnesses } from "./clientTypes/harness.js";
import { memoryBackend } from "../packages/core/src/backend/memory.js";
import { redisBackend } from "../packages/backend-redis/src/index.js";
import { Redis } from "ioredis";

/**
 * One suite, run against every backend.
 *
 * Two backends that disagree about ordering, fairness or refill timing are two
 * different products wearing one name. Nothing here is Redis-specific or
 * memory-specific on purpose: these are the promises the README makes, and any
 * backend that cannot keep them is broken regardless of how it stores things.
 *
 * The memory backend always runs. The Redis backend runs when REDIS_URL is set:
 *   docker run -d -p 6399:6379 redis:7-alpine
 *   REDIS_URL=redis://localhost:6399 npm test
 */

const REDIS_URL = process.env.REDIS_URL;
const describeIfRedis = REDIS_URL ? describe : describe.skip;
const TEST_DB = 13;

interface Harness {
  name: string;
  create: () => Promise<DianemoBackend>;
  reset: (backend: DianemoBackend) => Promise<void>;
  destroy: (backend: DianemoBackend) => Promise<void>;
}

const harnesses: Harness[] = [
  {
    name: "memory",
    create: async () => memoryBackend(),
    // A fresh instance is the cheapest possible flush.
    reset: async () => {},
    destroy: async (backend) => backend.close(),
  },
];

if (REDIS_URL) {
  let redis: Redis | null = null;
  harnesses.push({
    name: "redis",
    create: async () => {
      redis = new Redis(`${REDIS_URL}/${TEST_DB}`);
      await redis.ping();
      await redis.flushdb();
      return redisBackend(redis);
    },
    reset: async () => {
      await redis?.flushdb();
    },
    destroy: async (backend) => {
      await backend.close();
      await redis?.quit();
      redis = null;
    },
  });
}

/**
 * Not a behaviour test: a guard on the run itself.
 *
 * CI's `test` job differs from `test-no-redis` only by `REDIS_URL`, so anything
 * that stops a Redis harness materialising reduces the two jobs to one run, both
 * green, and every "runs on both backends" claim in this file quietly becomes
 * memory-only. ioredis reconnects rather than throwing on an unreachable
 * address, so the connection is proved instead of assumed.
 */
describeIfRedis("redis harness", () => {
  it("materialises a redis harness whenever REDIS_URL is set", () => {
    expect(harnesses.map((h) => h.name)).toContain("redis");
    expect(clientTypeHarnesses(0).map((h) => h.name)).toContain("redis");
  });

  it("reaches the server REDIS_URL names", async () => {
    const probe = new Redis(REDIS_URL!, {
      lazyConnect: true,
      connectTimeout: 2000,
      maxRetriesPerRequest: 1,
      retryStrategy: () => null,
    });
    try {
      await probe.connect();
      expect(await probe.ping()).toBe("PONG");
    } finally {
      probe.disconnect();
    }
  });
});

describe.each(harnesses)("backend conformance — $name", (harness) => {
  let backend: DianemoBackend;

  beforeAll(async () => {
    backend = await harness.create();
  });
  afterEach(async () => {
    await harness.reset(backend);
    if (harness.name === "memory") {
      // No flush for an in-process map; take a clean one instead.
      await harness.destroy(backend);
      backend = await harness.create();
    }
  });
  afterAll(async () => {
    await harness.destroy(backend);
  });

  // ------------------------------------------------------------ token bucket

  describe("token bucket", () => {
    const KEY = "tb:rateLimit";
    const config = { maxTokens: 10, tokensToAdd: 10, interval: 10_000 };
    const drain = () => backend.acquireTokens(KEY, 10, config);

    it("spends exactly the cost and reports what is left", async () => {
      expect(await backend.acquireTokens(KEY, 3, config)).toMatchObject({
        acquired: true,
        remainingTokens: 7,
      });
      expect(await backend.acquireTokens(KEY, 7, config)).toMatchObject({
        acquired: true,
        remainingTokens: 0,
      });
      expect((await backend.acquireTokens(KEY, 1, config)).acquired).toBe(
        false
      );
    });

    it("refunds a claim for an attempt that was never dispatched", async () => {
      await backend.acquireTokens(KEY, 4, config);
      await backend.refundTokens!(KEY, 3, config.maxTokens);
      expect(await backend.getTokenBucketState(KEY, config)).toMatchObject({
        tokens: 9,
      });
      await backend.refundTokens!(KEY, 50, config.maxTokens);
      expect(await backend.getTokenBucketState(KEY, config)).toMatchObject({
        tokens: 10,
      });
    });

    /**
     * A freeze empties the bucket deliberately, so crediting a refund into a
     * frozen one hands back budget the fleet has just been told not to spend.
     * The refusal has to be decided against the backend's own clock, inside the
     * same operation as the write.
     */
    it("refuses a refund while a freeze is in force", async () => {
      const freezeKey = "tb:freezeState";
      await backend.acquireTokens(KEY, 4, config);
      await backend.armFreeze(freezeKey, (await backend.now()) + 60_000, 3);
      await backend.resetTokenBucket(KEY, 0);

      await backend.refundTokens!(KEY, 3, config.maxTokens, freezeKey);
      expect(await backend.getTokenBucketState(KEY, config)).toMatchObject({
        tokens: 0,
      });

      // Once it lapses, the same call is honoured again.
      await backend.clearFreezeState(freezeKey);
      await backend.refundTokens!(KEY, 3, config.maxTokens, freezeKey);
      expect(await backend.getTokenBucketState(KEY, config)).toMatchObject({
        tokens: 3,
      });
    });

    it("refuses rather than dividing by zero when tokensToAdd is 0", async () => {
      const res = await backend.acquireTokens(KEY, 1, {
        maxTokens: 10,
        tokensToAdd: 0,
        interval: 1000,
      });
      expect(res.acquired).toBe(false);
      expect(res.error).toBeTruthy();
    });

    it("counts only the time still to run, not the whole interval", async () => {
      await drain();
      // Nine of the ten seconds have already passed.
      await backend.hset(KEY, { lastUpdate: (await backend.now()) - 9_000 });

      const res = await backend.acquireTokens(KEY, 1, config);
      expect(res.acquired).toBe(false);
      // The caller sleeps on waitTime directly, so an over-estimate is not a
      // rounding error — it is a stall of exactly that length.
      expect(res.waitTime).toBeGreaterThan(500);
      expect(res.waitTime).toBeLessThan(2_000);
    });

    it("reports close to a full interval only when one just started", async () => {
      await drain();
      await backend.hset(KEY, { lastUpdate: await backend.now() });
      const res = await backend.acquireTokens(KEY, 1, config);
      expect(res.waitTime).toBeGreaterThan(9_000);
      expect(res.waitTime).toBeLessThanOrEqual(10_000);
    });

    it("never reports a negative wait", async () => {
      await drain();
      await backend.hset(KEY, { lastUpdate: (await backend.now()) - 60_000 });
      const res = await backend.acquireTokens(KEY, 1, config);
      expect(res.waitTime ?? 0).toBeGreaterThanOrEqual(0);
    });

    it("scales the wait with how many intervals the cost needs", async () => {
      await drain();
      await backend.hset(KEY, { lastUpdate: await backend.now() });
      // 25 tokens at 10 per interval needs three refills.
      const res = await backend.acquireTokens(KEY, 25, config);
      expect(res.acquired).toBe(false);
      expect(res.waitTime).toBeGreaterThan(2 * config.interval);
      expect(res.waitTime).toBeLessThanOrEqual(3 * config.interval);
    });

    it("sleeping for the reported wait is enough to succeed", async () => {
      const fast = { maxTokens: 2, tokensToAdd: 2, interval: 400 };
      await backend.acquireTokens(KEY, 2, fast);
      const denied = await backend.acquireTokens(KEY, 1, fast);
      expect(denied.acquired).toBe(false);

      await new Promise((r) => setTimeout(r, (denied.waitTime ?? 0) + 25));
      expect((await backend.acquireTokens(KEY, 1, fast)).acquired).toBe(true);
    });

    it("adds a whole interval's worth at a time", async () => {
      await drain();
      await backend.hset(KEY, {
        lastUpdate: (await backend.now()) - config.interval,
      });
      const res = await backend.acquireTokens(KEY, 1, config);
      expect(res.acquired).toBe(true);
      expect(res.remainingTokens).toBe(9);
    });

    it("never exceeds the maximum however long it has been idle", async () => {
      await drain();
      await backend.hset(KEY, {
        lastUpdate: (await backend.now()) - config.interval * 100,
      });
      const res = await backend.acquireTokens(KEY, 1, config);
      expect(res.remainingTokens).toBe(config.maxTokens - 1);
    });
  });

  // ------------------------------------------------------- fast-path admission

  describe("fast-path admission", () => {
    const QUEUE = "fp:queue";
    const BUCKET = "fp:rateLimit";
    const FREEZE = "fp:freezeState";
    const config = { maxTokens: 10, tokensToAdd: 10, interval: 60_000 };
    const admit = (cost = 1) =>
      backend.tryAdmitImmediately(QUEUE, BUCKET, FREEZE, cost, config);
    const tokens = async () =>
      Number((await backend.hgetall(BUCKET)).tokens ?? NaN);

    /** Puts something on the queue the way a client would. */
    const enqueue = (requestId: string, grantId?: string) =>
      backend.addRequest(QUEUE, "fp:request", {
        requestId,
        clientName: "c",
        requestName: "r",
        status: "pending",
        priority: 5,
        cost: 1,
        retries: 0,
        timestamp: Date.now(),
        grantId,
        isThawRequest: false,
        ownerId: "owner",
      });

    it("refuses as soon as anything is queued", async () => {
      expect(await admit()).toBe(true);
      await enqueue("someone-else");
      // Even with tokens to spare, a waiting request outranks a new arrival.
      expect(await admit()).toBe(false);
    });

    it("does not spend a token when it refuses", async () => {
      await admit();
      const before = await tokens();
      await enqueue("waiting");
      expect(await admit()).toBe(false);
      expect(await tokens()).toBe(before);
    });

    it("resumes once the queue drains", async () => {
      await enqueue("waiting");
      expect(await admit()).toBe(false);
      await backend.removeRequest(QUEUE, "fp:request", "waiting");
      expect(await admit()).toBe(true);
    });

    it("draws from the same bucket the queued path uses", async () => {
      await backend.acquireTokens(BUCKET, 4, config);
      expect(await tokens()).toBe(6);
      expect(await admit(6)).toBe(true);
      expect(await tokens()).toBe(0);
      expect(await admit(1)).toBe(false);
    });

    it("never over-admits under concurrent claims", async () => {
      // Ten tokens, twenty simultaneous single-cost claims.
      const results = await Promise.all(
        Array.from({ length: 20 }, () => admit(1))
      );
      expect(results.filter(Boolean)).toHaveLength(10);
      expect(await tokens()).toBe(0);
    });

    it("linearizes a concurrent enqueue against fast-path admission", async () => {
      const [, admitted] = await Promise.all([enqueue("racing"), admit(1)]);
      expect(await backend.getQueueLength(QUEUE)).toBe(1);
      // Either order is valid; the bucket must reflect the same atomic order.
      expect((await backend.getTokenBucketState(BUCKET, config)).tokens).toBe(
        admitted ? 9 : 10
      );
    });

    it("refills on the same schedule as the queued path", async () => {
      await admit(10);
      expect(await admit(1)).toBe(false);
      await backend.hset(BUCKET, {
        lastUpdate: (await backend.now()) - config.interval,
      });
      expect(await admit(1)).toBe(true);
    });

    it("refuses while frozen even with a full bucket", async () => {
      await backend.setFreezeState(FREEZE, (await backend.now()) + 60_000, 1);
      expect(await admit()).toBe(false);
    });

    it("does not spend a token while frozen", async () => {
      await admit();
      const before = await tokens();
      await backend.setFreezeState(FREEZE, (await backend.now()) + 60_000, 1);
      expect(await admit()).toBe(false);
      expect(await tokens()).toBe(before);
    });

    it("admits again once the freeze lapses and no probe is owed", async () => {
      await backend.setFreezeState(FREEZE, (await backend.now()) - 1, 0);
      expect(await admit()).toBe(true);
    });

    // A lapsed freeze with thaw requests still outstanding means the client is
    // probing recovery one request at a time, and only the queued path can
    // arrange that — the fast path has no way to be the single probe. Admitting
    // here would resume at the full configured rate against an API that just
    // rate-limited us, and since fast-path requests never report thaw progress,
    // the counter would never move and the state would persist until its key
    // expired.
    it("still refuses while a thaw probe is owed", async () => {
      await backend.setFreezeState(FREEZE, (await backend.now()) - 1, 2);
      expect(await admit()).toBe(false);
    });

    it("resumes once thaw progress completes", async () => {
      await backend.setFreezeState(FREEZE, (await backend.now()) - 1, 1);
      expect(await admit()).toBe(false);
      await backend.updateThawProgress(FREEZE, true);
      expect(await admit()).toBe(true);
    });

    it("refuses rather than dividing by zero when tokensToAdd is 0", async () => {
      expect(
        await backend.tryAdmitImmediately(QUEUE, BUCKET, FREEZE, 1, {
          maxTokens: 10,
          tokensToAdd: 0,
          interval: 1000,
        })
      ).toBe(false);
    });

    // A budget can shrink under a bucket that is already stocked — that is what
    // `rateLimitChange` does when a vendor header announces a smaller
    // allowance. The balance banked under the old ceiling must not remain
    // spendable, or the first burst after the cut runs at the old rate.
    it("caps a stocked bucket when maxTokens is reduced", async () => {
      expect(await tokens()).toBeNaN();
      await backend.hset(BUCKET, {
        tokens: 1000,
        lastUpdate: await backend.now(),
      });
      const shrunk = { maxTokens: 10, tokensToAdd: 10, interval: 60_000 };
      expect(
        await backend.tryAdmitImmediately(QUEUE, BUCKET, FREEZE, 11, shrunk)
      ).toBe(false);
      expect(
        await backend.tryAdmitImmediately(QUEUE, BUCKET, FREEZE, 10, shrunk)
      ).toBe(true);
      expect(await tokens()).toBe(0);
    });

    it("refuses a cost larger than the whole budget", async () => {
      expect(await admit(999)).toBe(false);
    });
  });

  // ----------------------------------------------- fast-path admission (noLimit)

  /**
   * The no-budget fast path. There is no bucket and no ledger to inspect here, so
   * the whole promise is the two refusals — and that they are ONE operation: a
   * client with no limit of its own still must not overtake work that is waiting,
   * and must not walk through a freeze the vendor asked for.
   */
  describe("fast-path admission — noLimit", () => {
    const QUEUE = "fpn:queue";
    const FREEZE = "fpn:freezeState";
    const admit = () => backend.tryAdmitNoLimit(QUEUE, FREEZE);

    const enqueue = (requestId: string, status: "pending" | "inProgress") =>
      backend.addRequest(QUEUE, "fpn:request", {
        requestId,
        clientName: "c",
        requestName: "r",
        status,
        priority: 5,
        cost: 1,
        retries: 0,
        timestamp: Date.now(),
        isThawRequest: false,
        ownerId: "owner",
      });

    it("admits when nothing is queued and no freeze stands", async () => {
      expect(await admit()).toBe(true);
    });

    it("refuses as soon as anything is queued", async () => {
      await enqueue("waiting", "pending");
      expect(await admit()).toBe(false);
    });

    // An in-flight request is not waiting, but it did arrive first, and its entry
    // is what the queue uses to say so.
    it("refuses for an inProgress entry too", async () => {
      await enqueue("running", "inProgress");
      expect(await admit()).toBe(false);
    });

    it("resumes once the queue drains", async () => {
      await enqueue("waiting", "pending");
      expect(await admit()).toBe(false);
      await backend.removeRequest(QUEUE, "fpn:request", "waiting");
      expect(await admit()).toBe(true);
    });

    it("refuses while frozen", async () => {
      await backend.setFreezeState(FREEZE, (await backend.now()) + 60_000, 1);
      expect(await admit()).toBe(false);
    });

    // Recovery is one request at a time, and only the queue path can arrange
    // that, so a lapsed freeze with probes still owed is not a green light.
    it("refuses while a thaw probe is still owed", async () => {
      await backend.setFreezeState(FREEZE, (await backend.now()) - 1, 2);
      expect(await admit()).toBe(false);
    });

    it("resumes once thaw progress completes", async () => {
      await backend.setFreezeState(FREEZE, (await backend.now()) - 1, 1);
      expect(await admit()).toBe(false);
      await backend.updateThawProgress(FREEZE, true);
      expect(await admit()).toBe(true);
    });

    it("gates each grant on its own freeze key", async () => {
      await backend.setFreezeState(
        `${FREEZE}:grant:t1`,
        (await backend.now()) + 60_000,
        1
      );
      expect(await backend.tryAdmitNoLimit(QUEUE, `${FREEZE}:grant:t1`)).toBe(
        false
      );
      expect(await admit()).toBe(true);
    });
  });

  // ------------------------------------------- fast-path admission (concurrency)

  describe("fast-path admission — concurrency", () => {
    const QUEUE = "fpc:queue";
    const SLOTS = "fpc:concurrency";
    const FREEZE = "fpc:freezeState";
    const config = { maxConcurrency: 2, requestTtl: 60_000 };
    const admit = (requestId: string, cost = 1) =>
      backend.tryAdmitConcurrency(
        QUEUE,
        SLOTS,
        FREEZE,
        cost,
        requestId,
        config
      );

    const enqueue = (requestId: string) =>
      backend.addRequest(QUEUE, "fpc:request", {
        requestId,
        clientName: "c",
        requestName: "r",
        status: "pending",
        priority: 5,
        cost: 1,
        retries: 0,
        timestamp: Date.now(),
        isThawRequest: false,
        ownerId: "owner",
      });

    it("admits up to the concurrency limit", async () => {
      expect(await admit("a")).toBe(true);
      expect(await admit("b")).toBe(true);
      expect(await admit("c")).toBe(false);
    });

    it("refuses as soon as anything is queued", async () => {
      await enqueue("waiting");
      expect(await admit("a")).toBe(false);
    });

    it("refuses while frozen", async () => {
      await backend.setFreezeState(FREEZE, (await backend.now()) + 60_000, 1);
      expect(await admit("a")).toBe(false);
    });

    // Same contract as the token-bucket fast path: a lapsed freeze with a probe
    // still owed means one request at a time, which only the queue can arrange.
    // The concurrency path is the worse of the two to get wrong, because it has
    // no bucket to pace what escapes.
    it("still refuses while a thaw probe is owed", async () => {
      await backend.setFreezeState(FREEZE, (await backend.now()) - 1, 2);
      expect(await admit("a")).toBe(false);
    });

    it("admits once the freeze lapses and no probe is owed", async () => {
      await backend.setFreezeState(FREEZE, (await backend.now()) - 1, 0);
      expect(await admit("a")).toBe(true);
    });

    it("frees the slot again on release", async () => {
      expect(await admit("a")).toBe(true);
      expect(await admit("b")).toBe(true);
      expect(await admit("c")).toBe(false);
      await backend.releaseConcurrency(SLOTS, "a");
      expect(await admit("c")).toBe(true);
    });

    it("refuses a cost larger than the whole budget", async () => {
      expect(await admit("big", 999)).toBe(false);
    });

    /**
     * The lease and the key's own expiry are adjacent, differently-denominated
     * arguments — milliseconds and seconds — so transposing them still parses,
     * still admits and still expires. A 30ms lease read as 30 seconds keeps
     * refusing here; the other direction is only visible as a key TTL, which the
     * contract cannot see. See `test/redisKeyTtl.test.ts`.
     */
    it("reclaims a fast-path slot on the requestTtl it was promised, not on the key's own", async () => {
      const brief = { maxConcurrency: 1, requestTtl: 30 };
      const admitBrief = (requestId: string) =>
        backend.tryAdmitConcurrency(QUEUE, SLOTS, FREEZE, 1, requestId, brief);

      expect(await admitBrief("stuck")).toBe(true);
      expect(await admitBrief("next")).toBe(false);

      await new Promise((r) => setTimeout(r, 60));
      expect(await admitBrief("next")).toBe(true);
    });
  });

  // --------------------------------------------------------------- concurrency

  describe("concurrency slots", () => {
    const KEY = "cc:concurrency";
    const config = { maxConcurrency: 3, requestTtl: 60_000 };

    it("hands out no more than the limit", async () => {
      const results = await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          backend.acquireConcurrency(KEY, 1, `r${i}`, config)
        )
      );
      expect(results.filter((r) => r.acquired)).toHaveLength(3);
    });

    it("claims a queued slot only while its metadata is still in progress", async () => {
      const key = "cq:slots";
      const metadataKey = "cq:request:r1";
      const config = { maxConcurrency: 1, requestTtl: 10_000 };
      await backend.hset(metadataKey, { status: "pending" });
      expect(
        await backend.acquireQueuedConcurrency!(
          key,
          metadataKey,
          1,
          "r1",
          config
        )
      ).toMatchObject({ acquired: false });

      expect(
        await backend.acquireQueuedConcurrency!(key, metadataKey, 1, "r1", {
          maxConcurrency: 0,
          requestTtl: 10_000,
        })
      ).toMatchObject({
        acquired: false,
        error: expect.stringContaining("maxConcurrency"),
      });

      await backend.hset(metadataKey, { status: "inProgress" });
      expect(
        await backend.acquireQueuedConcurrency!(
          key,
          metadataKey,
          1,
          "r1",
          config
        )
      ).toMatchObject({ acquired: true });
      await backend.releaseConcurrency(key, "r1");
      await backend.del(metadataKey);
      expect(
        await backend.acquireQueuedConcurrency!(
          key,
          metadataKey,
          1,
          "r1",
          config
        )
      ).toMatchObject({ acquired: false });
    });

    it("reports queued occupancy without counting the caller's own slot", async () => {
      const key = "cq:own";
      const metadataKey = "cq:request:own";
      const config = { maxConcurrency: 4, requestTtl: 10_000 };
      // The request already holds a slot, and its entry is no longer in progress
      // — the refusal path. Counting its own cost against it made the same
      // request look like contention from someone else.
      await backend.acquireConcurrency(key, 2, "r1", config);
      await backend.hset(metadataKey, { status: "pending" });

      expect(
        await backend.acquireQueuedConcurrency!(
          key,
          metadataKey,
          1,
          "r1",
          config
        )
      ).toMatchObject({ acquired: false, currentConcurrency: 0 });
    });

    it("frees the slot on release", async () => {
      await backend.acquireConcurrency(KEY, 1, "a", config);
      await backend.acquireConcurrency(KEY, 1, "b", config);
      await backend.acquireConcurrency(KEY, 1, "c", config);
      expect(
        (await backend.acquireConcurrency(KEY, 1, "d", config)).acquired
      ).toBe(false);

      await backend.releaseConcurrency(KEY, "a");
      expect(
        (await backend.acquireConcurrency(KEY, 1, "d", config)).acquired
      ).toBe(true);
    });

    it("counts cost, not request count", async () => {
      expect(
        (await backend.acquireConcurrency(KEY, 3, "big", config)).acquired
      ).toBe(true);
      expect(
        (await backend.acquireConcurrency(KEY, 1, "small", config)).acquired
      ).toBe(false);
    });

    it("does not double-count a resubmitted request", async () => {
      await backend.acquireConcurrency(KEY, 2, "same", config);
      const again = await backend.acquireConcurrency(KEY, 2, "same", config);
      expect(again.acquired).toBe(true);
      expect(again.currentConcurrency).toBe(2);
    });

    it("reclaims slots held past the ttl", async () => {
      const brief = { maxConcurrency: 1, requestTtl: 30 };
      expect(
        (await backend.acquireConcurrency(KEY, 1, "stuck", brief)).acquired
      ).toBe(true);
      expect(
        (await backend.acquireConcurrency(KEY, 1, "next", brief)).acquired
      ).toBe(false);

      // The holder crashed; its slot must not be held forever.
      await new Promise((r) => setTimeout(r, 60));
      expect(
        (await backend.acquireConcurrency(KEY, 1, "next", brief)).acquired
      ).toBe(true);
    });

    it("reclaims a queued slot on the requestTtl it was promised, not on the key's own", async () => {
      const key = "cq:lease";
      const metadataKey = "cq:request:lease";
      const brief = { maxConcurrency: 1, requestTtl: 30 };
      const acquire = (requestId: string) =>
        backend.acquireQueuedConcurrency!(
          key,
          metadataKey,
          1,
          requestId,
          brief
        );
      await backend.hset(metadataKey, { status: "inProgress" });

      expect(await acquire("stuck")).toMatchObject({ acquired: true });
      expect(await acquire("next")).toMatchObject({ acquired: false });

      await new Promise((r) => setTimeout(r, 60));
      expect(await acquire("next")).toMatchObject({ acquired: true });
    });

    it("reports current usage and active members", async () => {
      await backend.acquireConcurrency(KEY, 2, "x", config);
      const state = await backend.getConcurrencyState(KEY, config.requestTtl);
      expect(state.currentConcurrency).toBe(2);
      expect(state.activeRequests).toEqual(["x"]);
    });

    it("does not mutate the slot ledger while reporting expired entries", async () => {
      await backend.acquireConcurrency(KEY, 1, "old", {
        maxConcurrency: 2,
        requestTtl: 10_000,
      });
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(await backend.getConcurrencyState(KEY, 10)).toMatchObject({
        currentConcurrency: 0,
        activeRequests: [],
      });
      // A longer observer still sees it: the short-TTL read did not delete it.
      expect(await backend.getConcurrencyState(KEY, 10_000)).toMatchObject({
        currentConcurrency: 1,
        activeRequests: ["old"],
      });
    });

    it("reports an empty list, not an empty object, when nothing is held", async () => {
      // A JSON encoder that turns an empty collection into `{}` produces
      // something the caller cannot iterate, and the idle case is exactly the
      // one a non-empty assertion never reaches.
      const idle = await backend.getConcurrencyState(KEY, config.requestTtl);
      expect(Array.isArray(idle.activeRequests)).toBe(true);
      expect(idle.activeRequests).toEqual([]);
      expect(idle.currentConcurrency).toBe(0);

      // And still an array after slots have been taken and released.
      await backend.acquireConcurrency(KEY, 1, "gone", config);
      await backend.releaseConcurrency(KEY, "gone");
      const after = await backend.getConcurrencyState(KEY, config.requestTtl);
      expect(Array.isArray(after.activeRequests)).toBe(true);
      expect(after.activeRequests).toEqual([]);
    });

    it("clears every slot at once", async () => {
      await backend.acquireConcurrency(KEY, 1, "a", config);
      await backend.acquireConcurrency(KEY, 2, "b", config);

      await backend.clearConcurrency(KEY);

      expect(await backend.getConcurrencyState(KEY, config.requestTtl)).toEqual(
        {
          currentConcurrency: 0,
          activeRequests: [],
        }
      );
      // Reporting zero is not the same as being claimable: the stored costs live
      // beside the slots under their own key, and a ledger that outlived them
      // would keep turning requests away at a ceiling nothing is holding.
      expect(
        (await backend.acquireConcurrency(KEY, 3, "after", config)).acquired
      ).toBe(true);
    });
  });

  // --------------------------------------------------------------- queue order

  describe("queue ordering", () => {
    const QUEUE = "q:queue";
    const PREFIX = "q:request";

    const add = (
      requestId: string,
      overrides: Partial<{
        priority: number;
        retries: number;
        timestamp: number;
        grantId: string;
      }> = {}
    ) =>
      backend.addRequest(QUEUE, PREFIX, {
        requestId,
        clientName: "c",
        requestName: "r",
        status: "pending",
        priority: overrides.priority ?? 5,
        cost: 1,
        retries: overrides.retries ?? 0,
        timestamp: overrides.timestamp ?? Date.now(),
        grantId: overrides.grantId,
        isThawRequest: false,
        ownerId: "owner",
      });

    it("serves higher priority first", async () => {
      await add("low", { priority: 1 });
      await add("high", { priority: 9 });
      expect((await backend.getNextRequest(QUEUE, PREFIX))?.requestId).toBe(
        "high"
      );
    });

    it("serves the earlier arrival within one priority", async () => {
      const now = Date.now();
      await add("second", { timestamp: now + 1000 });
      await add("first", { timestamp: now });
      expect((await backend.getNextRequest(QUEUE, PREFIX))?.requestId).toBe(
        "first"
      );
    });

    it("puts a retried request ahead of a fresh one at equal priority", async () => {
      const now = Date.now();
      await add("fresh", { timestamp: now });
      await add("retried", { timestamp: now + 1000, retries: 2 });
      expect((await backend.getNextRequest(QUEUE, PREFIX))?.requestId).toBe(
        "retried"
      );
    });

    it("keeps a re-scored request on the same scale as fresh arrivals", async () => {
      // Re-scoring on retry once used different constants from addRequest, so a
      // retried request sorted behind the entire queue — the exact opposite of
      // retrying sooner. It has to stay comparable with everything else.
      const now = Date.now();
      await add("fresh", { timestamp: now });
      await add("bumped", { timestamp: now + 5_000 });
      await backend.updateRequest(QUEUE, PREFIX, "bumped", { retries: 3 });
      expect((await backend.getNextRequest(QUEUE, PREFIX))?.requestId).toBe(
        "bumped"
      );
    });

    it("marks what it hands out as in progress, and does not serve it twice", async () => {
      await add("only");
      expect((await backend.getNextRequest(QUEUE, PREFIX))?.status).toBe(
        "inProgress"
      );
      expect(await backend.getNextRequest(QUEUE, PREFIX)).toBeNull();
    });

    it("skips grants it was told to skip", async () => {
      await add("blocked", { grantId: "gA", priority: 9 });
      await add("open", { grantId: "gB", priority: 1 });
      const next = await backend.getNextRequest(QUEUE, PREFIX, ["gA"]);
      expect(next?.requestId).toBe("open");
    });

    it("refuses to queue the same request twice", async () => {
      expect(await add("dupe")).toBe(true);
      // A retried enqueue must not reset the request's queue position.
      expect(await add("dupe")).toBe(false);
    });

    it("reports pending, in-progress and total cost", async () => {
      await add("a");
      await add("b");
      await backend.getNextRequest(QUEUE, PREFIX);
      expect(await backend.getQueueStats(QUEUE, PREFIX)).toEqual({
        pending: 1,
        inProgress: 1,
        totalCost: 2,
      });
    });

    it("round-trips every field", async () => {
      await add("full", { priority: 7, retries: 2, grantId: "g1" });
      const got = await backend.getRequest(PREFIX, "full");
      expect(got).toMatchObject({
        requestId: "full",
        clientName: "c",
        requestName: "r",
        status: "pending",
        priority: 7,
        retries: 2,
        cost: 1,
        grantId: "g1",
        isThawRequest: false,
        ownerId: "owner",
      });
    });

    it.each([
      ["blank", "", 1],
      ["unparseable", "abc", 1],
      ["zero", "0", 0],
      ["fractional", "2.5", 2.5],
    ])(
      "counts a %s stored cost the same way on every backend",
      async (_label, stored, expected) => {
        await add("odd");
        // Overwrite after enqueueing: addRequest refuses an id already present, so
        // planting the field first would leave nothing in the queue to count.
        await backend.hset(`${PREFIX}:odd`, { cost: stored });

        const stats = await backend.getQueueStats(QUEUE, PREFIX);
        expect(stats.totalCost).toBe(expected);
      }
    );

    it("counts nothing for an entry whose status is blank", async () => {
      await add("blank");
      // An empty string is truthy in Lua and falsy in JavaScript, so this entry
      // contributed its cost on Redis and was skipped on memory — a totalCost the
      // two backends disagreed about, for a status that is neither state.
      await backend.hset(`${PREFIX}:blank`, { status: "" });

      expect(await backend.getQueueStats(QUEUE, PREFIX)).toEqual({
        pending: 0,
        inProgress: 0,
        totalCost: 0,
      });
    });

    it("reads a metadata hash that has lost its status as gone, not as a request", async () => {
      // A queue entry is identified by four fields, and a hash missing any of
      // them is a partially-expired or externally-written remnant rather than a
      // request. Both backends must agree on that: the Redis reader once
      // accepted a status-less hash and returned `status: undefined` with
      // `retries: NaN`, while memory returned null for the same bytes.
      await backend.hset(`${PREFIX}:remnant`, {
        requestId: "remnant",
        clientName: "c",
        requestName: "r",
        ownerId: "owner",
      });

      expect(await backend.getRequest(PREFIX, "remnant")).toBeNull();
    });

    it("defaults a stored numeric field rather than reporting NaN", async () => {
      await add("defaults");
      // Blank out the two fields whose absence used to decode to NaN on Redis.
      await backend.hset(`${PREFIX}:defaults`, { retries: "", timestamp: "" });

      const got = await backend.getRequest(PREFIX, "defaults");
      expect(got?.retries).toBe(0);
      expect(got?.timestamp).toBe(0);
      expect(got?.ownerId).toBe("owner");
    });

    it("removes a request and says whether it was the thaw probe", async () => {
      await add("probe");
      await backend.updateRequest(QUEUE, PREFIX, "probe", {
        isThawRequest: true,
      });
      expect(await backend.removeRequest(QUEUE, PREFIX, "probe")).toEqual({
        wasThawRequest: true,
      });
      expect(await backend.getRequest(PREFIX, "probe")).toBeNull();
    });

    it("reaps a queue member whose metadata expired under it", async () => {
      await add("stranded");
      // `addRequest` refreshes the queue key's TTL on every add but each metadata
      // key's only at its own, so a queue kept alive by traffic outlives the
      // metadata of anything left undrained in it. Deleting the hash reaches that
      // state directly rather than waiting out a TTL.
      await backend.del(`${PREFIX}:stranded`);

      // Nothing drains it — and while it is there it counts toward the
      // queue-empty gate, so every request takes the queued path.
      expect(await backend.getQueueLength(QUEUE)).toBe(1);
      expect(await backend.getNextRequest(QUEUE, PREFIX)).toBeNull();

      // Orphan cleanup is what recovers this, so the window is one health-check
      // tick rather than the queue key's whole 24h TTL.
      expect(
        await backend.cleanupOrphanedRequests(
          QUEUE,
          PREFIX,
          new Set(["owner"]),
          "owner"
        )
      ).toBe(1);
      expect(await backend.getQueueLength(QUEUE)).toBe(0);
    });

    it("drops requests owned by instances that are gone", async () => {
      await add("mine");
      await add("theirs");
      await backend.updateRequest(QUEUE, PREFIX, "theirs", {});
      // `add` stamps ownerId "owner" for both; only "owner" is still alive.
      expect(
        await backend.cleanupOrphanedRequests(
          QUEUE,
          PREFIX,
          new Set(["someone-else"]),
          "someone-else"
        )
      ).toBe(2);
      expect(await backend.getAllRequests(QUEUE, PREFIX)).toEqual([]);
    });

    it("reaps nothing when the sweeper is absent from the alive set", async () => {
      await add("owned");
      await add("stranded");
      await backend.del(`${PREFIX}:stranded`);

      // An instance re-adds its own id on every heartbeat, so a set without the
      // sweeper's own is a read that was lost rather than a fleet that died.
      // Both branches of the sweep are gated on it, not only the dead-owner one.
      expect(
        await backend.cleanupOrphanedRequests(
          QUEUE,
          PREFIX,
          new Set(),
          "sweeper"
        )
      ).toBe(0);
      expect(
        await backend.cleanupOrphanedRequests(
          QUEUE,
          PREFIX,
          new Set(["someone-else"]),
          "sweeper"
        )
      ).toBe(0);
      expect(await backend.getQueueLength(QUEUE)).toBe(2);

      // The identical call, once the sweeper can see itself, reaps both.
      expect(
        await backend.cleanupOrphanedRequests(
          QUEUE,
          PREFIX,
          new Set(["sweeper"]),
          "sweeper"
        )
      ).toBe(2);
      expect(await backend.getQueueLength(QUEUE)).toBe(0);
    });

    it("returns every queued request, in queue order", async () => {
      await add("mid", { priority: 5 });
      await add("low", { priority: 1 });
      await add("high", { priority: 9 });
      await backend.getNextRequest(QUEUE, PREFIX);

      const all = await backend.getAllRequests(QUEUE, PREFIX);
      expect(all.map((r) => r.requestId)).toEqual(["high", "mid", "low"]);
      expect(all.map((r) => r.status)).toEqual([
        "inProgress",
        "pending",
        "pending",
      ]);
    });
  });

  // ------------------------------------------------------------- freeze / thaw

  describe("freeze and thaw", () => {
    const FROZEN = "ft:frozenGrants";
    const QUEUE = "ft:queue";
    const PREFIX = "ft:request";
    const freezeKey = "ft:freezeState";

    const enqueueInProgress = async (requestId: string, grantId?: string) => {
      await backend.addRequest(QUEUE, PREFIX, {
        requestId,
        clientName: "c",
        requestName: "r",
        status: "pending",
        priority: 5,
        cost: 1,
        retries: 0,
        timestamp: Date.now(),
        grantId,
        isThawRequest: false,
        ownerId: "owner",
      });
      await backend.updateRequest(QUEUE, PREFIX, requestId, {
        status: "inProgress",
      });
    };

    it("admits exactly one probe across concurrent claimants", async () => {
      await enqueueInProgress("r1");
      await enqueueInProgress("r2");
      await enqueueInProgress("r3");

      const results = await Promise.all(
        ["r1", "r2", "r3"].map((id) =>
          backend.tryStartThawRequest(FROZEN, QUEUE, PREFIX, id, "")
        )
      );
      // This is the guarantee the README sells: one probe, fleet-wide.
      expect(results.filter((r) => r === "started")).toHaveLength(1);
      expect(results.filter((r) => r === "exists")).toHaveLength(2);
    });

    it("marks only the winner as the thaw request", async () => {
      await enqueueInProgress("r1");
      await enqueueInProgress("r2");
      await backend.tryStartThawRequest(FROZEN, QUEUE, PREFIX, "r1", "");
      await backend.tryStartThawRequest(FROZEN, QUEUE, PREFIX, "r2", "");

      expect((await backend.getRequest(PREFIX, "r1"))?.isThawRequest).toBe(
        true
      );
      expect((await backend.getRequest(PREFIX, "r2"))?.isThawRequest).toBe(
        false
      );
    });

    it("does not put an empty member in the frozen-grants set", async () => {
      await enqueueInProgress("r1");
      await backend.tryStartThawRequest(FROZEN, QUEUE, PREFIX, "r1", "");
      // That set drives getNextRequest's skip list; it keys on a real grant id.
      expect(await backend.smembers(FROZEN)).toEqual([]);
    });

    it("allows a new probe once the previous one leaves the queue", async () => {
      await enqueueInProgress("r1");
      expect(
        await backend.tryStartThawRequest(FROZEN, QUEUE, PREFIX, "r1", "")
      ).toBe("started");

      await backend.removeRequest(QUEUE, PREFIX, "r1");

      await enqueueInProgress("r2");
      expect(
        await backend.tryStartThawRequest(FROZEN, QUEUE, PREFIX, "r2", "")
      ).toBe("started");
    });

    it("isolates the probe per grant", async () => {
      await enqueueInProgress("a1", "grantA");
      await enqueueInProgress("b1", "grantB");
      // One grant's freeze must not stall another.
      expect(
        await backend.tryStartThawRequest(FROZEN, QUEUE, PREFIX, "a1", "grantA")
      ).toBe("started");
      expect(
        await backend.tryStartThawRequest(FROZEN, QUEUE, PREFIX, "b1", "grantB")
      ).toBe("started");
      expect((await backend.smembers(FROZEN)).sort()).toEqual([
        "grantA",
        "grantB",
      ]);
    });

    it("admits one probe per grant, not one per request", async () => {
      await enqueueInProgress("a1", "grantA");
      await enqueueInProgress("a2", "grantA");
      const results = await Promise.all([
        backend.tryStartThawRequest(FROZEN, QUEUE, PREFIX, "a1", "grantA"),
        backend.tryStartThawRequest(FROZEN, QUEUE, PREFIX, "a2", "grantA"),
      ]);
      expect(results.filter((r) => r === "started")).toHaveLength(1);
    });

    it("keeps a client-level probe separate from a grant probe", async () => {
      await enqueueInProgress("c1");
      await enqueueInProgress("a1", "grantA");
      // Different budgets, so neither blocks the other.
      expect(
        await backend.tryStartThawRequest(FROZEN, QUEUE, PREFIX, "a1", "grantA")
      ).toBe("started");
      expect(
        await backend.tryStartThawRequest(FROZEN, QUEUE, PREFIX, "c1", "")
      ).toBe("started");
    });

    it("reports an in-progress probe for a grant", async () => {
      await enqueueInProgress("a1", "grantA");
      expect(
        await backend.hasThawRequestInProgress(QUEUE, PREFIX, "grantA")
      ).toBe(false);
      await backend.tryStartThawRequest(FROZEN, QUEUE, PREFIX, "a1", "grantA");
      expect(
        await backend.hasThawRequestInProgress(QUEUE, PREFIX, "grantA")
      ).toBe(true);
    });

    it("blocks while frozen and permits once the freeze has lapsed", async () => {
      await backend.setFreezeState(
        freezeKey,
        (await backend.now()) + 50_000,
        1
      );
      expect((await backend.canProcessRequest(freezeKey)).canProcess).toBe(
        false
      );
      expect(await backend.isFrozen(freezeKey)).toBe(true);

      await backend.setFreezeState(freezeKey, (await backend.now()) - 1, 1);
      expect((await backend.canProcessRequest(freezeKey)).canProcess).toBe(
        true
      );
    });

    it("nominates the first request after expiry as the probe", async () => {
      const lapsedAt = (await backend.now()) - 1;
      await backend.setFreezeState(freezeKey, lapsedAt, 1);
      const state = await backend.canProcessRequest(freezeKey);
      // `frozenUntil` rides along with every answer that HAS freeze state, on the
      // backend's clock. A caller that declines a request books its wake-up from
      // this, rather than subtracting its own clock from a remote timestamp — the
      // one subtraction in the codebase that mixed two clocks.
      expect(state).toEqual({
        canProcess: true,
        isThawRequest: true,
        frozenUntil: lapsedAt,
      });
    });

    /**
     * `armFreeze` is monotone; `setFreezeState` is not. Both backends have to
     * agree on which, because the client only ever arms through the former and
     * fixtures only ever write through the latter.
     *
     * What this is defending: two failures arming concurrently carry different
     * back-off multipliers, and a plain write let whichever landed second set the
     * deadline. Measured on ordinary traffic — two requests at different retry
     * depths, nothing injected — `frozenUntil` moved BACKWARDS by 379ms; with a
     * third-retry arm against a first-failure arm, a 2988ms freeze was cut to
     * 848ms. Every replica then read the shortened deadline, so the fleet
     * resumed early on the strength of the least-backed-off request.
     */
    it("arms a freeze without ever shortening one already standing", async () => {
      const now = await backend.now();

      // A third-retry arm: long window, full probe budget.
      expect(await backend.armFreeze(freezeKey, now + 50_000, 3)).toEqual({
        frozenUntil: now + 50_000,
        thawRequestCount: 3,
      });

      // A first-failure arm landing a moment later asks for a shorter window.
      expect(
        (await backend.armFreeze(freezeKey, now + 1_000, 3)).frozenUntil
      ).toBe(now + 50_000);

      // An upstream-failure arm carries NO probe budget. It must not cancel the
      // probes the 429 is still owed, or single-flight recovery becomes a
      // stampede the moment the window lapses.
      expect(await backend.armFreeze(freezeKey, now + 2_000, 0)).toEqual({
        frozenUntil: now + 50_000,
        thawRequestCount: 3,
      });

      // A genuinely longer arm still extends it, and still cannot lower the
      // budget it found.
      expect(await backend.armFreeze(freezeKey, now + 90_000, 0)).toEqual({
        frozenUntil: now + 90_000,
        thawRequestCount: 3,
      });

      // `setFreezeState` stays the unconditional setter: the fast-path tests
      // above use it to move a freeze from live to lapsed, which `armFreeze`
      // deliberately cannot do.
      await backend.setFreezeState(freezeKey, now + 1_000, 1);
      expect(await backend.getFreezeState(freezeKey)).toEqual({
        frozenUntil: now + 1_000,
        thawRequestCount: 1,
      });
    });

    it("clears the freeze once enough probes succeed", async () => {
      await backend.setFreezeState(freezeKey, (await backend.now()) - 1, 1);
      await backend.canProcessRequest(freezeKey);
      await backend.updateThawProgress(freezeKey, true);
      expect(await backend.getFreezeState(freezeKey)).toBeNull();
    });

    it("counts a completion toward thaw progress only once", async () => {
      await backend.setFreezeState(freezeKey, (await backend.now()) - 1, 3);
      await backend.updateThawProgress(freezeKey, true, "completion-1");
      await backend.updateThawProgress(freezeKey, true, "completion-1");
      expect(await backend.getFreezeState(freezeKey)).toMatchObject({
        thawRequestCount: 2,
      });
    });

    it("remembers a completion delivered while no freeze stands", async () => {
      // Recorded even though there is nothing to decrement, so a redelivery of
      // it cannot be spent against a freeze armed afterwards. The memory backend
      // read the freeze state first and returned before reaching the set, which
      // let exactly that redelivery end the next freeze a probe early.
      expect(
        await backend.updateThawProgress(freezeKey, true, "completion-late")
      ).toBeNull();

      await backend.setFreezeState(freezeKey, (await backend.now()) - 1, 2);
      await backend.updateThawProgress(freezeKey, true, "completion-late");

      expect(await backend.getFreezeState(freezeKey)).toMatchObject({
        thawRequestCount: 2,
      });
    });

    it("keeps probing one at a time when a probe fails", async () => {
      await backend.setFreezeState(freezeKey, (await backend.now()) - 1, 2);
      await backend.canProcessRequest(freezeKey);
      const after = await backend.updateThawProgress(freezeKey, false);
      // A failed probe does not consume thaw progress.
      expect(after?.thawRequestCount).toBe(2);
      expect((await backend.canProcessRequest(freezeKey)).isThawRequest).toBe(
        true
      );
    });

    it("requires every probe to succeed before the freeze clears", async () => {
      await backend.setFreezeState(freezeKey, (await backend.now()) - 1, 2);
      await backend.updateThawProgress(freezeKey, true);
      expect(await backend.getFreezeState(freezeKey)).not.toBeNull();
      await backend.updateThawProgress(freezeKey, true);
      expect(await backend.getFreezeState(freezeKey)).toBeNull();
    });

    it("cleans up grants whose freeze has passed and which are not probing", async () => {
      await backend.sadd(FROZEN, "stale");
      await backend.sadd(FROZEN, "still-frozen");
      await backend.setFreezeState(
        "ft:grant:still-frozen:freezeState",
        (await backend.now()) + 60_000,
        1
      );
      expect(
        await backend.cleanupStaleFrozenGrants(
          FROZEN,
          QUEUE,
          PREFIX,
          "ft:grant:"
        )
      ).toBe(1);
      expect(await backend.smembers(FROZEN)).toEqual(["still-frozen"]);
    });

    /**
     * Dropping a grant whose probe is still in flight readmits the whole queue
     * at full rate against a vendor that has just rate-limited you — the
     * stampede `tryStartThawRequest` single-flights, reached through the cleanup
     * door instead. The grant here has no freeze state at all, so only the probe
     * is holding it in the set.
     */
    it("keeps a grant whose freeze has lapsed while its probe is still in progress", async () => {
      await enqueueInProgress("probe", "grantA");
      expect(
        await backend.tryStartThawRequest(
          FROZEN,
          QUEUE,
          PREFIX,
          "probe",
          "grantA"
        )
      ).toBe("started");

      expect(
        await backend.cleanupStaleFrozenGrants(
          FROZEN,
          QUEUE,
          PREFIX,
          "ft:grant:"
        )
      ).toBe(0);
      expect(await backend.smembers(FROZEN)).toEqual(["grantA"]);

      await backend.removeRequest(QUEUE, PREFIX, "probe");
      expect(
        await backend.cleanupStaleFrozenGrants(
          FROZEN,
          QUEUE,
          PREFIX,
          "ft:grant:"
        )
      ).toBe(1);
      expect(await backend.smembers(FROZEN)).toEqual([]);
    });
  });

  // --------------------------------------------------------- store and pub/sub

  describe("store", () => {
    it("round-trips strings, hashes and sets", async () => {
      await backend.set("s:key", "value");
      expect(await backend.get("s:key")).toBe("value");
      expect(await backend.get("s:missing")).toBeNull();

      await backend.hset("s:hash", { a: "1", b: 2 });
      expect(await backend.hgetall("s:hash")).toEqual({ a: "1", b: "2" });

      await backend.sadd("s:set", "x");
      await backend.sadd("s:set", "y");
      expect((await backend.smembers("s:set")).sort()).toEqual(["x", "y"]);
      await backend.srem("s:set", "x");
      expect(await backend.smembers("s:set")).toEqual(["y"]);
    });

    it("expires a key with a ttl", async () => {
      await backend.set("s:ttl", "v", 1);
      expect(await backend.get("s:ttl")).toBe("v");
      await new Promise((r) => setTimeout(r, 1100));
      expect(await backend.get("s:ttl")).toBeNull();
    });

    it("deletes keys", async () => {
      await backend.set("s:a", "1");
      await backend.set("s:b", "2");
      await backend.del("s:a", "s:b");
      expect(await backend.get("s:a")).toBeNull();
      expect(await backend.get("s:b")).toBeNull();
    });

    it("applies a batch", async () => {
      await backend.batch([
        { op: "set", key: "b:one", value: "1" },
        { op: "sadd", key: "b:set", member: "m" },
        { op: "hset", key: "b:hash", fields: { f: "v" } },
      ]);
      expect(await backend.get("b:one")).toBe("1");
      expect(await backend.smembers("b:set")).toEqual(["m"]);
      expect(await backend.hgetall("b:hash")).toEqual({ f: "v" });
    });

    it("applies an atomic batch", async () => {
      await backend.batch(
        [
          { op: "set", key: "b:x", value: "1" },
          { op: "del", key: "b:one" },
        ],
        { atomic: true }
      );
      expect(await backend.get("b:x")).toBe("1");
    });

    it("never exposes a half-applied atomic batch to a reader", async () => {
      await backend.set("b:first", "stale");
      await backend.set("b:second", "stale");

      // The reader has to resume BETWEEN two ops, which is the only shape that
      // separates an isolated group from a sequential one — asserting the settled
      // state passes either way. The memory backend tore here for as long as its
      // batch loop awaited its own `set`/`hset`.
      const [, observed] = await Promise.all([
        backend.batch(
          [
            { op: "set", key: "b:first", value: "new" },
            { op: "sadd", key: "b:witness", member: "m" },
            { op: "set", key: "b:second", value: "new" },
          ],
          { atomic: true }
        ),
        // Reads issued while the group is in flight, and BOTH in one tick so they
        // snapshot the same instant. Awaiting them one after the other instead
        // lets the group finish in the gap between them, and the tear closes
        // before it can be observed — which is why this reader adds no `await`
        // of its own before reading.
        Promise.all([backend.get("b:first"), backend.get("b:second")]),
      ]);

      expect([
        ["stale", "stale"],
        ["new", "new"],
      ]).toContainEqual(observed);
    });
  });

  describe("locks", () => {
    it("gives the lock to exactly one caller", async () => {
      const results = await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          backend.acquireLock("l:key", `token-${i}`, 5_000)
        )
      );
      expect(results.filter(Boolean)).toHaveLength(1);
    });

    it("refuses to release a lock held by someone else", async () => {
      await backend.acquireLock("l:key", "mine", 5_000);
      // Releasing another holder's lock is how two writers end up inside one
      // critical section.
      expect(await backend.releaseLock("l:key", "theirs")).toBe(false);
      expect(await backend.releaseLock("l:key", "mine")).toBe(true);
    });

    it("frees the lock once released", async () => {
      await backend.acquireLock("l:key", "a", 5_000);
      await backend.releaseLock("l:key", "a");
      expect(await backend.acquireLock("l:key", "b", 5_000)).toBe(true);
    });

    it("expires a lock whose holder never released it", async () => {
      expect(await backend.acquireLock("l:brief", "a", 50)).toBe(true);
      expect(await backend.acquireLock("l:brief", "b", 50)).toBe(false);
      await new Promise((r) => setTimeout(r, 80));
      expect(await backend.acquireLock("l:brief", "b", 50)).toBe(true);
    });
  });

  describe("pub/sub", () => {
    it("delivers published messages to a subscriber", async () => {
      const seen: Array<[string, string]> = [];
      await backend.subscribe(["ps:chan"], (channel, message) => {
        seen.push([channel, message]);
      });
      await backend.publish("ps:chan", "hello");

      await new Promise((r) => setTimeout(r, 100));
      expect(seen).toEqual([["ps:chan", "hello"]]);
    });

    it("does not deliver channels it did not subscribe to", async () => {
      const seen: string[] = [];
      await backend.subscribe(["ps:wanted"], (_c, m) => seen.push(m));
      await backend.publish("ps:unwanted", "nope");
      await backend.publish("ps:wanted", "yes");

      await new Promise((r) => setTimeout(r, 100));
      expect(seen).toEqual(["yes"]);
    });

    it("gives a second subscriber only the channels it asked for", async () => {
      // `subscribe` is a documented extension point a host may call again, and one
      // Redis connection hands every message to every listener attached to it —
      // so the second handler received the first's channels until each
      // registration filtered to its own.
      const first: string[] = [];
      const second: string[] = [];
      await backend.subscribe(["ps:first"], (channel) => first.push(channel));
      await backend.subscribe(["ps:second"], (channel) => second.push(channel));

      await backend.publish("ps:first", "x");
      await new Promise((r) => setTimeout(r, 100));

      expect(first).toEqual(["ps:first"]);
      expect(second).toEqual([]);
    });
  });

  describe("instances", () => {
    it("lists peers and drops ones whose record has gone", async () => {
      await backend.sadd("i:instances", "self");
      await backend.sadd("i:instances", "alive");
      await backend.sadd("i:instances", "dead");
      await backend.set("i:instance:alive", '{"id":"alive"}');

      const instances = await backend.getInstances(
        "i:instances",
        "i:instance:",
        "self"
      );
      expect(instances).toEqual([{ id: "alive", data: '{"id":"alive"}' }]);
      // The dead one had no record, so it should have been reaped.
      expect((await backend.smembers("i:instances")).sort()).toEqual([
        "alive",
        "self",
      ]);
    });
  });

  describe("identity", () => {
    it("declares whether it is safe across processes", async () => {
      // The handler refuses a multi-process deployment on this flag, so it has
      // to be right rather than decorative.
      expect(backend.distributed).toBe(harness.name === "redis");
    });
  });

  // ------------------------------------------------- unusable configuration

  describe("unusable configuration", () => {
    const bucket = { maxTokens: 10, tokensToAdd: 1, interval: 1000 };

    it.each([
      ["maxTokens NaN", { ...bucket, maxTokens: NaN }, 1],
      ["maxTokens Infinity", { ...bucket, maxTokens: Infinity }, 1],
      ["maxTokens zero", { ...bucket, maxTokens: 0 }, 1],
      ["cost NaN", bucket, NaN],
      ["cost Infinity", bucket, Infinity],
    ])(
      "reports %s through error rather than raising",
      async (label, config, cost) => {
        const result = await backend.acquireTokens(
          `bad:${label}`,
          cost,
          config
        );
        expect(result.acquired).toBe(false);
        expect(result.error).toBeTruthy();
      }
    );

    it.each([
      ["NaN", NaN],
      ["Infinity", Infinity],
    ])(
      "refuses the concurrency fast path on a %s ceiling instead of admitting without one",
      async (label, maxConcurrency) => {
        const config = { maxConcurrency, requestTtl: 60_000 };
        for (let i = 0; i < 3; i++) {
          expect(
            await backend.tryAdmitConcurrency(
              `uc:q:${label}`,
              `uc:c:${label}`,
              `uc:f:${label}`,
              1,
              `r${i}`,
              config
            )
          ).toBe(false);
        }
        expect(
          (await backend.getConcurrencyState(`uc:c:${label}`, 60_000))
            .currentConcurrency
        ).toBe(0);
      }
    );

    it("reports an infinite concurrency ceiling as unusable", async () => {
      expect(
        await backend.acquireConcurrency("uc:inf", 1, "r1", {
          maxConcurrency: Infinity,
          requestTtl: 60_000,
        })
      ).toMatchObject({ acquired: false, currentConcurrency: 0 });
      expect(
        (
          await backend.acquireConcurrency("uc:inf2", 1, "r1", {
            maxConcurrency: Infinity,
            requestTtl: 60_000,
          })
        ).error
      ).toBeTruthy();
    });
  });

  // ------------------------------------------------------ corrupt stored state

  describe("corrupt stored state", () => {
    const config = { maxTokens: 10, tokensToAdd: 1, interval: 1000 };

    it.each([["abc"], [""], ["NaN"], ["Infinity"]])(
      "treats a stored tokens value of %j as a full bucket rather than wedging it",
      async (stored) => {
        const key = `cs:tb:${stored || "blank"}`;
        await backend.hset(key, {
          tokens: stored,
          lastUpdate: String(await backend.now()),
        });
        // Three in a row: the failure this guards wrote NaN back and refused for good.
        for (let i = 0; i < 3; i++) {
          expect(await backend.acquireTokens(key, 1, config)).toMatchObject({
            acquired: true,
          });
        }
      }
    );

    it("keeps enforcing the ceiling when a slot's stored cost is unparseable", async () => {
      const key = "cs:conc";
      const config = { maxConcurrency: 1, requestTtl: 60_000 };
      await backend.acquireConcurrency(key, 1, "held", config);
      await backend.hset(`${key}:costs`, { held: "abc" });
      for (let i = 0; i < 3; i++) {
        expect(
          await backend.acquireConcurrency(key, 1, `extra${i}`, config)
        ).toMatchObject({ acquired: false });
      }
    });

    it("counts a queue cost of NaN as the default instead of failing the whole read", async () => {
      const queueKey = "cs:q";
      const prefix = "cs:m";
      await backend.addRequest(queueKey, prefix, {
        requestId: "r1",
        clientName: "c",
        requestName: "r",
        status: "pending",
        priority: 1,
        cost: 1,
        retries: 0,
        timestamp: Date.now(),
        isThawRequest: false,
        ownerId: "o",
      });
      await backend.hset(`${prefix}:r1`, { cost: "NaN" });
      expect(await backend.getQueueStats(queueKey, prefix)).toMatchObject({
        totalCost: 1,
      });
    });

    it.each([["abc"], [""], ["NaN"]])(
      "treats a stored frozenUntil of %j as no freeze state",
      async (stored) => {
        const key = `cs:fz:${stored || "blank"}`;
        await backend.hset(key, {
          frozenUntil: stored,
          thawRequestCount: "0",
        });
        expect(await backend.getFreezeState(key)).toBeNull();
        expect(await backend.isFrozen(key)).toBe(false);
      }
    );
  });

  // ---------------------------------------------------------- freeze arming

  describe("arming a freeze", () => {
    it("keeps the standing probe budget when handed a NaN count", async () => {
      const key = "fz:nan";
      const now = await backend.now();
      await backend.armFreeze(key, now + 60_000, 3);
      // NaN compares false against 3, so an unguarded merge kept it and
      // string.format wrote "0" — cancelling the probes a 429 was owed.
      expect(await backend.armFreeze(key, now + 1_000, NaN)).toMatchObject({
        thawRequestCount: 3,
      });
    });

    it("does not claim a thaw probe for a request with no metadata", async () => {
      expect(
        await backend.tryStartThawRequest(
          "fz:grants",
          "fz:queue",
          "fz:meta",
          "gone",
          "g1"
        )
      ).toBe("exists");
      expect(await backend.hgetall("fz:meta:gone")).toEqual({});
    });
  });

  // ------------------------------------------------------------------- ttl

  describe("ttl arguments", () => {
    it.each([
      ["NaN", NaN],
      ["Infinity", Infinity],
      ["out of range", 1e17],
    ])("refuses a %s ttl before writing anything", async (label, ttl) => {
      const key = `ttl:${label}`;
      await expect(backend.hset(key, { secret: "token" }, ttl)).rejects.toThrow(
        RangeError
      );
      expect(await backend.hgetall(key)).toEqual({});
    });

    it("rounds a fractional ttl up rather than refusing it", async () => {
      await backend.hset("ttl:frac", { a: "1" }, 1.5);
      expect(await backend.hgetall("ttl:frac")).toEqual({ a: "1" });
    });

    it("leaves a batch unapplied when one op carries an unusable ttl", async () => {
      await expect(
        backend.batch(
          [
            { op: "set", key: "ttl:b1", value: "v" },
            { op: "expire", key: "ttl:b1", ttlSeconds: 1e17 },
          ],
          { atomic: true }
        )
      ).rejects.toThrow(RangeError);
      expect(await backend.get("ttl:b1")).toBeNull();
    });

    it("gives a reset bucket an expiry, as every other bucket write does", async () => {
      await backend.resetTokenBucket("ttl:bucket", 10);
      // Observable through the contract on both backends: the key is still there.
      expect((await backend.hgetall("ttl:bucket")).tokens).toBe("10");
    });
  });

  // --------------------------------------------------------- removal ordering

  /**
   * `addRequest` must commit before any `publish` issued after it is delivered, and
   * the transport does not guarantee that: ioredis retries a `NOSCRIPT` as a new
   * command a microtask later, so an abandonment published behind an outstanding add
   * can reach Redis first. The removal it triggers then finds nothing, and the add
   * goes on to create an entry with a live `ownerId` that nobody awaits — spared by
   * orphan cleanup and pinning the queue non-empty, which disables the fast path for
   * the whole client.
   *
   * These assert the invariant directly rather than trying to win that race: a
   * removal refuses a later add, whichever order the two actually arrive in.
   */
  describe("a removed request cannot be re-added", () => {
    const QUEUE = "ord:queue";
    const PREFIX = "ord:request";
    const queued = (requestId: string) => ({
      requestId,
      clientName: "c",
      requestName: "r",
      status: "pending" as const,
      priority: 1,
      cost: 1,
      retries: 0,
      timestamp: Date.now(),
      isThawRequest: false,
      ownerId: "owner-that-is-alive",
    });

    it("refuses an add for an id that was just removed", async () => {
      await backend.removeRequest(QUEUE, PREFIX, "gone");
      expect(await backend.addRequest(QUEUE, PREFIX, queued("gone"))).toBe(
        false
      );
      // The point of the refusal: nothing is left to pin the queue non-empty.
      expect(await backend.getQueueLength(QUEUE)).toBe(0);
      expect(await backend.getRequest(PREFIX, "gone")).toBeNull();
    });

    it("marks the id even when there was no entry to remove", async () => {
      // The case the marker exists for — the removal arrives first.
      expect(
        await backend.removeRequest(QUEUE, PREFIX, "never-added")
      ).toMatchObject({ wasThawRequest: false });
      expect(
        await backend.addRequest(QUEUE, PREFIX, queued("never-added"))
      ).toBe(false);
    });

    it("still enqueues an id that has not been removed", async () => {
      expect(await backend.addRequest(QUEUE, PREFIX, queued("fresh"))).toBe(
        true
      );
      expect(await backend.getQueueLength(QUEUE)).toBe(1);
    });

    /**
     * A retry keeps its entry rather than removing and re-adding, so it must never
     * be refused into nonexistence — the add is a no-op and the entry survives.
     */
    it("leaves a retry's surviving entry intact", async () => {
      await backend.addRequest(QUEUE, PREFIX, queued("retried"));
      const readded = await backend.addRequest(QUEUE, PREFIX, {
        ...queued("retried"),
        retries: 1,
      });
      expect(readded).toBe(false);
      expect(await backend.getRequest(PREFIX, "retried")).not.toBeNull();
    });
  });

  // ------------------------------------------------------- token bucket state

  describe("token bucket state", () => {
    it("advances lastUpdate with the refill it reports", async () => {
      const config = { maxTokens: 10, tokensToAdd: 1, interval: 1000 };
      const base = await backend.now();
      await backend.hset("tbs:key", {
        tokens: "0",
        lastUpdate: String(base - 2500),
      });
      const state = await backend.getTokenBucketState("tbs:key", config);
      // Whole intervals only, so 2 credited and lastUpdate moved 2000ms — a caller
      // deriving progress toward the next refill needs the pair to agree.
      expect(state.tokens).toBe(2);
      expect(state.lastUpdate - (base - 2500)).toBe(2000);
    });
  });
});
