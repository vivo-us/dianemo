import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { redisBackend } from "../packages/backend-redis/src/index.js";
import { Redis } from "ioredis";
import type {
  ConcurrencyConfig,
  DianemoBackend,
  QueuedRequest,
  TokenBucketConfig,
} from "../packages/core/src/backend/types.js";

/**
 * What the Lua does with a number it cannot use — a config field, a cost, or a
 * field read back out of a key.
 *
 * Redis only, and deliberately below the client layer. Every value here is one
 * the TypeScript above never produces: it is passed as config or planted
 * directly in a key, so the script is the only thing between it and the
 * keyspace. The cases a caller can actually reach belong in the conformance
 * suite, which runs both backends; these are the ones only the Lua can answer
 * for.
 *
 * db6 is SHARED with `clientTypes/concurrencyLimit.test.ts`, and both flush it.
 * `fileParallelism: false` in vitest.config.ts is what keeps that safe, so it is
 * load-bearing here rather than a performance setting. Do not read a free index
 * off the harness comment — db15 looked free by that reading and has three
 * claimants, one of them `replicas.failover`. See the warning on `harnesses` in
 * clientTypes/harness.ts for what a collision looks like.
 */

const REDIS_URL = process.env.REDIS_URL;
const TEST_DB = 6;

const CONCURRENCY: ConcurrencyConfig = {
  maxConcurrency: 2,
  requestTtl: 60_000,
};
const BUCKET: TokenBucketConfig = {
  maxTokens: 10,
  tokensToAdd: 1,
  interval: 1000,
};

const COST_ERROR = "cost must be a finite number that is not negative";

const queued = (requestId: string): QueuedRequest => ({
  requestId,
  clientName: "c",
  requestName: "n",
  status: "pending",
  priority: 1,
  cost: 1,
  retries: 0,
  timestamp: Date.now(),
  isThawRequest: false,
  ownerId: "o",
});

describe.skipIf(!REDIS_URL)("lua numeric guards", () => {
  let redis: Redis;
  let backend: DianemoBackend;

  beforeAll(async () => {
    redis = new Redis(`${REDIS_URL}/${TEST_DB}`);
    await redis.ping();
    backend = redisBackend(redis);
  });

  beforeEach(async () => {
    await redis.flushdb();
  });

  afterAll(async () => {
    await backend.close();
    await redis.quit();
  });

  // ------------------------------------------------------- concurrency cost

  describe("a cost a slot cannot hold", () => {
    it.each([[NaN], [Infinity], [-1]])(
      "refuses %j from acquireConcurrency and says why",
      async (cost) => {
        expect(
          await backend.acquireConcurrency("g:ac", cost, "r1", CONCURRENCY)
        ).toMatchObject({
          acquired: false,
          currentConcurrency: 0,
          error: COST_ERROR,
        });
        expect(await redis.zcard("g:ac")).toBe(0);
        expect(await redis.hgetall("g:ac:costs")).toEqual({});
      }
    );

    it.each([[NaN], [Infinity], [-1]])(
      "refuses %j from acquireQueuedConcurrency without storing it",
      async (cost) => {
        await redis.hset("g:qm", "status", "inProgress");
        expect(
          await backend.acquireQueuedConcurrency?.(
            "g:aq",
            "g:qm",
            cost,
            "r1",
            CONCURRENCY
          )
        ).toMatchObject({
          acquired: false,
          currentConcurrency: 0,
          error: COST_ERROR,
        });
        expect(await redis.hgetall("g:aq:costs")).toEqual({});
      }
    );

    it.each([[NaN], [Infinity], [-1]])(
      "refuses %j from tryAdmitConcurrency without storing it",
      async (cost) => {
        expect(
          await backend.tryAdmitConcurrency(
            "g:tq",
            "g:tc",
            "g:tf",
            cost,
            "r1",
            CONCURRENCY
          )
        ).toBe(false);
        expect(await redis.hgetall("g:tc:costs")).toEqual({});
      }
    );

    it.each([[0], [0.5]])("admits a cost of %j on every path", async (cost) => {
      expect(
        await backend.acquireConcurrency("g:ok", cost, "r1", CONCURRENCY)
      ).toMatchObject({ acquired: true });
      await redis.hset("g:okm", "status", "inProgress");
      expect(
        await backend.acquireQueuedConcurrency?.(
          "g:okq",
          "g:okm",
          cost,
          "r2",
          CONCURRENCY
        )
      ).toMatchObject({ acquired: true });
      expect(
        await backend.tryAdmitConcurrency(
          "g:okqueue",
          "g:okc",
          "g:okf",
          cost,
          "r3",
          CONCURRENCY
        )
      ).toBe(true);
    });

    it("admits a cost that exactly fills the ceiling and refuses the next", async () => {
      expect(
        await backend.acquireConcurrency("g:edge", 2, "r1", CONCURRENCY)
      ).toMatchObject({ acquired: true, currentConcurrency: 2 });
      expect(
        await backend.acquireConcurrency("g:edge", 0.5, "r2", CONCURRENCY)
      ).toMatchObject({ acquired: false, currentConcurrency: 2 });
    });
  });

  // ------------------------------------------------------ stored slot costs

  describe("a slot whose stored cost is not a usable number", () => {
    const oneSlot: ConcurrencyConfig = {
      maxConcurrency: 1,
      requestTtl: 60_000,
    };

    beforeEach(async () => {
      await backend.acquireConcurrency("g:pc", 1, "held", oneSlot);
      await redis.hset("g:pc:costs", "held", "nan");
    });

    it("keeps the ceiling on acquireConcurrency", async () => {
      expect(
        await backend.acquireConcurrency("g:pc", 1, "next", oneSlot)
      ).toMatchObject({ acquired: false, currentConcurrency: 1 });
    });

    it("keeps the ceiling on the no-queue fast path", async () => {
      expect(
        await backend.tryAdmitConcurrency(
          "g:pcq",
          "g:pc",
          "g:pcf",
          1,
          "next",
          oneSlot
        )
      ).toBe(false);
    });

    it("reports the occupancy instead of failing the whole read", async () => {
      expect(await backend.getConcurrencyState("g:pc", 60_000)).toMatchObject({
        currentConcurrency: 1,
        activeRequests: ["held"],
      });
    });
  });

  it("leaves an expired slot alone when the ceiling it was handed is unusable", async () => {
    await backend.acquireConcurrency("g:reap", 1, "old", CONCURRENCY);
    await redis.zadd("g:reap", 0, "old");
    await redis.hset("g:reapm", "status", "inProgress");

    expect(
      await backend.acquireQueuedConcurrency?.("g:reap", "g:reapm", 1, "new", {
        maxConcurrency: NaN,
        requestTtl: 60_000,
      })
    ).toMatchObject({ acquired: false, currentConcurrency: 0 });
    expect(await redis.zscore("g:reap", "old")).toBe("0");
  });

  // ------------------------------------------------------------ freeze gate

  describe("a stored frozenUntil no clock can be compared against", () => {
    it.each([
      ["nan", "5"],
      ["inf", "0"],
    ])(
      "reads frozenUntil %j with %j probes owed as no freeze at all",
      async (frozenUntil, thawRequestCount) => {
        await redis.hset("g:fz", { frozenUntil, thawRequestCount });

        expect(await backend.getFreezeState("g:fz")).toBeNull();
        expect(await backend.isFrozen("g:fz")).toBe(false);
        expect(await backend.tryAdmitNoLimit("g:fzq", "g:fz")).toBe(true);
        expect(
          await backend.tryAdmitImmediately("g:fzq", "g:fzb", "g:fz", 1, BUCKET)
        ).toBe(true);
        expect(
          await backend.tryAdmitConcurrency(
            "g:fzq",
            "g:fzc",
            "g:fz",
            1,
            "r1",
            CONCURRENCY
          )
        ).toBe(true);
      }
    );

    it("still blocks the fast paths while a usable deadline stands", async () => {
      const now = await backend.now();
      await redis.hset("g:live", {
        frozenUntil: String(now + 60_000),
        thawRequestCount: "0",
      });
      expect(await backend.tryAdmitNoLimit("g:liveq", "g:live")).toBe(false);
      expect(
        await backend.tryAdmitImmediately(
          "g:liveq",
          "g:liveb",
          "g:live",
          1,
          BUCKET
        )
      ).toBe(false);
      expect(
        await backend.tryAdmitConcurrency(
          "g:liveq",
          "g:livec",
          "g:live",
          1,
          "r1",
          CONCURRENCY
        )
      ).toBe(false);
    });
  });

  // ---------------------------------------------------------- freeze arming

  describe("a freeze deadline beyond what the key can carry", () => {
    it("clamps the deadline and leaves the key with a live TTL", async () => {
      expect(await backend.armFreeze("g:arm", 1e20, 1)).toMatchObject({
        frozenUntil: Number.MAX_SAFE_INTEGER,
      });
      expect(await redis.hget("g:arm", "frozenUntil")).toBe(
        String(Number.MAX_SAFE_INTEGER)
      );
      expect(await redis.ttl("g:arm")).toBeGreaterThan(0);
    });

    it("brings a deadline already stored past the ceiling back down", async () => {
      await redis.hset("g:armed", {
        frozenUntil: "9223372036854775807",
        thawRequestCount: "1",
      });
      const now = await backend.now();

      expect(await backend.armFreeze("g:armed", now + 1_000, 1)).toMatchObject({
        frozenUntil: Number.MAX_SAFE_INTEGER,
      });
      expect(await redis.ttl("g:armed")).toBeGreaterThan(0);
    });

    it("sizes an ordinary deadline's TTL from the deadline itself", async () => {
      const now = await backend.now();
      await backend.armFreeze("g:normal", now + 120_000, 1);
      const ttl = await redis.ttl("g:normal");
      expect(ttl).toBeGreaterThan(150);
      expect(ttl).toBeLessThanOrEqual(181);
    });
  });

  // --------------------------------------------------------- bucket interval

  describe("a token-bucket interval that is not a finite number", () => {
    it.each([[Infinity], [NaN]])(
      "refuses an interval of %j instead of spending a token under it",
      async (interval) => {
        expect(
          await backend.acquireTokens("g:tb", 1, { ...BUCKET, interval })
        ).toMatchObject({
          acquired: false,
          waitTime: 0,
          remainingTokens: 0,
          error: "interval must be a finite number",
        });
        expect(await redis.exists("g:tb")).toBe(0);
      }
    );

    it.each([[Infinity], [NaN]])(
      "refuses the fast path on an interval of %j without writing the bucket",
      async (interval) => {
        expect(
          await backend.tryAdmitImmediately("g:tbq", "g:tb2", "g:tbf", 1, {
            ...BUCKET,
            interval,
          })
        ).toBe(false);
        expect(await redis.exists("g:tb2")).toBe(0);
      }
    );
  });

  // -------------------------------------------------------- frozen grant set

  describe("frozen-grant bookkeeping", () => {
    it("releases a grant whose stored deadline is unusable", async () => {
      await redis.sadd("g:grants", "g1");
      await redis.hset("g:st:g1:freezeState", {
        frozenUntil: "nan",
        thawRequestCount: "0",
      });

      expect(
        await backend.cleanupStaleFrozenGrants(
          "g:grants",
          "g:gq",
          "g:gm",
          "g:st:"
        )
      ).toBe(1);
      expect(await redis.smembers("g:grants")).toEqual([]);
    });

    it("keeps a grant whose freeze has not lapsed", async () => {
      await redis.sadd("g:grants2", "g1");
      const now = await backend.now();
      await redis.hset("g:st2:g1:freezeState", {
        frozenUntil: String(now + 60_000),
        thawRequestCount: "0",
      });

      expect(
        await backend.cleanupStaleFrozenGrants(
          "g:grants2",
          "g:gq2",
          "g:gm2",
          "g:st2:"
        )
      ).toBe(0);
      expect(await redis.smembers("g:grants2")).toEqual(["g1"]);
    });

    it("expires the frozen-grants set it adds to", async () => {
      await backend.tryStartThawRequest(
        "g:grants3",
        "g:gq3",
        "g:gm3",
        "r1",
        "g1"
      );

      expect(await redis.smembers("g:grants3")).toEqual(["g1"]);
      expect(await redis.ttl("g:grants3")).toBeGreaterThan(0);
    });
  });

  // ------------------------------------------------------------ queue rescore

  describe("re-scoring a queue entry", () => {
    it("writes neither the metadata nor the score when the entry has no request id", async () => {
      await backend.addRequest("g:uq", "g:um", queued("r1"));
      const score = await redis.zscore("g:uq", "r1");
      await redis.hdel("g:um:r1", "requestId");

      await backend.updateRequest("g:uq", "g:um", "r1", { priority: 5 });

      expect(await redis.hget("g:um:r1", "priority")).toBe("1");
      expect(await redis.zscore("g:uq", "r1")).toBe(score);
    });

    it("re-scores an entry that still carries its request id", async () => {
      await backend.addRequest("g:uq2", "g:um2", queued("r1"));
      const score = await redis.zscore("g:uq2", "r1");

      await backend.updateRequest("g:uq2", "g:um2", "r1", { priority: 5 });

      expect(await redis.hget("g:um2:r1", "priority")).toBe("5");
      expect(await redis.zscore("g:uq2", "r1")).not.toBe(score);
    });

    it("still applies a status-only update to an entry with no request id", async () => {
      await backend.addRequest("g:uq3", "g:um3", queued("r1"));
      await redis.hdel("g:um3:r1", "requestId");

      await backend.updateRequest("g:uq3", "g:um3", "r1", {
        status: "inProgress",
      });

      expect(await redis.hget("g:um3:r1", "status")).toBe("inProgress");
    });
  });
});
