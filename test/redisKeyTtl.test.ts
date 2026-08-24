import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import type { DianemoBackend } from "../packages/core/src/backend/types.js";
import { redisBackend } from "../packages/backend-redis/src/index.js";
import { Redis } from "ioredis";

/**
 * The half of the expiry contract no backend-agnostic test can reach.
 *
 * `DianemoBackend` exposes no way to read a key's TTL, so `conformance.test.ts`
 * can only assert the lease a slot is held for — the milliseconds side of the
 * transposed-TTL hazard `lua/fragments.ts` warns about. The other direction, a
 * key given `requestTtl` as its own expiry in SECONDS, is invisible from there
 * and shows up only as `TTL` on the key. Same for an `EXPIRE` deleted outright:
 * every assertion the contract can make still passes on a key that lives for
 * ever.
 *
 * Redis only, on db12 beside `sharedLimit.scenarios` — see the map on
 * `harnesses` in `test/clientTypes/harness.ts`; no index is unclaimed.
 *   REDIS_URL=redis://localhost:6399 npm test
 */
const REDIS_URL = process.env.REDIS_URL;
const describeIfRedis = REDIS_URL ? describe : describe.skip;
const TEST_DB = 12;

/** The 24 hours every key the scripts write is sized to. */
const KEY_TTL_SECONDS = 86_400;

/**
 * Far enough above the day-long expiry that reading it as seconds is
 * unmistakable, and far enough above the assertions' lower bound that reading
 * the day-long expiry as a lease cannot be mistaken for it either.
 */
const REQUEST_TTL_MS = 120_000;

describeIfRedis("redis key expiry", () => {
  let redis: Redis;
  let backend: DianemoBackend;

  const config = { maxConcurrency: 2, requestTtl: REQUEST_TTL_MS };

  const expectDayLongTtl = async (key: string) => {
    const ttl = await redis.ttl(key);
    expect(ttl).toBeGreaterThan(86_000);
    expect(ttl).toBeLessThanOrEqual(KEY_TTL_SECONDS);
  };

  beforeAll(async () => {
    redis = new Redis(`${REDIS_URL}/${TEST_DB}`);
    await redis.flushdb();
    backend = redisBackend(redis);
  });

  afterEach(async () => {
    await redis.flushdb();
  });

  afterAll(async () => {
    await redis.flushdb();
    await backend.close();
    redis.disconnect();
  });

  it("expires a fast-path slot key a day out, not on the request's lease", async () => {
    expect(
      await backend.tryAdmitConcurrency(
        "kt:queue",
        "kt:slots",
        "kt:freezeState",
        1,
        "r1",
        config
      )
    ).toBe(true);

    await expectDayLongTtl("kt:slots");
    await expectDayLongTtl("kt:slots:costs");
  });

  it("expires an acquired slot key a day out, not on the request's lease", async () => {
    expect(
      (await backend.acquireConcurrency("kt:acq", 1, "r1", config)).acquired
    ).toBe(true);

    await expectDayLongTtl("kt:acq");
    await expectDayLongTtl("kt:acq:costs");
  });

  it("expires a queued slot key a day out, not on the request's lease", async () => {
    await backend.hset("kt:request:r1", { status: "inProgress" });
    expect(
      (
        await backend.acquireQueuedConcurrency!(
          "kt:queued",
          "kt:request:r1",
          1,
          "r1",
          config
        )
      ).acquired
    ).toBe(true);

    await expectDayLongTtl("kt:queued");
    await expectDayLongTtl("kt:queued:costs");
  });

  it("gives a reset bucket an expiry rather than leaving it to live for ever", async () => {
    await backend.resetTokenBucket("kt:bucket", 10);

    await expectDayLongTtl("kt:bucket");
  });
});
