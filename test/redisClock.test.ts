import type { DianemoBackend } from "../packages/core/src/backend/types.js";
import { redisBackend } from "../packages/backend-redis/src/index.js";
import { Redis } from "ioredis";
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from "vitest";

/**
 * The Redis backend reads timestamps on Redis's clock, never the process's.
 *
 * `lastUpdate` is written by whichever replica last refilled the bucket, and
 * every acquire script measures elapsed time with redis `TIME`. A reader on the
 * local clock therefore answers a different question than the bucket will: ahead
 * of Redis it reports refill that will not be granted, behind it hides refill
 * that will. Skew of a few seconds between hosts is ordinary, and the memory
 * backend cannot exhibit this at all — its own clock is the clock — so this is
 * the one place the rule has to be checked directly.
 *
 * Skipped unless REDIS_URL is set:
 *   REDIS_URL=redis://localhost:6399 npm test
 */
const REDIS_URL = process.env.REDIS_URL;
const describeIfRedis = REDIS_URL ? describe : describe.skip;
const TEST_DB = 11;

describeIfRedis("redis backend clock source", () => {
  let redis: Redis;
  let backend: DianemoBackend;

  const KEY = "clock:bucket";
  const CONFIG = { maxTokens: 5, tokensToAdd: 5, interval: 60_000 };

  beforeAll(async () => {
    redis = new Redis(`${REDIS_URL}/${TEST_DB}`);
    await redis.flushdb();
    backend = redisBackend(redis);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  afterAll(async () => {
    await redis.flushdb();
    await backend.close();
    redis.disconnect();
  });

  it("reports the balance the bucket will actually honour, on a clock running ahead", async () => {
    await redis.del(KEY);

    // Drain the bucket. lastUpdate is now stamped from redis TIME.
    const drained = await backend.acquireTokens(KEY, 5, CONFIG);
    expect(drained.acquired).toBe(true);

    // Push this process ten whole intervals ahead of Redis.
    const realNow = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(realNow + 10 * CONFIG.interval);

    const state = await backend.getTokenBucketState(KEY, CONFIG);
    vi.restoreAllMocks();

    // On the local clock this would have credited a full refill.
    expect(state.tokens).toBe(0);

    // And the report is honest: the bucket really does refuse.
    const next = await backend.acquireTokens(KEY, 1, CONFIG);
    expect(next.acquired).toBe(false);
  });

  it("reports the balance the bucket will actually honour, on a clock running behind", async () => {
    await redis.del(KEY);

    const drained = await backend.acquireTokens(KEY, 5, CONFIG);
    expect(drained.acquired).toBe(true);

    // Let a real interval elapse on Redis's clock, so a refill is genuinely due.
    await redis.hset(
      KEY,
      "lastUpdate",
      (await backend.now()) - CONFIG.interval
    );

    // Drag this process back behind Redis, which on a local clock would hide it.
    const realNow = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(realNow - 10 * CONFIG.interval);

    const state = await backend.getTokenBucketState(KEY, CONFIG);
    vi.restoreAllMocks();

    expect(state.tokens).toBe(5);

    const next = await backend.acquireTokens(KEY, 1, CONFIG);
    expect(next.acquired).toBe(true);
  });
});
