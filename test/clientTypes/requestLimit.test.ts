import { RequestCostExceedsBudgetError } from "../../packages/core/src/errors.js";
import { fireMany, harnesses, startUpstream, withHandler } from "./harness.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

/**
 * `requestLimit` — a token bucket.
 *
 * The promise is a *rate*: over any window, no more than the configured budget
 * is spent. These check that the budget is respected, that nothing is dropped
 * while respecting it, and that `cost` is metered rather than ignored.
 */
describe.each(harnesses(5))("requestLimit — $name", (harness) => {
  let upstream: Awaited<ReturnType<typeof startUpstream>>;

  beforeAll(async () => {
    upstream = await startUpstream(0);
  });
  afterAll(async () => {
    await upstream.close();
  });

  const CLIENT = "rl:_:a";
  const BUCKET = "ct:requestHandler:rl:_:a:rateLimit:default";
  const QUEUE = "ct:requestHandler:rl:_:a:queue";
  const META = "ct:requestHandler:rl:_:a:request";

  const run = (
    rateLimit: Record<string, unknown>,
    fn: Parameters<typeof withHandler>[3]
  ) =>
    withHandler(
      harness,
      upstream.baseURL,
      [{ name: "rl", rateLimit: [{ type: "requestLimit", ...rateLimit }] }],
      fn
    );

  it("serves everything when the budget is ample", async () => {
    await run(
      { interval: 1000, tokensToAdd: 1000, maxTokens: 1000 },
      async (handler) => {
        const results = await fireMany(handler, CLIENT, 50);
        expect(results.every((r) => r.status === 200)).toBe(true);
      }
    );
  }, 30_000);

  it("loses nothing when the budget forces queueing", async () => {
    // Ten tokens per 300 ms against forty requests: most must queue, and every
    // one must still complete. A limiter that drops under pressure is worse
    // than no limiter, because the loss is silent.
    await run(
      { interval: 300, tokensToAdd: 10, maxTokens: 10 },
      async (handler) => {
        const results = await fireMany(handler, CLIENT, 40);
        expect(results).toHaveLength(40);
        expect(results.every((r) => r.status === 200)).toBe(true);
      }
    );
  }, 30_000);

  it("holds the configured rate over a window", async () => {
    // 20 tokens/second against 60 requests cannot finish in under ~2s.
    await run(
      { interval: 1000, tokensToAdd: 20, maxTokens: 20 },
      async (handler) => {
        const started = Date.now();
        await fireMany(handler, CLIENT, 60);
        // Slack for scheduling, but finishing near-instantly would mean the
        // budget was never consulted.
        expect(Date.now() - started).toBeGreaterThan(1_500);
      }
    );
  }, 30_000);

  it("spends `cost` rather than counting requests", async () => {
    await run(
      { interval: 60_000, tokensToAdd: 10, maxTokens: 10 },
      async (handler, backend) => {
        await handler.handleRequest({
          clientName: CLIENT,
          requestName: "t.costly",
          method: "GET",
          url: "/",
          cost: 4,
        });
        const state = await backend.getTokenBucketState(BUCKET, {
          maxTokens: 10,
          tokensToAdd: 10,
          interval: 60_000,
        });
        expect(state.tokens).toBe(6);
      }
    );
  }, 30_000);

  it("rejects a request that can never fit the budget", async () => {
    // Queueing it forever would present as a hang; failing fast is the contract.
    await run(
      { interval: 1000, tokensToAdd: 5, maxTokens: 5 },
      async (handler) => {
        await expect(
          handler.handleRequest({
            clientName: CLIENT,
            requestName: "t.toobig",
            method: "GET",
            url: "/",
            cost: 6,
          })
        ).rejects.toBeInstanceOf(RequestCostExceedsBudgetError);
      }
    );
  }, 30_000);

  it("absorbs a burst up to maxTokens and then paces", async () => {
    // maxTokens above tokensToAdd is the documented burst allowance: a full
    // bucket at t=0 covers all twenty with no refill wait.
    await run(
      { interval: 1000, tokensToAdd: 5, maxTokens: 20 },
      async (handler) => {
        const started = Date.now();
        await fireMany(handler, CLIENT, 20);
        expect(Date.now() - started).toBeLessThan(1_000);
      }
    );
  }, 30_000);

  it("reports remaining tokens in stats", async () => {
    await run(
      { interval: 60_000, tokensToAdd: 100, maxTokens: 100 },
      async (handler) => {
        await fireMany(handler, CLIENT, 10);
        const stats = await handler.getClientStats(CLIENT);
        // One limit, reported the same way several are: a named entry apiece.
        expect(stats.rateLimit).toHaveLength(1);
        expect(stats.rateLimit[0]).toMatchObject({
          type: "requestLimit",
          name: "default",
          tokens: 90,
        });
      }
    );
  }, 30_000);

  it("leaves the queue empty once the work is done", async () => {
    await run(
      { interval: 200, tokensToAdd: 10, maxTokens: 10 },
      async (handler, backend) => {
        await fireMany(handler, CLIENT, 30);
        // Removal rides the requestDone round-trip, so let it land.
        await new Promise((r) => setTimeout(r, 500));
        const stats = await backend.getQueueStats(QUEUE, META);
        expect(stats.pending + stats.inProgress).toBe(0);
      }
    );
  }, 30_000);
});
