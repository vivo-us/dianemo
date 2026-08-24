import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  fire,
  fireMany,
  harnesses,
  startUpstream,
  withHandler,
} from "./harness.js";

/**
 * `noLimit` — no budget, no queue, no coordination.
 *
 * The interesting claim is a negative one: that nothing is enqueued and no
 * budget is consulted. A `noLimit` client that quietly routed through the queue
 * would still pass every functional test while costing several backend
 * round-trips per request.
 */
describe.each(harnesses(4))("noLimit — $name", (harness) => {
  let upstream: Awaited<ReturnType<typeof startUpstream>>;

  beforeAll(async () => {
    upstream = await startUpstream(0);
  });
  afterAll(async () => {
    await upstream.close();
  });

  const CLIENT = "nl:_:a";
  const QUEUE = "ct:requestHandler:nl:_:a:queue";
  const META = "ct:requestHandler:nl:_:a:request";

  const run = (fn: Parameters<typeof withHandler>[3]) =>
    withHandler(
      harness,
      upstream.baseURL,
      [{ name: "nl", rateLimit: { type: "noLimit" } }],
      fn
    );

  it("serves every request", async () => {
    await run(async (handler) => {
      const results = await fireMany(handler, CLIENT, 25);
      expect(results).toHaveLength(25);
      expect(results.every((r) => r.status === 200)).toBe(true);
    });
  }, 30_000);

  it("never touches the queue", async () => {
    await run(async (handler, backend) => {
      await fireMany(handler, CLIENT, 50);
      const stats = await backend.getQueueStats(QUEUE, META);
      expect(stats.pending + stats.inProgress).toBe(0);
      expect(await backend.getAllRequests(QUEUE, META)).toEqual([]);
    });
  }, 30_000);

  it("applies no back-pressure at all", async () => {
    // The documented caveat, asserted so it cannot drift into a surprise: with
    // no budget, offered concurrency reaches the upstream unchanged.
    //
    // The delay is load-bearing. Against an instant upstream each request
    // finishes before the next is issued, so nothing ever overlaps and any
    // limit — or none — would look identical.
    upstream.setDelay(25);
    try {
      await run(async (handler) => {
        upstream.resetCounters();
        await fireMany(handler, CLIENT, 40);
        expect(upstream.peakInFlight()).toBeGreaterThan(5);
        expect(upstream.servedCount()).toBe(40);
      });
    } finally {
      upstream.setDelay(0);
    }
  }, 30_000);

  it("reports its type in stats", async () => {
    await run(async (handler) => {
      await fire(handler, CLIENT);
      expect(await handler.getClientStats(CLIENT)).toMatchObject({
        rateLimit: { type: "noLimit" },
      });
    });
  }, 30_000);
});
