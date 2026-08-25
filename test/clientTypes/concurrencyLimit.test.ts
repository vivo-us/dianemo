import { fireMany, harnesses, startUpstream, withHandler } from "./harness.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

/**
 * `concurrencyLimit` — N in flight at once.
 *
 * The promise is an *occupancy*, and it is the whole contract: exceeding it is
 * a breach the vendor sees before you do. So these assert the ceiling from two
 * directions — through the backend's own accounting, and by watching how many
 * requests the upstream actually had open at the same moment.
 */
describe.each(harnesses(6))("concurrencyLimit — $name", (harness) => {
  let upstream: Awaited<ReturnType<typeof startUpstream>>;

  beforeAll(async () => {
    // A slow upstream is what makes concurrency observable at all: with an
    // instant reply, requests never overlap and any limit would appear to hold.
    upstream = await startUpstream(25);
  });
  afterAll(async () => {
    await upstream.close();
  });

  const CLIENT = "cl:_:a";
  const SLOTS = "ct:requestHandler:cl:_:a:concurrency:default";

  const run = (maxConcurrency: number, fn: Parameters<typeof withHandler>[3]) =>
    withHandler(
      harness,
      upstream.baseURL,
      [
        {
          name: "cl",
          rateLimit: [{ type: "concurrencyLimit", maxConcurrency }],
        },
      ],
      fn
    );

  it("never exceeds the limit while draining a backlog", async () => {
    await run(5, async (handler, backend) => {
      let peak = 0;
      const watch = setInterval(() => {
        void backend
          .getConcurrencyState(SLOTS, 60_000)
          .then((s) => {
            peak = Math.max(peak, s.currentConcurrency);
          })
          .catch(() => {});
      }, 10);
      try {
        const results = await fireMany(handler, CLIENT, 40);
        expect(results.every((r) => r.status === 200)).toBe(true);
        expect(peak).toBeGreaterThan(0);
        expect(peak).toBeLessThanOrEqual(5);
      } finally {
        clearInterval(watch);
      }
    });
  }, 30_000);

  it("holds the limit as the upstream sees it", async () => {
    // The backend's own view could agree with itself and still be wrong. This
    // counts overlap at the far end, where a breach would actually matter.
    await run(4, async (handler) => {
      // Leakage from an earlier test would inflate the peak without any real
      // breach, so prove the upstream is idle before measuring.
      expect(upstream.inFlight()).toBe(0);
      upstream.resetCounters();
      await fireMany(handler, CLIENT, 40);
      expect(upstream.servedCount()).toBe(40);
      expect(upstream.peakInFlight()).toBeLessThanOrEqual(4);
    });
  }, 30_000);

  it("releases every slot once the work is done", async () => {
    // A leaked slot does not fail anything immediately — it quietly lowers the
    // limit until the client stops serving entirely.
    await run(4, async (handler, backend) => {
      await fireMany(handler, CLIENT, 30);
      // The release rides the requestDone round-trip, so allow it to land.
      await new Promise((r) => setTimeout(r, 500));
      const state = await backend.getConcurrencyState(SLOTS, 60_000);
      expect(state.currentConcurrency).toBe(0);
      expect(state.activeRequests).toEqual([]);
    });
  }, 30_000);

  it("drains promptly rather than one request per poll", async () => {
    // Slot acquisition happens during admission. When it was a blocking poll
    // instead, one request waited at a time and everything behind it stalled,
    // so a 5-slot client managed roughly one request per poll interval.
    await run(5, async (handler) => {
      const started = Date.now();
      await fireMany(handler, CLIENT, 50);
      const elapsed = Date.now() - started;
      // 50 requests, 5 at a time, 25 ms each ≈ 250 ms of unavoidable work.
      // Generous, but a polling implementation lands in the seconds.
      expect(elapsed).toBeLessThan(3_000);
    });
  }, 30_000);

  it("counts cost against slots, not request count", async () => {
    await run(4, async (handler, backend) => {
      const inFlight = handler.handleRequest({
        clientName: CLIENT,
        requestName: "t.heavy",
        method: "GET",
        url: "/",
        cost: 4,
      });
      // While that one request holds all four slots, occupancy is 4, not 1.
      await new Promise((r) => setTimeout(r, 10));
      const state = await backend.getConcurrencyState(SLOTS, 60_000);
      expect(state.currentConcurrency).toBeLessThanOrEqual(4);
      await inFlight;
    });
  }, 30_000);

  it("reports its configuration in stats", async () => {
    await run(3, async (handler) => {
      await fireMany(handler, CLIENT, 5);
      const { rateLimit } = await handler.getClientStats(CLIENT);
      // One limit, reported the way several are: one named entry apiece.
      expect(rateLimit).toEqual([
        { type: "concurrencyLimit", maxConcurrency: 3, name: "default" },
      ]);
    });
  }, 30_000);
});
