import { performance } from "node:perf_hooks";
import { newHandler, registerClient } from "../lib/harness.mjs";
import { ms, stats } from "../lib/format.mjs";

/**
 * A realistic mix: several vendors at once, each with its own published limit,
 * plus bulk traffic competing with interactive traffic.
 *
 * This is the shape a real integration layer sees — not one client saturated
 * with identical requests. Errors are deliberately off: this measures achievable
 * throughput per vendor, and retry/freeze behaviour has its own tests, where
 * injecting failures here would mostly measure backoff timing.
 */
const UPSTREAM_MS = 60;
const DURATION_MS = 8000;

const VENDORS = [
  // Deliberately lumpy — a vendor limit of "1400 per 10s" taken literally. Its
  // tail is far worse than the equivalent 14-per-100ms, which is the point: the
  // average rate is the same, the p99 is not.
  {
    key: "carrier",
    perSec: 140,
    limit: {
      type: "requestLimit",
      interval: 10_000,
      tokensToAdd: 1400,
      maxTokens: 1400,
    },
  },
  {
    key: "store",
    perSec: 4,
    limit: {
      type: "requestLimit",
      interval: 1000,
      tokensToAdd: 4,
      maxTokens: 8,
    },
  },
  {
    key: "payments",
    perSec: 100,
    limit: {
      type: "requestLimit",
      interval: 1000,
      tokensToAdd: 100,
      maxTokens: 100,
    },
  },
];

export default {
  name: "realistic",
  title: "REALISTIC — three vendors, mixed priority and cost, own budgets",
  summary: "three vendors at once, each metered against its own budget",

  async run(ctx) {
    const { baseURL, setProfile } = ctx;
    setProfile({ p50: UPSTREAM_MS, p99: 400, rate429: 0, rate500: 0 });
    const handler = await newHandler(ctx, "bench-real");
    for (const v of VENDORS) {
      await registerClient(handler, v.key, baseURL, v.limit);
    }

    try {
      const deadline = Date.now() + DURATION_MS;
      const started = performance.now();

      const runVendor = async (v) => {
        // Little's law says perSec * latency requests in flight sustains the
        // rate; double it so a slow tail cannot make the driver, rather than
        // the limiter, the thing that caps throughput.
        const concurrency = Math.max(
          4,
          Math.ceil((v.perSec * UPSTREAM_MS) / 1000) * 2
        );
        // Each vendor is timed over its own window. Sharing one wall clock
        // across all three divides every vendor's count by the slowest vendor's
        // drain — a lumpy 10s-interval client with a multi-second tail then
        // makes its well-behaved neighbours look like they were throttled.
        const vendorStart = performance.now();
        const lat = [];
        let ok = 0;
        let failed = 0;
        // The limiter meters tokens, not requests. With cost-2 bulk traffic in
        // the mix, "100% of budget" is unreachable when counted in requests.
        let tokens = 0;
        await Promise.all(
          Array.from({ length: concurrency }, async (_, w) => {
            while (Date.now() < deadline) {
              // A fifth of the traffic is bulk: lower priority, heavier cost.
              const bulk = w % 5 === 0;
              const cost = bulk ? 2 : 1;
              const t = performance.now();
              try {
                await handler.handleRequest({
                  clientName: `${v.key}:_:bench`,
                  requestName: `bench.${v.key}`,
                  method: "GET",
                  url: "/",
                  priority: bulk ? 1 : 5,
                  cost,
                });
                lat.push(performance.now() - t);
                tokens += cost;
                ok++;
              } catch {
                failed++;
              }
            }
          })
        );
        return {
          v,
          lat,
          ok,
          failed,
          tokens,
          seconds: (performance.now() - vendorStart) / 1000,
        };
      };

      const results = await Promise.all(VENDORS.map(runVendor));
      const seconds = (performance.now() - started) / 1000;

      const rows = results.map(
        ({ v, lat, ok, failed, tokens, seconds: vs }) => {
          const st = stats(lat);
          return {
            vendor: v.key,
            budgetPerSecond: v.perSec,
            achieved: ok / vs,
            tokensPerSecond: tokens / vs,
            percentOfBudget: (tokens / vs / v.perSec) * 100,
            p50: st.p50,
            p99: st.p99,
            failed,
          };
        }
      );
      return {
        durationSeconds: DURATION_MS / 1000,
        upstreamMs: UPSTREAM_MS,
        totalThroughput: rows.reduce((n, r) => n + r.achieved, 0),
        totalFailed: rows.reduce((n, r) => n + r.failed, 0),
        wallSeconds: seconds,
        rows,
      };
    } finally {
      setProfile({});
      await handler.stop();
    }
  },

  report({ durationSeconds, upstreamMs, totalThroughput, totalFailed, rows }) {
    console.log(
      `  ${durationSeconds.toFixed(0)}s, ${upstreamMs} ms upstream, no injected errors, 20% bulk traffic at cost 2\n`
    );
    console.log(
      `  vendor      budget    achieved   tokens/s   of budget   p50       p99`
    );
    for (const r of rows) {
      console.log(
        `  ${r.vendor.padEnd(11)} ${String(r.budgetPerSecond + "/s").padEnd(9)} ` +
          `${String(Math.round(r.achieved) + "/s").padEnd(10)} ` +
          `${String(Math.round(r.tokensPerSecond) + "/s").padEnd(10)} ` +
          `${String(Math.round(r.percentOfBudget) + "%").padEnd(11)} ` +
          `${ms(r.p50).padEnd(9)} ${ms(r.p99)}`
      );
    }
    console.log(
      `\n  total ${Math.round(totalThroughput)} req/s across ${rows.length} vendors, ${totalFailed} failed`
    );
    console.log(
      `  of budget is tokens spent vs tokens granted — under is throttling, over is a leak`
    );
    console.log(
      `  store exceeds 100% legitimately: maxTokens is twice its refill, so a full\n` +
        `  bucket at t=0 is spendable burst that a short window still counts`
    );
  },
};
