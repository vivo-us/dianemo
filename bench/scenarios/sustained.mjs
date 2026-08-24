import { performance } from "node:perf_hooks";
import { drive, newHandler, registerClient } from "../lib/harness.mjs";
import { median, ms, rps, stats } from "../lib/format.mjs";

/**
 * A long run at fixed concurrency.
 *
 * Short bursts hide the things that only appear over time: GC pauses, memory
 * that never comes back, a queue that grows faster than it drains, connection
 * churn. This reports throughput per window rather than one average, so decay is
 * visible instead of averaged away.
 *
 * Opt-in, because it takes far longer than the rest.
 * `SUSTAINED_TOTAL` / `SUSTAINED_CONCURRENCY` tune it.
 */
export default {
  name: "sustained",
  title: "SUSTAINED — a long run at fixed concurrency",
  summary: "long run (default 100k) reporting throughput over time",
  optIn: true,

  async run(ctx) {
    const { baseURL } = ctx;
    const total = Number(process.env.SUSTAINED_TOTAL || 100_000);
    const concurrency = Number(process.env.SUSTAINED_CONCURRENCY || 50);
    const windowSize = Math.max(1000, Math.floor(total / 10));

    const handler = await newHandler(ctx, "bench-s");
    const clientName = await registerClient(handler, "sustained", baseURL);

    try {
      const latencies = new Float64Array(total);
      const windows = [];
      let issued = 0;
      let completed = 0;
      let failed = 0;
      let windowStart = performance.now();
      let windowCount = 0;
      const rssStart = process.memoryUsage().rss;
      let rssPeak = rssStart;

      // Warm the handler path itself. The global warmup only exercises raw
      // HTTP, so without this the first window absorbs JIT cost and the drift
      // check reports a rise that is really the process getting up to speed.
      await drive(
        () =>
          handler.handleRequest({
            clientName,
            requestName: "bench.warmup",
            method: "GET",
            url: "/",
          }),
        Math.min(5000, Math.max(1000, Math.floor(total / 20))),
        concurrency
      );

      console.log(
        `  ${total.toLocaleString()} requests, concurrency ${concurrency}, reporting every ${windowSize.toLocaleString()}\n`
      );
      const started = performance.now();

      await Promise.all(
        Array.from({ length: concurrency }, async () => {
          while (issued < total) {
            const slot = issued++;
            if (slot >= total) break;
            const t = performance.now();
            try {
              await handler.handleRequest({
                clientName,
                requestName: "bench.sustained",
                method: "GET",
                url: "/",
              });
              latencies[slot] = performance.now() - t;
            } catch {
              failed++;
              latencies[slot] = NaN;
            }
            completed++;
            if (++windowCount === windowSize) {
              const elapsed = (performance.now() - windowStart) / 1000;
              const rss = process.memoryUsage().rss;
              rssPeak = Math.max(rssPeak, rss);
              windows.push({
                completed,
                throughput: windowCount / elapsed,
                rss,
              });
              // Printed as it happens: a long run is more useful watched live
              // than reported at the end.
              console.log(
                `    ${String(completed).padStart(7)}  ${rps(windowCount, elapsed).padStart(11)}` +
                  `   rss ${(rss / 1048576).toFixed(0)} MB`
              );
              windowCount = 0;
              windowStart = performance.now();
            }
          }
        })
      );

      const seconds = (performance.now() - started) / 1000;
      const clean = Array.from(latencies).filter((n) => Number.isFinite(n));
      const s = stats(clean);
      const sorted = [...clean].sort((a, b) => a - b);
      const p999 =
        sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.999))];

      // Compare halves by median, and drop the first window: even after a
      // warmup pass it runs slightly cold, and letting it anchor the comparison
      // reported a ~57% "rise" on a run that was flat throughout.
      const measured = (windows.length > 2 ? windows.slice(1) : windows).map(
        (w) => w.throughput
      );
      const half = Math.floor(measured.length / 2);
      const firstHalf = median(measured.slice(0, half || 1));
      const secondHalf = median(measured.slice(half));
      const drift = firstHalf
        ? ((secondHalf - firstHalf) / firstHalf) * 100
        : 0;

      return {
        requests: total,
        concurrency,
        seconds,
        throughput: completed / seconds,
        completed,
        failed,
        latency: { p50: s.p50, p95: s.p95, p99: s.p99, p999, max: s.max },
        memory: { startBytes: rssStart, peakBytes: rssPeak },
        driftPercent: drift,
        windows,
      };
    } finally {
      await handler.stop();
    }
  },

  report(r) {
    console.log(
      `\n    overall      ${rps(r.completed, r.seconds)}   over ${r.seconds.toFixed(1)}s   failed ${r.failed}`
    );
    console.log(
      `    latency      p50 ${ms(r.latency.p50)}  p95 ${ms(r.latency.p95)}  ` +
        `p99 ${ms(r.latency.p99)}  p99.9 ${ms(r.latency.p999)}  max ${ms(r.latency.max)}`
    );
    console.log(
      `    memory       ${(r.memory.startBytes / 1048576).toFixed(0)} MB start, ` +
        `${(r.memory.peakBytes / 1048576).toFixed(0)} MB peak`
    );
    console.log(
      `    throughput   ${r.driftPercent >= 0 ? "+" : ""}${r.driftPercent.toFixed(1)}% second half vs first` +
        `${Math.abs(r.driftPercent) < 10 ? " (stable)" : " (DRIFTING — investigate)"}`
    );
  },
};
