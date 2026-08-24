import {
  CONCURRENCIES,
  drive,
  newHandler,
  registerClient,
} from "../lib/harness.mjs";
import { ms, rps, stats } from "../lib/format.mjs";

export default {
  name: "throughput",
  title: "THROUGHPUT — one unthrottled client, rising concurrency",
  summary: "sustained req/s through one unthrottled client",
  needs: ["baseline"],

  async run(ctx) {
    const { baseURL, shared } = ctx;
    const handler = await newHandler(ctx, "bench-t");
    const clientName = await registerClient(handler, "bench", baseURL);
    const rows = [];
    try {
      for (const concurrency of CONCURRENCIES) {
        const total = concurrency <= 10 ? 500 : 2000;
        const { seconds, latencies, failed } = await drive(
          () =>
            handler.handleRequest({
              clientName,
              requestName: "bench.noop",
              method: "GET",
              url: "/",
            }),
          total,
          concurrency
        );
        const s = stats(latencies);
        const floor = shared.floor?.[concurrency];
        rows.push({
          concurrency,
          requests: total,
          seconds,
          throughput: total / seconds,
          p50: s.p50,
          p95: s.p95,
          p99: s.p99,
          failed,
          percentOfUpstream: floor ? (total / seconds / floor) * 100 : null,
        });
      }
    } finally {
      await handler.stop();
    }
    return { rows };
  },

  report({ rows }) {
    for (const r of rows) {
      const overhead =
        r.percentOfUpstream === null
          ? ""
          : ` (${r.percentOfUpstream.toFixed(0)}% of upstream at this concurrency)`;
      console.log(
        `  concurrency ${String(r.concurrency).padStart(3)}     ${rps(r.requests, r.seconds)}   ` +
          `p50 ${ms(r.p50)}  p95 ${ms(r.p95)}  p99 ${ms(r.p99)}  failed ${r.failed}${overhead}`
      );
    }
  },
};
