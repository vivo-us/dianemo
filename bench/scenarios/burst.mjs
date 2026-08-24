import { performance } from "node:perf_hooks";
import { newHandler, registerClient } from "../lib/harness.mjs";

const PER_SECOND = 50;

export default {
  name: "burst",
  title: "BURST — idle, then far more than the budget at once",
  summary: "idle then a burst far exceeding the budget",

  async run(ctx) {
    const { baseURL, setProfile } = ctx;
    setProfile({ p50: 20, p99: 80 });
    const handler = await newHandler(ctx, "bench-b");
    const clientName = await registerClient(handler, "burst", baseURL, {
      type: "requestLimit",
      interval: 1000,
      tokensToAdd: PER_SECOND,
      maxTokens: PER_SECOND,
    });
    const rows = [];
    try {
      for (const size of [50, 200, 500]) {
        const started = performance.now();
        const results = await Promise.allSettled(
          Array.from({ length: size }, () =>
            handler.handleRequest({
              clientName,
              requestName: "bench.burst",
              method: "GET",
              url: "/",
            })
          )
        );
        const seconds = (performance.now() - started) / 1000;
        const succeeded = results.filter(
          (r) => r.status === "fulfilled"
        ).length;
        rows.push({
          size,
          seconds,
          minimumSeconds: Math.max(0, (size - PER_SECOND) / PER_SECOND),
          succeeded,
          throughput: succeeded / seconds,
        });
      }
    } finally {
      setProfile({});
      await handler.stop();
    }
    return { budgetPerSecond: PER_SECOND, rows };
  },

  report({ rows }) {
    for (const r of rows) {
      console.log(
        `  burst of ${String(r.size).padStart(3)}   drained in ${r.seconds.toFixed(1)}s   ` +
          `(budget implies >=${r.minimumSeconds.toFixed(1)}s)   ${r.succeeded}/${r.size} succeeded   ` +
          `${Math.round(r.throughput)} req/s`
      );
    }
  },
};
