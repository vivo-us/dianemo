import { drive, newHandler, registerClient } from "../lib/harness.mjs";
import { ms, rps, stats } from "../lib/format.mjs";

export default {
  name: "queue",
  title: "QUEUE — admission cost when a rate limit is binding",
  summary: "admission cost when a rate limit forces queueing",

  async run(ctx) {
    const { baseURL } = ctx;
    const rows = [];
    for (const [tokens, interval] of [
      [50, 1000],
      [200, 1000],
      [1000, 1000],
    ]) {
      const handler = await newHandler(ctx, `bench-q${tokens}`);
      try {
        const clientName = await registerClient(handler, "throttled", baseURL, {
          type: "requestLimit",
          interval,
          tokensToAdd: tokens,
          maxTokens: tokens,
        });
        const total = Math.min(tokens, 300);
        const { seconds, latencies, failed } = await drive(
          () =>
            handler.handleRequest({
              clientName,
              requestName: "bench.throttled",
              method: "GET",
              url: "/",
            }),
          total,
          50
        );
        const s = stats(latencies);
        rows.push({
          budgetPerSecond: tokens,
          requests: total,
          seconds,
          throughput: total / seconds,
          p50: s.p50,
          p99: s.p99,
          failed,
        });
      } finally {
        await handler.stop();
      }
    }
    return { rows };
  },

  report({ rows }) {
    for (const r of rows) {
      console.log(
        `  budget ${String(r.budgetPerSecond).padStart(4)}/s      ${rps(r.requests, r.seconds)} admitted   ` +
          `p50 ${ms(r.p50)}  p99 ${ms(r.p99)}  failed ${r.failed}`
      );
    }
  },
};
