import { performance } from "node:perf_hooks";
import { drive, newHandler } from "../lib/harness.mjs";
import { ms, rps, stats } from "../lib/format.mjs";

export default {
  name: "clients",
  title: "CLIENTS — cost of N registered clients on one handler",
  summary: "cost of N registered clients on one handler",

  async run(ctx) {
    const { baseURL } = ctx;
    const rows = [];
    for (const count of [1, 10, 50, 200]) {
      const handler = await newHandler(ctx, `bench-c${count}`);
      try {
        const t0 = performance.now();
        await handler.registerClientTemplate("multi", (creds) =>
          Array.from({ length: count }, (_, i) => ({
            name: `multi:_:${creds.instanceId}:${i}`,
            rateLimit: { type: "noLimit" },
            requestOptions: { defaults: { baseURL } },
          }))
        );
        await handler.addTemplateClient("multi", { instanceId: "bench" });
        const buildMs = performance.now() - t0;

        const total = 400;
        const { seconds, latencies } = await drive(
          () =>
            handler.handleRequest({
              clientName: "multi:_:bench:0",
              requestName: "bench.noop",
              method: "GET",
              url: "/",
            }),
          total,
          20
        );
        const s = stats(latencies);
        rows.push({
          clients: count,
          buildMs,
          requests: total,
          seconds,
          throughput: total / seconds,
          p50: s.p50,
          p99: s.p99,
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
        `  ${String(r.clients).padStart(3)} clients        build ${ms(r.buildMs)}   ` +
          `then ${rps(r.requests, r.seconds)}   p50 ${ms(r.p50)}  p99 ${ms(r.p99)}`
      );
    }
  },
};
