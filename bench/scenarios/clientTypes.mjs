import { drive, newHandler, registerClient } from "../lib/harness.mjs";
import { ms, rps, stats } from "../lib/format.mjs";

const CASES = [
  { label: "noLimit", limit: { type: "noLimit" } },
  {
    label: "requestLimit",
    limit: {
      type: "requestLimit",
      interval: 1000,
      tokensToAdd: 300,
      maxTokens: 300,
    },
  },
  {
    label: "concurrencyLimit",
    limit: { type: "concurrencyLimit", maxConcurrency: 20 },
  },
  {
    label: "sharedLimit",
    limit: { type: "sharedLimit", clientName: "parent:_:bench" },
  },
];

export default {
  name: "clientTypes",
  title: "CLIENT TYPES — all four rate-limit strategies",
  summary: "all four rate-limit strategies, including sharedLimit",

  async run(ctx) {
    const { baseURL, setProfile } = ctx;
    setProfile({ p50: 15, p99: 60 });
    const handler = await newHandler(ctx, "bench-ct");

    // The parent whose budget the sharedLimit client borrows.
    await registerClient(handler, "parent", baseURL, {
      type: "requestLimit",
      interval: 1000,
      tokensToAdd: 300,
      maxTokens: 300,
    });

    const rows = [];
    try {
      for (const c of CASES) {
        const key = `ct${c.label}`;
        await registerClient(handler, key, baseURL, c.limit);
        // A sharedLimit client is addressed by the parent's name — that is the
        // whole point of the type, and worth asserting by using it.
        const target =
          c.limit.type === "sharedLimit"
            ? c.limit.clientName
            : `${key}:_:bench`;
        const total = 400;
        const { seconds, latencies, failed } = await drive(
          () =>
            handler.handleRequest({
              clientName: target,
              requestName: `bench.${c.label}`,
              method: "GET",
              url: "/",
            }),
          total,
          30
        );
        const s = stats(latencies);
        rows.push({
          type: c.label,
          requests: total,
          seconds,
          throughput: total / seconds,
          p50: s.p50,
          p99: s.p99,
          failed,
        });
      }
    } finally {
      setProfile({});
      await handler.stop();
    }
    return { upstreamMs: 15, concurrency: 30, rows };
  },

  report({ upstreamMs, concurrency, rows }) {
    console.log(`  ${upstreamMs} ms upstream, concurrency ${concurrency}\n`);
    console.log(
      `  type               throughput      p50       p99     failed`
    );
    for (const r of rows) {
      console.log(
        `  ${r.type.padEnd(18)} ${String(rps(r.requests, r.seconds)).padEnd(14)} ` +
          `${ms(r.p50).padEnd(9)} ${ms(r.p99).padEnd(9)} ${r.failed}`
      );
    }
  },
};
