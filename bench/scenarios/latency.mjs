import axios from "axios";
import { drive, newHandler, registerClient } from "../lib/harness.mjs";
import { ms, stats } from "../lib/format.mjs";

export default {
  name: "latency",
  title: "LATENCY — handler cost as the upstream slows to real speeds",
  summary: "handler overhead as upstream latency rises to realistic values",

  async run(ctx) {
    const { baseURL, setProfile } = ctx;
    const handler = await newHandler(ctx, "bench-l");
    const clientName = await registerClient(handler, "lat", baseURL);
    const http = axios.create({ baseURL });
    const rows = [];
    try {
      for (const p50 of [0, 10, 50, 200]) {
        setProfile({ p50, p99: p50 * 4 });
        const total = p50 === 0 ? 2000 : p50 <= 50 ? 1200 : 600;
        const concurrency = 50;
        const raw = await drive(() => http.get("/"), total, concurrency);
        const via = await drive(
          () =>
            handler.handleRequest({
              clientName,
              requestName: "bench.lat",
              method: "GET",
              url: "/",
            }),
          total,
          concurrency
        );
        const rawRps = total / raw.seconds;
        const viaRps = total / via.seconds;
        rows.push({
          upstreamP50: p50,
          rawThroughput: rawRps,
          handlerThroughput: viaRps,
          overheadPercent: 100 - (viaRps / rawRps) * 100,
          handlerP50: stats(via.latencies).p50,
        });
      }
    } finally {
      setProfile({});
      await handler.stop();
    }
    return { rows };
  },

  report({ rows }) {
    console.log(
      `  upstream p50   raw client   through handler   overhead   handler p50\n`
    );
    for (const r of rows) {
      console.log(
        `  ${String(r.upstreamP50 + " ms").padStart(9)}   ${String(Math.round(r.rawThroughput)).padStart(9)}   ` +
          `${String(Math.round(r.handlerThroughput)).padStart(15)}   ` +
          `${String(r.overheadPercent.toFixed(0) + "%").padStart(8)}   ` +
          `${ms(r.handlerP50).padStart(10)}`
      );
    }
  },
};
