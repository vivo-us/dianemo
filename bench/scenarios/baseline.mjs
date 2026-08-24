import axios from "axios";
import { CONCURRENCIES, drive } from "../lib/harness.mjs";
import { ms, rps, stats } from "../lib/format.mjs";

export default {
  name: "baseline",
  title: "BASELINE — same HTTP client, no handler, per concurrency level",
  summary: "upstream latency floor, no handler involved",

  async run({ baseURL, shared }) {
    const http = axios.create({ baseURL });
    const floor = {};
    const rows = [];
    for (const concurrency of CONCURRENCIES) {
      const total = concurrency <= 10 ? 500 : 2000;
      const { seconds, latencies } = await drive(
        () => http.get("/"),
        total,
        concurrency
      );
      const s = stats(latencies);
      floor[concurrency] = total / seconds;
      rows.push({
        concurrency,
        requests: total,
        seconds,
        throughput: total / seconds,
        p50: s.p50,
        p99: s.p99,
      });
    }
    shared.floor = floor;
    return { rows };
  },

  report({ rows }) {
    for (const r of rows) {
      console.log(
        `  concurrency ${String(r.concurrency).padStart(3)}     ${rps(r.requests, r.seconds)}   ` +
          `p50 ${ms(r.p50)}  p99 ${ms(r.p99)}`
      );
    }
  },
};
