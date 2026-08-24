import { fork } from "node:child_process";
import { performance } from "node:perf_hooks";
import { KEY } from "../lib/harness.mjs";
import { ms, rps, stats } from "../lib/format.mjs";

/**
 * N separate OS processes sharing one budget — the arrangement dianemo exists
 * for, and the one a single-process benchmark cannot say anything about.
 *
 * Redis only, and deliberately so: separate processes cannot share an
 * in-process budget, so on the memory backend each replica would enforce its
 * own private copy of the limit. The combined figure would look excellent and
 * mean nothing.
 */
const WORKER = new URL("../replicaWorker.mjs", import.meta.url).pathname;
const TOTAL = 1600;
const TOTAL_CONCURRENCY = 40;
const PER_REPLICA_CONCURRENCY = 20;

/** Waits for one message of a given type from every child. */
const collect = (children, type) =>
  Promise.all(
    children.map(
      (c) =>
        new Promise((resolve, reject) => {
          const onMessage = (m) => {
            if (m.type === type) {
              c.off("message", onMessage);
              resolve(m);
            } else if (m.type === "error") {
              c.off("message", onMessage);
              reject(new Error(m.message));
            }
          };
          c.on("message", onMessage);
        })
    )
  );

async function runPass(ctx, { fixedTotalConcurrency }) {
  const { baseURL, redisUrl } = ctx;
  const rows = [];
  const counts = process.env.REPLICA_COUNTS
    ? process.env.REPLICA_COUNTS.split(",").map(Number)
    : [1, 2, 4, 8];

  for (const count of counts) {
    const perReplica = Math.floor(TOTAL / count);
    const concurrency = fixedTotalConcurrency
      ? Math.max(1, Math.floor(TOTAL_CONCURRENCY / count))
      : PER_REPLICA_CONCURRENCY;
    const children = [];

    try {
      for (let i = 0; i < count; i++) {
        children.push(fork(WORKER, { stdio: "inherit" }));
      }
      for (const c of children) {
        c.send({
          type: "config",
          redisUrl,
          key: KEY,
          prefix: "bench-r",
          template: "shared",
          baseURL,
        });
      }
      await collect(children, "ready");

      // Start every replica at once and time the whole fleet from here, so the
      // combined figure is real wall clock rather than a sum of overlapping runs.
      const wallStart = performance.now();
      for (const c of children) {
        c.send({
          type: "go",
          template: "shared",
          total: perReplica,
          concurrency,
        });
      }
      const results = await collect(children, "done");
      const wallSeconds = (performance.now() - wallStart) / 1000;

      const latencies = results.flatMap((r) => r.latencies);
      const completed = results.reduce((n, r) => n + r.completed, 0);
      const failed = results.reduce((n, r) => n + r.failed, 0);
      const s = stats(latencies);
      rows.push({
        processes: count,
        concurrencyPerProcess: concurrency,
        totalConcurrency: concurrency * count,
        seconds: wallSeconds,
        completed,
        throughput: completed / wallSeconds,
        p50: s.p50,
        p99: s.p99,
        failed,
      });
    } finally {
      for (const c of children) c.send({ type: "shutdown" });
      await Promise.all(
        children.map(
          (c) =>
            new Promise((resolve) => {
              const timer = setTimeout(() => {
                c.kill("SIGKILL");
                resolve();
              }, 5000);
              c.once("exit", () => {
                clearTimeout(timer);
                resolve();
              });
            })
        )
      );
    }
  }
  return rows;
}

export default {
  name: "replicas",
  title: "REPLICAS — N separate OS processes sharing one budget",
  summary: "N handlers sharing one budget, as separate processes would",
  backends: ["redis"],
  skipReason:
    "separate processes cannot share an in-process budget, so each replica would\n" +
    "  enforce its own private copy of the limit — the combined number would look\n" +
    "  excellent and mean nothing",

  async run(ctx) {
    // Two framings, because they answer different questions. Scaling
    // concurrency with replicas measures what a growing deployment actually
    // offers; holding total concurrency fixed isolates coordination overhead
    // from the added load.
    return {
      scalingLoad: await runPass(ctx, { fixedTotalConcurrency: false }),
      fixedLoad: await runPass(ctx, { fixedTotalConcurrency: true }),
    };
  },

  report({ scalingLoad, fixedLoad }) {
    const table = (rows) => {
      for (const r of rows) {
        console.log(
          `  ${String(r.processes).padStart(2)} process${r.processes > 1 ? "es" : "  "}  ` +
            `(${String(r.totalConcurrency).padStart(3)} concurrent)   ` +
            `${rps(r.completed, r.seconds)} combined   ` +
            `p50 ${ms(r.p50)}  p99 ${ms(r.p99)}  failed ${r.failed}`
        );
      }
    };
    console.log(
      "  a) each replica offers its own load (what a growing deployment does)"
    );
    table(scalingLoad);
    console.log("  b) total load held constant (isolates coordination cost)");
    table(fixedLoad);
  },
};
