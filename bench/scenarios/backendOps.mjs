import { performance } from "node:perf_hooks";
import { memoryBackend } from "../../packages/core/dist/index.js";
import { redisBackend } from "../../packages/backend-redis/dist/index.js";
import { Redis } from "ioredis";

/**
 * Coordination cost with no HTTP in the path.
 *
 * Runs both backends regardless of `--backend`, because a one-sided number
 * answers nothing. See docs/benchmarks.md for why the other scenarios cannot
 * tell the backends apart.
 */
const OPS = 20_000;
const CONCURRENCY = 50;

/** Each workload is one full unit of the work a request actually causes. */
const WORKLOADS = {
  "tryAdmitImmediately (fast path)": (backend) =>
    backend.tryAdmitImmediately(
      "b:queue",
      "b:bucket",
      "b:freeze",
      1,
      { maxTokens: 1e9, tokensToAdd: 1e9, interval: 1000 },
      3600
    ),
  "acquireTokens (queued path)": (backend) =>
    backend.acquireTokens("b:bucket2", 1, {
      maxTokens: 1e9,
      tokensToAdd: 1e9,
      interval: 1000,
    }),
  "enqueue → dequeue → remove": async (backend, i) => {
    const requestId = `r${i}`;
    await backend.addRequest("b:q2", "b:req", {
      requestId,
      clientName: "c",
      requestName: "r",
      status: "pending",
      priority: 5,
      cost: 1,
      retries: 0,
      timestamp: Date.now(),
      isThawRequest: false,
      ownerId: "o",
    });
    await backend.getNextRequest("b:q2", "b:req");
    await backend.removeRequest("b:q2", "b:req", requestId);
  },
};

async function build(kind, redisUrl) {
  if (kind === "memory") {
    return { backend: memoryBackend(), close: async () => {} };
  }
  const conn = new Redis(redisUrl);
  await conn.flushdb();
  const backend = redisBackend(conn);
  return {
    backend,
    close: async () => {
      await backend.close();
      await conn.quit();
    },
  };
}

export default {
  name: "backendOps",
  title: "BACKEND OPS — coordination cost with no HTTP in the path",
  summary: "memory vs redis on the raw coordination primitives",
  optIn: true,

  async run({ redisUrl }) {
    const kinds = redisUrl ? ["memory", "redis"] : ["memory"];
    const results = {};

    for (const kind of kinds) {
      const { backend, close } = await build(kind, redisUrl);
      results[kind] = {};
      try {
        for (const [label, work] of Object.entries(WORKLOADS)) {
          let issued = 0;
          const started = performance.now();
          await Promise.all(
            Array.from({ length: CONCURRENCY }, async () => {
              while (issued < OPS) await work(backend, issued++);
            })
          );
          results[kind][label] = OPS / ((performance.now() - started) / 1000);
        }
      } finally {
        await close();
      }
    }

    return {
      ops: OPS,
      concurrency: CONCURRENCY,
      redisMeasured: Boolean(redisUrl),
      operations: Object.keys(WORKLOADS).map((label) => ({
        operation: label,
        memory: results.memory[label],
        redis: results.redis?.[label] ?? null,
        speedup: results.redis?.[label]
          ? results.memory[label] / results.redis[label]
          : null,
      })),
    };
  },

  report({ ops, concurrency, redisMeasured, operations }) {
    const pad = 34;
    if (!redisMeasured) console.log("  (redis skipped — REDIS_URL not set)\n");
    console.log(
      `  ${ops.toLocaleString()} ops per workload, concurrency ${concurrency}, no HTTP\n`
    );
    console.log(
      `  ${"operation".padEnd(pad)} ${"memory".padStart(13)} ${"redis".padStart(13)}   speedup`
    );
    for (const r of operations) {
      console.log(
        `  ${r.operation.padEnd(pad)} ${(Math.round(r.memory).toLocaleString() + "/s").padStart(13)} ` +
          `${(r.redis ? Math.round(r.redis).toLocaleString() + "/s" : "—").padStart(13)}   ` +
          `${r.speedup ? `${r.speedup.toFixed(1)}×` : "—"}`
      );
    }
  },
};
