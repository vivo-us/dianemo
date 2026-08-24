#!/usr/bin/env node
/**
 * Throughput and scale benchmark for a dianemo handler.
 *
 * Measures the coordination layer, not the network: every request is served by
 * a local HTTP server on loopback, so what is being timed is backend
 * round-trips, queue ordering and leader election — the parts that decide how
 * many clients and requests one deployment can carry.
 *
 * Each scenario is a file in ./scenarios exporting a default object:
 *
 *   name         selector on the command line
 *   title        printed header
 *   summary      one line for --list
 *   run(ctx)     performs the measurement and RETURNS plain data
 *   report(data) prints that data for a human
 *   optIn        excluded from a bare run; must be named explicitly (or --all)
 *   needs        other scenarios to run first (results land in ctx.shared)
 *   backends     restrict to certain backends; skipReason explains the skip
 *
 * Keeping `run` free of printing is what makes --json possible: the same data
 * either goes through `report` or straight out as JSON.
 *
 * Usage:
 *   node bench/index.mjs                          every default scenario
 *   node bench/index.mjs baseline throughput      just these
 *   node bench/index.mjs --all                    including the opt-in ones
 *   node bench/index.mjs --list                   what is available
 *   node bench/index.mjs --backend=memory         no Redis required
 *   node bench/index.mjs --json                   machine-readable results
 *
 * REDIS_URL is required unless --backend=memory.
 */
import { readdirSync } from "node:fs";
import { performance } from "node:perf_hooks";
import axios from "axios";
import { Redis } from "ioredis";
import { drive } from "./lib/harness.mjs";
import { startUpstream } from "./lib/upstream.mjs";

// ------------------------------------------------------------------ scenarios

const dir = new URL("./scenarios/", import.meta.url);
const files = readdirSync(dir)
  .filter((f) => f.endsWith(".mjs"))
  .sort();

const loaded = await Promise.all(
  files.map((f) => import(new URL(f, dir).href).then((m) => m.default))
);

// A stable, meaningful order: cheap and explanatory first, long ones last. Any
// scenario not listed here still runs, alphabetically, after the known ones.
const ORDER = [
  "baseline",
  "throughput",
  "latency",
  "realistic",
  "clientTypes",
  "burst",
  "clients",
  "queue",
  "replicas",
  "sustained",
  "backendOps",
];
const rank = (s) => {
  const i = ORDER.indexOf(s.name);
  return i === -1 ? ORDER.length : i;
};
const SCENARIOS = loaded.sort(
  (a, b) => rank(a) - rank(b) || (a.name < b.name ? -1 : 1)
);
const byName = new Map(SCENARIOS.map((s) => [s.name, s]));

// ------------------------------------------------------------------ arguments

const argv = process.argv.slice(2);
const flag = (name) => argv.some((a) => a === `--${name}`);
const option = (name) =>
  argv.find((a) => a.startsWith(`--${name}=`))?.slice(name.length + 3);

const BACKEND = option("backend") ?? "redis";
const REDIS_URL = process.env.REDIS_URL;

if (flag("list")) {
  const width = Math.max(...SCENARIOS.map((s) => s.name.length));
  console.log("\nScenarios:\n");
  for (const s of SCENARIOS) {
    const tags = [
      s.optIn ? "opt-in" : null,
      s.backends ? `${s.backends.join("/")} only` : null,
    ].filter(Boolean);
    console.log(
      `  ${s.name.padEnd(width)}  ${s.summary}${tags.length ? `  [${tags.join(", ")}]` : ""}`
    );
  }
  console.log(
    `\n  A bare run does everything except the opt-in ones. Name them, or --all.\n`
  );
  process.exit(0);
}

if (!["redis", "memory"].includes(BACKEND)) {
  console.error(`Unknown --backend=${BACKEND}. Use "redis" or "memory".`);
  process.exit(1);
}
if (BACKEND === "redis" && !REDIS_URL) {
  console.error("REDIS_URL is required for --backend=redis");
  process.exit(1);
}

// Names may arrive positionally or via the older --only=a,b form.
const requested = [
  ...argv.filter((a) => !a.startsWith("--")),
  ...(option("only")?.split(",").filter(Boolean) ?? []),
];

const unknown = requested.filter((n) => !byName.has(n));
if (unknown.length) {
  console.error(
    `Unknown scenario${unknown.length > 1 ? "s" : ""}: ${unknown.join(", ")}\n` +
      `Available: ${SCENARIOS.map((s) => s.name).join(", ")}`
  );
  process.exit(1);
}

let selected;
if (requested.length) {
  selected = SCENARIOS.filter((s) => requested.includes(s.name));
} else {
  selected = SCENARIOS.filter((s) => flag("all") || !s.optIn);
}

// Pull in prerequisites (baseline publishes the floor throughput compares to).
for (const s of [...selected]) {
  for (const need of s.needs ?? []) {
    if (!selected.some((x) => x.name === need)) selected.push(byName.get(need));
  }
}
selected.sort((a, b) => rank(a) - rank(b));

// ----------------------------------------------------------------------- main

const { server, baseURL, setProfile } = await startUpstream();

// Warm the process before measuring: V8 needs to JIT the hot paths and the
// socket pool needs to exist. Without this the first scenario measured is
// systematically slower — and since the baseline usually runs first, it would
// report a floor worse than the handler it is supposed to bound.
{
  const warm = axios.create({ baseURL });
  await drive(() => warm.get("/"), 2000, 50);
}

let redis = null;
let version = "n/a";
if (BACKEND === "redis") {
  redis = new Redis(REDIS_URL);
  await redis.ping();
  version =
    /redis_version:(\S+)/.exec(await redis.info("server"))?.[1] ?? "unknown";
}

const JSON_OUT = flag("json");

if (!JSON_OUT) {
  console.log(
    `\ndianemo benchmark — node ${process.version}, backend ${BACKEND}` +
      (BACKEND === "redis" ? ` (redis ${version})` : " (in-process)")
  );
  console.log("upstream: local HTTP on loopback (no network, no vendor)\n");
}

const ctx = {
  baseURL,
  setProfile,
  redis,
  redisUrl: REDIS_URL,
  backend: BACKEND,
  /** Cross-scenario results, e.g. the baseline floor. */
  shared: {},
};

const report = {
  tool: "dianemo-bench",
  node: process.version,
  backend: BACKEND,
  redisVersion: BACKEND === "redis" ? version : null,
  startedAt: new Date().toISOString(),
  scenarios: [],
};

// A few scenarios narrate as they go, which is a feature when watching and
// noise when piping JSON.
const write = console.log;
const silence = () => {
  console.log = () => {};
};
const restore = () => {
  console.log = write;
};

try {
  for (const scenario of selected) {
    if (scenario.backends && !scenario.backends.includes(BACKEND)) {
      report.scenarios.push({
        name: scenario.name,
        skipped: true,
        reason: `unsupported on the ${BACKEND} backend`,
      });
      if (!JSON_OUT) {
        write(
          `${scenario.title}\n  Skipped on the ${BACKEND} backend` +
            (scenario.skipReason ? `:\n  ${scenario.skipReason}` : ".") +
            "\n"
        );
      }
      continue;
    }

    if (!JSON_OUT) write(scenario.title);
    const started = performance.now();
    if (JSON_OUT) silence();
    let result;
    try {
      result = await scenario.run(ctx);
    } finally {
      if (JSON_OUT) restore();
    }
    const seconds = (performance.now() - started) / 1000;

    report.scenarios.push({
      name: scenario.name,
      seconds,
      ...(result ?? {}),
    });

    if (!JSON_OUT) {
      scenario.report?.(result);
      if (flag("timings")) write(`  (${seconds.toFixed(1)}s)`);
      write();
    }
  }
  if (JSON_OUT) write(JSON.stringify(report, null, 2));
} finally {
  restore();
  if (redis) {
    await redis.flushdb();
    await redis.quit();
  }
  // `server.close()` stops accepting new sockets but waits on open ones, and
  // Node's fetch keeps connections alive in a pool — so the loop would stay
  // alive and the process would hang after the results printed.
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
  await globalThis[Symbol.for("undici.globalDispatcher.1")]?.close?.();
}
