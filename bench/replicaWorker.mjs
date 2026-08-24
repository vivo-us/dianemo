#!/usr/bin/env node
/**
 * One replica in the replica benchmark — a separate OS process with its own
 * event loop, its own Redis connection and its own handler, which is how a
 * deployment actually runs. Forked by scripts/benchmark.mjs; not run directly.
 *
 * Protocol over IPC:
 *   parent -> { type: "config", ... }   configure and connect
 *   child  -> { type: "ready" }         handler started, client registered
 *   parent -> { type: "go" }            all replicas start together
 *   child  -> { type: "done", ... }     per-replica timings
 */
import { performance } from "node:perf_hooks";
import { Redis } from "ioredis";
import RequestHandler from "../packages/core/dist/index.js";
import { redisBackend } from "../packages/backend-redis/dist/index.js";

let handler;
let redis;

const send = (msg) => process.send?.(msg);

process.on("message", async (msg) => {
  try {
    if (msg.type === "config") {
      redis = new Redis(msg.redisUrl);
      handler = new RequestHandler({
        key: msg.key,
        backend: redisBackend(redis),
        keyPrefix: msg.prefix,
      });
      await handler.registerClientTemplate(msg.template, (creds) => [
        {
          name: `${msg.template}:_:${creds.instanceId}`,
          rateLimit: msg.rateLimit ?? { type: "noLimit" },
          requestOptions: { defaults: { baseURL: msg.baseURL } },
        },
      ]);
      await handler.start();
      // Every replica registers the same credential, exactly as N pods of one
      // service each load the same integration row.
      await handler.addTemplateClient(msg.template, { instanceId: "bench" });
      send({ type: "ready" });
      return;
    }

    if (msg.type === "go") {
      const clientName = `${msg.template}:_:bench`;
      const latencies = [];
      let issued = 0;
      let failed = 0;
      const started = performance.now();

      await Promise.all(
        Array.from({ length: msg.concurrency }, async () => {
          while (issued < msg.total) {
            issued++;
            const t = performance.now();
            try {
              await handler.handleRequest({
                clientName,
                requestName: "bench.replica",
                method: "GET",
                url: "/",
              });
              latencies.push(performance.now() - t);
            } catch {
              failed++;
            }
          }
        })
      );

      send({
        type: "done",
        seconds: (performance.now() - started) / 1000,
        latencies,
        failed,
        completed: latencies.length,
      });
      return;
    }

    if (msg.type === "shutdown") {
      await handler?.stop();
      await redis?.quit();
      process.exit(0);
    }
  } catch (error) {
    send({ type: "error", message: String(error?.message ?? error) });
    process.exit(1);
  }
});
