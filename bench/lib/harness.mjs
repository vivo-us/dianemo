import { performance } from "node:perf_hooks";
import RequestHandler, {
  memoryBackend,
} from "../../packages/core/dist/index.js";
import { redisBackend } from "../../packages/backend-redis/dist/index.js";

/** Not a secret — the benchmark encrypts nothing worth protecting. */
export const KEY = "0123456789abcdef0123456789abcdef";

export const CONCURRENCIES = [1, 10, 50, 200];

/**
 * A handler on whichever backend is under test.
 *
 * The memory backend gets a fresh instance per handler: it has no shared server
 * to namespace against, so isolation comes from the object itself.
 */
export async function newHandler({ backend, redis }, prefix) {
  const handler = new RequestHandler({
    key: KEY,
    backend: backend === "memory" ? memoryBackend() : redisBackend(redis),
    keyPrefix: prefix,
  });
  await handler.start();
  return handler;
}

export async function registerClient(handler, name, baseURL, rateLimit) {
  await handler.registerClientTemplate(name, (creds) => [
    {
      name: `${name}:_:${creds.instanceId}`,
      rateLimit: rateLimit ?? { type: "noLimit" },
      requestOptions: { defaults: { baseURL } },
    },
  ]);
  await handler.addTemplateClient(name, { instanceId: "bench" });
  return `${name}:_:bench`;
}

/** Fires `total` requests with at most `concurrency` in flight. */
export async function drive(fn, total, concurrency) {
  const latencies = [];
  let issued = 0;
  let failed = 0;
  const started = performance.now();
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (issued < total) {
        issued++;
        const t = performance.now();
        try {
          await fn();
          latencies.push(performance.now() - t);
        } catch {
          failed++;
        }
      }
    })
  );
  return { seconds: (performance.now() - started) / 1000, latencies, failed };
}
