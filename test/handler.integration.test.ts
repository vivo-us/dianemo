import type { DianemoBackend } from "../packages/core/src/backend/types.js";
import { memoryBackend } from "../packages/core/src/backend/memory.js";
import { redisBackend } from "../packages/backend-redis/src/index.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import RequestHandler from "../packages/core/src/index.js";
import { createServer, type Server } from "node:http";
import { Redis } from "ioredis";

/**
 * A whole handler, end to end, on each backend.
 *
 * The conformance suite proves the backends agree on the primitives; this
 * proves the handler behaves the same when it is driving them — the fast path,
 * the queued fallback, and the concurrency limit that is only worth anything if
 * it is never exceeded.
 *
 * Assertions go through the backend interface rather than poking at storage, so
 * the same test is meaningful for a backend that has no Redis keys to inspect.
 *
 * The memory backend always runs. Redis runs when REDIS_URL is set.
 */

const REDIS_URL = process.env.REDIS_URL;
const TEST_DB = 14;
const KEY = "0123456789abcdef0123456789abcdef";

interface Harness {
  name: string;
  create: () => Promise<{
    backend: DianemoBackend;
    cleanup: () => Promise<void>;
  }>;
}

const harnesses: Harness[] = [
  {
    name: "memory",
    create: async () => {
      const backend = memoryBackend();
      return { backend, cleanup: async () => backend.close() };
    },
  },
];

if (REDIS_URL) {
  harnesses.push({
    name: "redis",
    create: async () => {
      const redis = new Redis(`${REDIS_URL}/${TEST_DB}`);
      await redis.flushdb();
      const backend = redisBackend(redis);
      return {
        backend,
        cleanup: async () => {
          await backend.close();
          await redis.quit();
        },
      };
    },
  });
}

describe.each(harnesses)("handler end to end — $name", (harness) => {
  let server: Server;
  let baseURL: string;

  beforeAll(async () => {
    server = createServer((_req, res) => {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end('{"ok":true}');
    });
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
    const addr = server.address();
    baseURL = `http://127.0.0.1:${
      typeof addr === "object" && addr ? addr.port : 0
    }`;
  });
  afterAll(async () => {
    server.closeAllConnections();
    await new Promise<void>((r) => server.close(() => r()));
  });

  const withHandler = async (
    rateLimit: Record<string, unknown>[],
    fn: (
      handler: RequestHandler,
      clientName: string,
      backend: DianemoBackend
    ) => Promise<void>
  ) => {
    const { backend, cleanup } = await harness.create();
    const handler = new RequestHandler({ key: KEY, backend, keyPrefix: "e2e" });
    await handler.registerClientTemplate(
      "t" as never,
      ((creds: { instanceId: string }) => [
        {
          name: `t:_:${creds.instanceId}`,
          rateLimit,
          requestOptions: { defaults: { baseURL } },
        },
      ]) as never
    );
    await handler.start();
    await handler.addTemplateClient("t" as never, { instanceId: "a" } as never);
    try {
      await fn(handler, "t:_:a", backend);
    } finally {
      await handler.stop();
      await cleanup();
    }
  };

  const fire = (handler: RequestHandler, clientName: string) =>
    handler.handleRequest({
      clientName,
      requestName: "t.noop",
      method: "GET",
      url: "/",
    });

  const QUEUE = "e2e:requestHandler:t:_:a:queue";
  const META = "e2e:requestHandler:t:_:a:request";
  const CONCURRENCY = "e2e:requestHandler:t:_:a:concurrency:default";

  it("serves an unlimited client without touching the queue", async () => {
    await withHandler([{ type: "noLimit" }], async (handler, name, backend) => {
      const results = await Promise.all(
        Array.from({ length: 25 }, () => fire(handler, name))
      );
      expect(results).toHaveLength(25);
      expect(results.every((r) => r.status === 200)).toBe(true);
      // Nothing should have been enqueued at all.
      const stats = await backend.getQueueStats(QUEUE, META);
      expect(stats.pending + stats.inProgress).toBe(0);
    });
  }, 30_000);

  it("falls back to the queue when the budget runs out, losing nothing", async () => {
    // Ten tokens, forty requests: the first few take the fast path, the rest
    // must queue and still complete.
    await withHandler(
      [{ type: "requestLimit", interval: 300, tokensToAdd: 10, maxTokens: 10 }],
      async (handler, name) => {
        const results = await Promise.all(
          Array.from({ length: 40 }, () => fire(handler, name))
        );
        expect(results).toHaveLength(40);
        expect(results.every((r) => r.status === 200)).toBe(true);
      }
    );
  }, 30_000);

  it("holds a concurrency limit exactly while draining a backlog", async () => {
    // The limit is the whole contract: more in flight than configured is a
    // breach, and the limiter is worthless if it cannot also drain promptly.
    await withHandler(
      [{ type: "concurrencyLimit", maxConcurrency: 5 }],
      async (handler, name, backend) => {
        let peak = 0;
        const watch = setInterval(() => {
          void backend
            .getConcurrencyState(CONCURRENCY, 60_000)
            .then((state) => {
              peak = Math.max(peak, state.currentConcurrency);
            })
            .catch(() => {});
        }, 3);
        try {
          const results = await Promise.all(
            Array.from({ length: 120 }, () => fire(handler, name))
          );
          expect(results.every((r) => r.status === 200)).toBe(true);
          expect(peak).toBeGreaterThan(0);
          expect(peak).toBeLessThanOrEqual(5);
        } finally {
          clearInterval(watch);
        }
      }
    );
  }, 30_000);

  it("releases every concurrency slot once the work is done", async () => {
    await withHandler(
      [{ type: "concurrencyLimit", maxConcurrency: 4 }],
      async (handler, name, backend) => {
        await Promise.all(
          Array.from({ length: 40 }, () => fire(handler, name))
        );
        // The release rides the requestDone round-trip, so allow it to land.
        await new Promise((r) => setTimeout(r, 500));
        const state = await backend.getConcurrencyState(CONCURRENCY, 60_000);
        expect(state.currentConcurrency).toBe(0);
      }
    );
  }, 30_000);
});
