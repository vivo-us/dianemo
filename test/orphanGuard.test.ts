import { memoryBackend } from "../packages/core/src/backend/memory.js";
import { redisBackend } from "../packages/backend-redis/src/index.js";
import { harnesses, withReplicas } from "./clientTypes/harness.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import RequestHandler from "../packages/core/src/index.js";
import { createServer } from "node:http";
import { Redis } from "ioredis";
import type {
  DianemoBackend,
  QueuedRequest,
} from "../packages/core/src/backend/types.js";

/**
 * Two guards that keep a healthy fleet's queued work alive.
 *
 * The first is orphan cleanup's self-id check. The alive-instance set is the
 * only evidence a sweep has, and a set that has been lost — evicted under an
 * `allkeys-*` policy, flushed by a neighbour, or not yet re-registered after a
 * recovery — is indistinguishable from a fleet that died. Every instance
 * re-adds its own id on each heartbeat, so a read missing the sweeper's own id
 * is proof the read is not authoritative.
 *
 * The second is the removal tombstone's lifetime, which has to cover the
 * caller's admission budget rather than a fixed minute.
 *
 * Redis logical DB 8 is SHARED with `regressions.test.ts`. Both flush it, so
 * `fileParallelism: false` in `vitest.config.ts` is what keeps that safe rather
 * than luck.
 */

const REDIS_URL = process.env.REDIS_URL;
const REDIS_DB = 8;
const KEY = "0123456789abcdef0123456789abcdef";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const QUEUE = "og:queue";
const PREFIX = "og:request";

const queued = (requestId: string, ownerId: string): QueuedRequest => ({
  requestId,
  clientName: "c",
  requestName: "r",
  status: "pending",
  priority: 5,
  cost: 1,
  retries: 0,
  timestamp: Date.now(),
  isThawRequest: false,
  ownerId,
});

async function startUpstream(seen: string[]) {
  const server = createServer((request, response) => {
    seen.push(request.url ?? "");
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end('{"ok":true}');
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const address = server.address();
  return {
    baseURL: `http://127.0.0.1:${
      typeof address === "object" && address ? address.port : 0
    }`,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

// ------------------------------------------------ the guard, on both backends

describe.each(harnesses(REDIS_DB))("orphan sweep guard — $name", (harness) => {
  let backend: DianemoBackend;
  let cleanup: () => Promise<void>;

  beforeAll(async () => {
    ({ backend, cleanup } = await harness.create());
  });
  afterAll(async () => {
    await cleanup();
  });

  it("refuses to sweep when the alive set does not contain the sweeper", async () => {
    await backend.addRequest(QUEUE, PREFIX, queued("lost-set", "worker-1"));

    expect(
      await backend.cleanupOrphanedRequests(QUEUE, PREFIX, new Set(), "sweeper")
    ).toBe(0);
    expect(
      await backend.cleanupOrphanedRequests(
        QUEUE,
        PREFIX,
        new Set(["someone-else"]),
        "sweeper"
      )
    ).toBe(0);
    expect(await backend.getRequest(PREFIX, "lost-set")).not.toBeNull();

    // The same call, once the sweeper can see itself, does reap it.
    expect(
      await backend.cleanupOrphanedRequests(
        QUEUE,
        PREFIX,
        new Set(["sweeper"]),
        "sweeper"
      )
    ).toBe(1);
    expect(await backend.getRequest(PREFIX, "lost-set")).toBeNull();
  });

  it("refuses to sweep a queue entry whose metadata expired under it", async () => {
    await backend.addRequest(QUEUE, PREFIX, queued("stranded", "worker-1"));
    await backend.del(`${PREFIX}:stranded`);

    expect(
      await backend.cleanupOrphanedRequests(QUEUE, PREFIX, new Set(), "sweeper")
    ).toBe(0);
    expect(await backend.getQueueLength(QUEUE)).toBe(1);

    expect(
      await backend.cleanupOrphanedRequests(
        QUEUE,
        PREFIX,
        new Set(["sweeper"]),
        "sweeper"
      )
    ).toBe(1);
    expect(await backend.getQueueLength(QUEUE)).toBe(0);
  });

  it("spares an owner that is alive alongside the sweeper", async () => {
    await backend.addRequest(QUEUE, PREFIX, queued("mine", "worker-1"));
    await backend.addRequest(QUEUE, PREFIX, queued("theirs", "worker-2"));

    expect(
      await backend.cleanupOrphanedRequests(
        QUEUE,
        PREFIX,
        new Set(["sweeper", "worker-1"]),
        "sweeper"
      )
    ).toBe(1);
    expect(await backend.getRequest(PREFIX, "mine")).not.toBeNull();
    expect(await backend.getRequest(PREFIX, "theirs")).toBeNull();
  });
});

// ----------------------------------------------------- the Lua argument paths

describe.skipIf(!REDIS_URL)("orphan sweep guard — Lua arguments", () => {
  let redis: Redis;
  let backend: DianemoBackend;

  beforeAll(async () => {
    redis = new Redis(`${REDIS_URL}/${REDIS_DB}`);
    await redis.flushdb();
    backend = redisBackend(redis);
  });
  afterAll(async () => {
    await backend.close();
    await redis.quit();
  });

  /** The script unwrapped, so arguments the ops layer never sends can be sent. */
  const script = (aliveIdsJson: string, currentInstanceId: string) =>
    (
      redis as unknown as Record<string, (...a: unknown[]) => Promise<number>>
    ).dianemoCleanupOrphanedRequests(
      QUEUE,
      PREFIX,
      aliveIdsJson,
      currentInstanceId
    );

  it("cleans nothing when the alive-id argument does not decode", async () => {
    await backend.addRequest(QUEUE, PREFIX, queued("undecodable", "worker-1"));

    expect(await script("{not json", "sweeper")).toBe(0);
    expect(await script("", "sweeper")).toBe(0);
    expect(await script('"a string"', "sweeper")).toBe(0);
    expect(await backend.getRequest(PREFIX, "undecodable")).not.toBeNull();
  });

  it("cleans nothing when the sweeper sends no instance id of its own", async () => {
    expect(await script('["worker-1"]', "")).toBe(0);
    expect(await backend.getQueueLength(QUEUE)).toBe(1);
  });
});

// -------------------------------------------- the fleet-level loss it prevents

describe.skipIf(!REDIS_URL)("a lost alive set", () => {
  /**
   * Every instance re-adds its own id once a second, so a sweep fired a few
   * milliseconds after the set is deleted would otherwise be judged against
   * whichever memberships happened to have been restored by then. Stopping the
   * heartbeats is what makes the measurement the one this test names.
   */
  const stopHeartbeat = (handler: RequestHandler) =>
    clearInterval(
      (handler as unknown as { heartbeatInterval?: NodeJS.Timeout })
        .heartbeatInterval
    );

  it("keeps a live replica's queued request when the instances key is gone", async () => {
    const seen: string[] = [];
    const upstream = await startUpstream(seen);
    const raw = new Redis(`${REDIS_URL}/${REDIS_DB}`);

    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              // One token, refilling after the sweep below: the entry's
              // survival is measured while the budget is still empty, and the
              // caller is then served on the refill.
              name: "og",
              rateLimit: [
                {
                  type: "requestLimit",
                  maxTokens: 1,
                  tokensToAdd: 1,
                  interval: 4000,
                },
              ],
            },
          ],
          redisDb: REDIS_DB,
          keyPrefix: "og",
        },
        async ({ replicas, controllerFor, workersFor }) => {
          const client = "og:_:a";
          const controller = controllerFor(client);
          expect(controller).toBeDefined();
          if (!controller) return;
          const worker = workersFor(client)[0];
          const namespace = controller.getNamespace();
          const instancesKey = `${namespace}:instances`;
          const queueKey = `${namespace}:${client}:queue`;

          // Spends the only token, so the worker's request has to queue.
          const first = await controller.handleRequest({
            clientName: client,
            requestName: "t.noop",
            method: "GET",
            url: "/first",
          });
          expect(first.status).toBe(200);

          let failure: unknown;
          const second = worker
            .handleRequest({
              clientName: client,
              requestName: "t.noop",
              method: "GET",
              url: "/second",
            })
            .catch((error: unknown) => {
              failure = error;
              return undefined;
            });

          const deadline = Date.now() + 10_000;
          while ((await raw.zcard(queueKey)) !== 1 && Date.now() < deadline) {
            await sleep(25);
          }
          expect(await raw.zcard(queueKey)).toBe(1);
          expect(await raw.smembers(instancesKey)).toHaveLength(2);

          for (const replica of replicas) stopHeartbeat(replica);
          await raw.del(instancesKey);

          // What `handleInstanceStopped` runs on any departure, and what the
          // health tick runs on its own every 10 s.
          await raw.publish(`${namespace}:instanceStopped`, "og-test-nudge");
          await sleep(1000);

          // The owner is alive; only the evidence was lost.
          expect(await raw.zcard(queueKey)).toBe(1);

          await second;
          expect(failure).toBeUndefined();
          expect(seen).toContain("/second");
        }
      );
    } finally {
      await raw.quit();
      await upstream.close();
    }
  }, 60_000);
});

// ----------------------------------------------------------- tombstone sizing

describe.each(harnesses(REDIS_DB))("removal tombstone — $name", (harness) => {
  let backend: DianemoBackend;
  let cleanup: () => Promise<void>;

  beforeAll(async () => {
    ({ backend, cleanup } = await harness.create());
  });
  afterAll(async () => {
    await cleanup();
  });

  it("keeps the mark for the window the caller asked for, not the default", async () => {
    await backend.removeRequest(QUEUE, PREFIX, "short", 1);
    await backend.removeRequest(QUEUE, PREFIX, "defaulted");

    expect(await backend.addRequest(QUEUE, PREFIX, queued("short", "o"))).toBe(
      false
    );

    await sleep(1300);

    expect(await backend.addRequest(QUEUE, PREFIX, queued("short", "o"))).toBe(
      true
    );
    expect(
      await backend.addRequest(QUEUE, PREFIX, queued("defaulted", "o"))
    ).toBe(false);
  });
});

describe("tombstone sizing at the client", () => {
  it("sizes the mark from cleanupTimeout, floored at the default", async () => {
    const upstream = await startUpstream([]);
    const ttls: (number | undefined)[] = [];
    const backend = memoryBackend();
    const removeRequest = backend.removeRequest.bind(backend);
    backend.removeRequest = async (queueKey, prefix, requestId, ttlSeconds) => {
      ttls.push(ttlSeconds);
      return removeRequest(queueKey, prefix, requestId, ttlSeconds);
    };

    const handler = new RequestHandler({ key: KEY, backend, keyPrefix: "ogc" });
    await handler.registerClientTemplate(
      "t" as never,
      ((creds: { instanceId: string }) => [
        {
          name: `t:_:${creds.instanceId}`,
          rateLimit: [
            {
              type: "requestLimit",
              maxTokens: 1,
              tokensToAdd: 1,
              interval: 300,
            },
          ],
          requestOptions: {
            cleanupTimeout: 300_000,
            defaults: { baseURL: upstream.baseURL },
          },
        },
      ]) as never
    );
    await handler.start();
    await handler.addTemplateClient("t" as never, { instanceId: "a" } as never);

    try {
      // The second exhausts the budget and takes the queued path, so its
      // completion is what removes an entry.
      const results = await Promise.all(
        Array.from({ length: 2 }, () =>
          handler.handleRequest({
            clientName: "t:_:a",
            requestName: "t.noop",
            method: "GET",
            url: "/",
          })
        )
      );
      expect(results.every((r) => r.status === 200)).toBe(true);
      expect(ttls.length).toBeGreaterThan(0);
      expect(new Set(ttls)).toEqual(new Set([300]));
    } finally {
      await handler.stop();
      await backend.close();
      await upstream.close();
    }
  }, 30_000);
});
