import type { DianemoBackend } from "../packages/core/src/backend/types.js";
import { memoryBackend } from "../packages/core/src/backend/memory.js";
import { redisBackend } from "../packages/backend-redis/src/index.js";
import { describe, expect, it } from "vitest";
import { createServer } from "node:http";
import crypto from "node:crypto";
import { Redis } from "ioredis";
import RequestHandler, {
  RequestTimeoutError,
} from "../packages/core/src/index.js";

/**
 * What the core client does when a request gives up between admission and
 * dispatch, and what it hands the backend when configuration is not quite the
 * shape the backend stores.
 *
 * Every test here forces the queued path — the fast path admits and dispatches
 * in one step, so it cannot show the gap between the two.
 *
 * Redis logical DB 3, SHARED with `clientTypes/backendFailure.test.ts`. Both
 * flush it, so `fileParallelism: false` in `vitest.config.ts` is what keeps them
 * from wiping each other's queue entries mid-test; it is load-bearing here, not
 * a performance preference.
 */

const REDIS_URL = process.env.REDIS_URL;
const DB = 3;
const KEY = "0123456789abcdef0123456789abcdef";
const RUN = crypto.randomBytes(3).toString("hex");

// ------------------------------------------------------------------ upstream

/** An upstream whose status code the test controls. */
async function startUpstream() {
  let status = 200;
  let served = 0;
  const server = createServer((_req, res) => {
    served++;
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end('{"ok":true}');
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  return {
    baseURL: `http://127.0.0.1:${port}`,
    setStatus: (value: number) => {
      status = value;
    },
    servedCount: () => served,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

// ------------------------------------------------------------------- backend

/**
 * A backend with some methods replaced.
 *
 * A `Proxy` rather than a spread: both backends are class instances, so a copy
 * loses every prototype method, and the overrides have to sit in front of an
 * object that still answers the other forty.
 */
function overrideBackend(
  backend: DianemoBackend,
  overrides: Partial<Record<keyof DianemoBackend, unknown>>
): DianemoBackend {
  return new Proxy(backend, {
    get(target, property, _receiver) {
      if (property in overrides) {
        return overrides[property as keyof DianemoBackend];
      }
      const value = Reflect.get(target, property, target) as unknown;
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

interface BackendCase {
  name: string;
  create: () => Promise<{
    backend: DianemoBackend;
    cleanup: () => Promise<void>;
  }>;
}

function backendCases(): BackendCase[] {
  const cases: BackendCase[] = [
    {
      name: "memory",
      create: async () => {
        const backend = memoryBackend();
        return { backend, cleanup: () => backend.close() };
      },
    },
  ];
  if (REDIS_URL) {
    cases.push({
      name: "redis",
      create: async () => {
        const redis = new Redis(`${REDIS_URL}/${DB}`);
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
  return cases;
}

// --------------------------------------------------------------------- rig

interface Spec {
  name: string;
  rateLimit: Record<string, unknown>;
  requestOptions?: Record<string, unknown>;
  retryOptions?: Record<string, unknown>;
}

interface Rig {
  handler: RequestHandler;
  /** The registered name of a spec, which is what `handleRequest` is given. */
  clientName: (spec: string) => string;
  /** The live client object, for a test that has to reach past the template. */
  clientFor: (spec: string) => {
    requestOptions: Record<string, unknown>;
  };
}

/**
 * Builds a handler over `backend`, runs `fn`, and tears it down.
 *
 * Not `clientTypes/harness.ts`: its `withHandler` fixes `requestOptions` to a
 * base URL and passes no `retryOptions`, and two tests here turn on exactly
 * those fields.
 */
async function withRig(
  options: {
    backend: DianemoBackend;
    baseURL: string;
    keyPrefix: string;
    clients: Spec[];
  },
  fn: (rig: Rig) => Promise<void>
): Promise<void> {
  const handler = new RequestHandler({
    key: KEY,
    backend: options.backend,
    keyPrefix: `${options.keyPrefix}-${RUN}`,
  });
  handler.setDrainTimeout(1000);

  for (const spec of options.clients) {
    await handler.registerClientTemplate(
      spec.name as never,
      ((creds: { instanceId: string }) => [
        {
          name: `${spec.name}:_:${creds.instanceId}`,
          rateLimit: spec.rateLimit,
          requestOptions: {
            defaults: { baseURL: options.baseURL },
            ...spec.requestOptions,
          },
          ...(spec.retryOptions ? { retryOptions: spec.retryOptions } : {}),
        },
      ]) as never
    );
  }
  await handler.start();
  for (const spec of options.clients) {
    await handler.addTemplateClient(
      spec.name as never,
      {
        instanceId: "a",
      } as never
    );
  }

  const clientName = (spec: string) => `${spec}:_:a`;
  const clients = (
    handler as unknown as {
      clients: Map<string, { requestOptions: Record<string, unknown> }>;
    }
  ).clients;

  try {
    await fn({
      handler,
      clientName,
      clientFor: (spec) => {
        const client = clients.get(clientName(spec));
        if (!client) throw new Error(`no client registered for ${spec}`);
        return client;
      },
    });
  } finally {
    await handler.stop().catch(() => undefined);
  }
}

const fire = (handler: RequestHandler, clientName: string) =>
  handler.handleRequest({
    clientName,
    requestName: "t.noop",
    method: "GET",
    url: "/",
  } as never);

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitFor(
  predicate: () => boolean,
  timeoutMs: number,
  what: string
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(10);
  }
  throw new Error(`timed out after ${timeoutMs}ms waiting for ${what}`);
}

// --------------------------------------------------------------------- tests

for (const backendCase of backendCases()) {
  describe(`core recovery paths — '${backendCase.name}'`, () => {
    /**
     * `thawRequestCount` is a probe budget, and both backends store it as a
     * whole number. A fraction reaching the backend is one configuration
     * meaning a different number of probes per backend.
     */
    it("hands the backend a whole probe budget for a fractional thawRequestCount", async () => {
      const upstream = await startUpstream();
      upstream.setStatus(429);
      const { backend, cleanup } = await backendCase.create();
      const armed: number[] = [];
      try {
        await withRig(
          {
            backend: overrideBackend(backend, {
              armFreeze: (key: string, frozenUntil: number, thaw: number) => {
                armed.push(thaw);
                return backend.armFreeze(key, frozenUntil, thaw);
              },
            }),
            baseURL: upstream.baseURL,
            keyPrefix: "cr-thaw",
            clients: [
              {
                name: "crth",
                rateLimit: { type: "noLimit" },
                retryOptions: {
                  thawRequestCount: 2.5,
                  maxRetries: 0,
                  retryBackoffBaseTime: 50,
                },
              },
            ],
          },
          async ({ handler, clientName }) => {
            await expect(fire(handler, clientName("crth"))).rejects.toThrow();
            await waitFor(() => armed.length > 0, 5000, "the freeze to arm");
            expect(armed[0]).toBe(2);
          }
        );
      } finally {
        await cleanup();
        await upstream.close();
      }
    }, 30000);

    /**
     * The enqueue is the one backend call on the queued path that nothing else
     * bounds: `waitForRequestReady` cannot return its own timer's rejection
     * while it is still awaiting the add.
     */
    it("fails a queued request at its admission budget when the enqueue never settles", async () => {
      const upstream = await startUpstream();
      const { backend, cleanup } = await backendCase.create();
      try {
        await withRig(
          {
            backend: overrideBackend(backend, {
              tryAdmitNoLimit: async () => false,
              addRequest: () => new Promise(() => {}),
            }),
            baseURL: upstream.baseURL,
            keyPrefix: "cr-add",
            clients: [
              {
                name: "crad",
                rateLimit: { type: "noLimit" },
                requestOptions: { cleanupTimeout: 600 },
                retryOptions: { maxRetries: 0 },
              },
            ],
          },
          async ({ handler, clientName }) => {
            const started = Date.now();
            await expect(
              fire(handler, clientName("crad"))
            ).rejects.toBeInstanceOf(RequestTimeoutError);
            expect(Date.now() - started).toBeLessThan(4000);
            expect(upstream.servedCount()).toBe(0);
          }
        );
      } finally {
        await cleanup();
        await upstream.close();
      }
    }, 30000);

    /**
     * A throw in `handlePreRequest` lands after admission has already spent, and
     * the announcement it triggers releases a concurrency slot but has nothing
     * to give back to a token bucket. The refill is a minute away, so a token
     * that is not returned is a request that cannot be served.
     */
    it("returns a spent token when a pre-request step throws after admission", async () => {
      const upstream = await startUpstream();
      const { backend, cleanup } = await backendCase.create();
      let explode = true;
      try {
        await withRig(
          {
            backend: overrideBackend(backend, {
              tryAdmitImmediately: async () => false,
            }),
            baseURL: upstream.baseURL,
            keyPrefix: "cr-tok",
            clients: [
              {
                name: "crtk",
                rateLimit: {
                  type: "requestLimit",
                  maxTokens: 1,
                  tokensToAdd: 1,
                  interval: 60_000,
                },
                requestOptions: { cleanupTimeout: 2000 },
                retryOptions: { maxRetries: 0 },
              },
            ],
          },
          async ({ handler, clientName, clientFor }) => {
            const client = clientFor("crtk");
            client.requestOptions = {
              ...client.requestOptions,
              requestInterceptor: (config: unknown) => {
                if (explode) throw new Error("credential store unavailable");
                return config;
              },
            };

            await expect(fire(handler, clientName("crtk"))).rejects.toThrow(
              /credential store unavailable/
            );

            explode = false;
            await fire(handler, clientName("crtk"));
            expect(upstream.servedCount()).toBe(1);
          }
        );
      } finally {
        await cleanup();
        await upstream.close();
      }
    }, 30000);

    /** The concurrency half of the pair above, which already held. */
    it("returns a claimed slot when a pre-request step throws after admission", async () => {
      const upstream = await startUpstream();
      const { backend, cleanup } = await backendCase.create();
      let explode = true;
      try {
        await withRig(
          {
            backend: overrideBackend(backend, {
              tryAdmitConcurrency: async () => false,
            }),
            baseURL: upstream.baseURL,
            keyPrefix: "cr-slot",
            clients: [
              {
                name: "crsl",
                rateLimit: { type: "concurrencyLimit", maxConcurrency: 1 },
                requestOptions: {
                  cleanupTimeout: 2000,
                  concurrencySlotTtl: 120_000,
                },
                retryOptions: { maxRetries: 0 },
              },
            ],
          },
          async ({ handler, clientName, clientFor }) => {
            const client = clientFor("crsl");
            client.requestOptions = {
              ...client.requestOptions,
              requestInterceptor: (config: unknown) => {
                if (explode) throw new Error("credential store unavailable");
                return config;
              },
            };

            await expect(fire(handler, clientName("crsl"))).rejects.toThrow(
              /credential store unavailable/
            );

            explode = false;
            await fire(handler, clientName("crsl"));
            expect(upstream.servedCount()).toBe(1);
          }
        );
      } finally {
        await cleanup();
        await upstream.close();
      }
    }, 30000);
  });
}
