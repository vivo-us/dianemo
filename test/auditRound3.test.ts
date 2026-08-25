import type { DianemoBackend } from "../packages/core/src/backend/types.js";
import { memoryBackend } from "../packages/core/src/backend/memory.js";
import { redisBackend } from "../packages/backend-redis/src/index.js";
import { RequestAbortedError } from "../packages/core/src/errors.js";
import RequestHandler from "../packages/core/src/index.js";
import { afterEach, describe, expect, it } from "vitest";
import { createServer, type Server } from "node:http";
import type { Redis } from "ioredis";

const KEY = "0123456789abcdef0123456789abcdef";
const servers: Server[] = [];
const handlers: RequestHandler[] = [];

afterEach(async () => {
  await Promise.all(handlers.splice(0).map((handler) => handler.stop()));
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise<void>((resolve) => {
          server.closeAllConnections();
          server.close(() => resolve());
        })
    )
  );
});

async function upstream() {
  let hits = 0;
  const server = createServer((_req, res) => {
    hits++;
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end('{"ok":true}');
  });
  servers.push(server);
  await new Promise<void>((resolve) =>
    server.listen(0, "127.0.0.1", () => resolve())
  );
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  return { baseURL: `http://127.0.0.1:${port}`, hits: () => hits };
}

describe("audit round 3 regressions", () => {
  it("surfaces individual Redis pipeline failures from batch", async () => {
    const failure = new Error("pipeline command failed");
    const chain = {
      set: () => chain,
      del: () => chain,
      expire: () => chain,
      sadd: () => chain,
      srem: () => chain,
      hset: () => chain,
      publish: () => chain,
      exec: async () => [[failure, null]] as Array<[Error | null, unknown]>,
    };
    const fake = {
      defineCommand(name: string) {
        (this as Record<string, unknown>)[name] = async () => null;
      },
      pipeline: () => chain,
      multi: () => chain,
    } as unknown as Redis;
    const backend = redisBackend(fake);
    await expect(
      backend.batch([{ op: "set", key: "a", value: "b" }])
    ).rejects.toBe(failure);
  });

  it("refunds a fast-path token when abort lands during admission", async () => {
    const target = await upstream();
    const raw = memoryBackend();
    const controller = new AbortController();
    const backend = new Proxy(raw, {
      get(object, property, receiver) {
        const value = Reflect.get(object, property, receiver);
        if (property !== "tryAdmitImmediately" || typeof value !== "function") {
          return typeof value === "function" ? value.bind(object) : value;
        }
        return async (
          ...args: Parameters<DianemoBackend["tryAdmitImmediately"]>
        ) => {
          const admitted = await value.apply(object, args);
          controller.abort();
          return admitted;
        };
      },
    }) as DianemoBackend;
    const handler = new RequestHandler({
      key: KEY,
      backend,
      defaultClientOptions: {
        name: "default",
        rateLimit: {
          type: "requestLimit",
          interval: 60_000,
          tokensToAdd: 1,
          maxTokens: 1,
        },
        requestOptions: { defaults: { baseURL: target.baseURL } },
      },
    });
    handlers.push(handler);

    await expect(
      handler.handleRequest({
        clientName: "default",
        requestName: "abort.fast-path",
        method: "GET",
        url: "/",
        signal: controller.signal,
      })
    ).rejects.toBeInstanceOf(RequestAbortedError);
    expect(target.hits()).toBe(0);
    const state = await backend.getTokenBucketState(
      "requestHandler:default:rateLimit:default",
      { interval: 60_000, tokensToAdd: 1, maxTokens: 1 }
    );
    expect(state.tokens).toBe(1);
  });

  it("does not reject an upstream success when post-response hooks throw", async () => {
    const target = await upstream();
    const errors: unknown[] = [];
    const handler = new RequestHandler({
      key: KEY,
      backend: memoryBackend(),
      logger: {
        debug: () => {},
        info: () => {},
        warn: () => {},
        error: (value: unknown) => errors.push(value),
      },
      defaultClientOptions: {
        name: "default",
        rateLimit: { type: "noLimit" },
        requestOptions: {
          defaults: { baseURL: target.baseURL },
          responseInterceptor: () => {
            throw new Error("interceptor failed");
          },
        },
        rateLimitChange: () => {
          throw new Error("change failed");
        },
      },
    });
    handlers.push(handler);

    const response = await handler.handleRequest({
      clientName: "default",
      requestName: "hooks.success",
      method: "GET",
      url: "/",
    });
    expect(response.status).toBe(200);
    expect(target.hits()).toBe(1);
    expect(errors.length).toBeGreaterThanOrEqual(2);
  });

  it("rejects new work while shutdown is in progress", async () => {
    const target = await upstream();
    const handler = new RequestHandler({
      key: KEY,
      backend: memoryBackend(),
      defaultClientOptions: {
        name: "default",
        requestOptions: { defaults: { baseURL: target.baseURL } },
      },
    });
    handlers.push(handler);
    await handler.start();
    const stop = handler.stop();
    await expect(
      handler.handleRequest({
        clientName: "default",
        requestName: "shutdown.race",
        method: "GET",
        url: "/",
      })
    ).rejects.toMatchObject({ code: "handler_stopping" });
    await stop;
  });

  it("waits for an in-progress start before tearing the handler down", async () => {
    let enterHook!: () => void;
    let leaveHook!: () => void;
    const entered = new Promise<void>((resolve) => (enterHook = resolve));
    const gate = new Promise<void>((resolve) => (leaveHook = resolve));
    const handler = new RequestHandler({ key: KEY, backend: memoryBackend() });
    handlers.push(handler);
    handler.registerStartupHook(async () => {
      enterHook();
      await gate;
    });

    const starting = handler.start();
    await entered;
    const stopping = handler.stop();
    await expect(
      handler.handleRequest({
        clientName: "default",
        requestName: "startup.stop-race",
        method: "GET",
      })
    ).rejects.toMatchObject({ code: "handler_stopping" });
    leaveHook();
    await Promise.all([starting, stopping]);
    expect(handler.getMetadata().status).toBe("stopped");
    expect(handler.getLoadedClients()).toEqual([]);
  });

  it("preserves cancellation when returning admission fails", async () => {
    const target = await upstream();
    const raw = memoryBackend();
    const controller = new AbortController();
    const backend = new Proxy(raw, {
      get(object, property, receiver) {
        if (property === "refundTokens") {
          return async () => {
            throw new Error("refund unavailable");
          };
        }
        const value = Reflect.get(object, property, receiver);
        if (property !== "tryAdmitImmediately" || typeof value !== "function") {
          return typeof value === "function" ? value.bind(object) : value;
        }
        return async (
          ...args: Parameters<DianemoBackend["tryAdmitImmediately"]>
        ) => {
          const admitted = await value.apply(object, args);
          controller.abort();
          return admitted;
        };
      },
    }) as DianemoBackend;
    const handler = new RequestHandler({
      key: KEY,
      backend,
      defaultClientOptions: {
        name: "default",
        rateLimit: {
          type: "requestLimit",
          interval: 60_000,
          tokensToAdd: 1,
          maxTokens: 1,
        },
        requestOptions: { defaults: { baseURL: target.baseURL } },
      },
    });
    handlers.push(handler);

    await expect(
      handler.handleRequest({
        clientName: "default",
        requestName: "abort.refund-failure",
        method: "GET",
        url: "/",
        signal: controller.signal,
      })
    ).rejects.toBeInstanceOf(RequestAbortedError);
    expect(target.hits()).toBe(0);
  });

  it("keeps completion cleanup single-flight across health ticks", async () => {
    const target = await upstream();
    const raw = memoryBackend();
    let releases = 0;
    const backend = new Proxy(raw, {
      get(object, property, receiver) {
        const value = Reflect.get(object, property, receiver);
        if (property !== "releaseConcurrency" || typeof value !== "function") {
          return typeof value === "function" ? value.bind(object) : value;
        }
        return async (
          ...args: Parameters<DianemoBackend["releaseConcurrency"]>
        ) => {
          releases++;
          await new Promise((resolve) => setTimeout(resolve, 30));
          await value.apply(object, args);
        };
      },
    }) as DianemoBackend;
    const handler = new RequestHandler({
      key: KEY,
      backend,
      defaultClientOptions: {
        name: "default",
        rateLimit: { type: "concurrencyLimit", maxConcurrency: 1 },
        healthCheckIntervalMs: 5,
        requestOptions: { defaults: { baseURL: target.baseURL } },
      },
    });
    handlers.push(handler);

    await handler.handleRequest({
      clientName: "default",
      requestName: "completion.single-flight",
      method: "GET",
      url: "/",
    });
    const deadline = Date.now() + 1000;
    let state = await backend.getConcurrencyState(
      "requestHandler:default:concurrency:default",
      120_000
    );
    while (state.currentConcurrency !== 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      state = await backend.getConcurrencyState(
        "requestHandler:default:concurrency:default",
        120_000
      );
    }
    expect(releases).toBe(1);
    expect(state).toEqual({ currentConcurrency: 0, activeRequests: [] });
  });

  /**
   * The two halves of this were each covered already — `batch` reporting a failed
   * pipeline command, and shutdown refusing new work — but never together, and
   * together they wedged the handler for good: status stuck at "stopping" (which
   * `getMetadata` reports as "stopped"), `stop()` a no-op, `start()` handing back
   * the already-resolved `startPromise` without re-running `doStart`, every
   * request refused with `handler_stopping`, and the heartbeat interval still
   * holding the event loop open so the process could not exit.
   */
  it("still reaches a stopped, restartable state when deregistration fails", async () => {
    const target = await upstream();
    const backend = memoryBackend();
    const handler = new RequestHandler({
      key: KEY,
      backend,
      defaultClientOptions: {
        name: "default",
        requestOptions: { defaults: { baseURL: target.baseURL } },
      },
    });
    handlers.push(handler);
    await handler.start();
    handler.setDrainTimeout(0);

    // What a backend that goes away before its handler does now produces.
    const realBatch = backend.batch.bind(backend);
    backend.batch = async () => {
      throw new Error("Connection is closed.");
    };

    // The failure still reaches the caller — only the state is guaranteed.
    await expect(handler.stop()).rejects.toThrow("Connection is closed.");
    expect(handler.getMetadata().status).toBe("stopped");
    // Nothing left pinning the event loop open.
    expect(
      (handler as unknown as { heartbeatInterval?: NodeJS.Timeout })
        .heartbeatInterval
    ).toBeUndefined();

    // And the handler is genuinely usable again, rather than permanently
    // refusing work with `handler_stopping`.
    backend.batch = realBatch;
    await handler.start();
    expect(handler.getMetadata().status).toBe("started");
    const response = await handler.handleRequest({
      clientName: "default",
      requestName: "shutdown.recovered",
      method: "GET",
      url: "/",
    });
    expect(response.status).toBe(200);
  });

  /**
   * The retry timer is bounded so a permanently missing budget owner cannot spin
   * forever, but the counter that bounds it used to reset only on success — so
   * one exhausted burst left every later completion stored with no timer behind
   * it, waiting on a health tick that a stopped client never fires.
   */
  it("gives a later completion its own completion-forward budget", async () => {
    const backend = memoryBackend();
    const handler = new RequestHandler({
      key: KEY,
      backend,
      defaultClientOptions: {
        name: "default",
        rateLimit: { type: "concurrencyLimit", maxConcurrency: 1 },
      },
    });
    handlers.push(handler);
    await handler.start();
    // The records this test strands would otherwise hold the shutdown drain for
    // its full budget — `hasOutstandingWork` counts a controller's pending
    // completions, which is deliberate.
    handler.setDrainTimeout(0);

    interface Internals {
      getBudgetOwnerClient?: () => unknown;
      completionForwardAttempts: number;
      completionForwardTimer?: NodeJS.Timeout;
      pendingRequestDone: Map<string, unknown>;
      finalizeOwnedRequest: (data: Record<string, unknown>) => Promise<void>;
    }
    const client = (
      handler as unknown as { clients: Map<string, unknown> }
    ).clients.get("default") as unknown as Internals;

    // Stand in for a shared-limit child whose parent is momentarily absent —
    // the only state that reaches the forward retry path.
    client.getBudgetOwnerClient = () => undefined;

    const completion = (requestId: string) => ({
      requestId,
      clientName: "default",
      requestName: "forward.budget",
      status: "inProgress",
      responseStatus: "success",
      priority: 1,
      cost: 1,
      retries: 0,
      timestamp: Date.now(),
      isThawRequest: false,
    });

    await client.finalizeOwnedRequest(completion("first"));
    // Burn the budget the way an owner that never returns would.
    client.completionForwardAttempts = 50;
    expect(client.pendingRequestDone.size).toBe(1);

    await client.finalizeOwnedRequest(completion("second"));
    expect(client.pendingRequestDone.size).toBe(2);
    // The new arrival must be retried, not stranded.
    expect(client.completionForwardAttempts).toBeLessThan(50);
    expect(client.completionForwardTimer).toBeDefined();

    // Let the retry path resolve normally so teardown is not measuring the
    // stranded records this test planted on purpose.
    client.getBudgetOwnerClient = undefined;
    client.pendingRequestDone.clear();
  });
});
