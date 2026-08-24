import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { redisBackend } from "../packages/backend-redis/src/index.js";
import RequestHandler from "../packages/core/src/index.js";
import { Redis } from "ioredis";
import type {
  BatchOp,
  DianemoBackend,
} from "../packages/core/src/backend/types.js";

/**
 * Exercises start() and stop() against a real Redis. Everything here depends on
 * Lua scripts, pub/sub, and MULTI semantics that a mock does not reproduce
 * faithfully enough to be worth testing against.
 *
 * Skipped unless REDIS_URL is set. CI provides a service container; locally,
 * `docker run -d -p 6399:6379 redis:7-alpine` and
 * `REDIS_URL=redis://localhost:6399 npm test`.
 */
const REDIS_URL = process.env.REDIS_URL;
const describeIfRedis = REDIS_URL ? describe : describe.skip;

/**
 * A dedicated logical database keeps flushdb from touching anything else.
 *
 * Genuinely dedicated, which db9 was not: `auditRound2` reaches it through
 * `harnesses(9)`, and this suite asserts that EVERY key in the DB carries the
 * configured prefix. Anything a neighbour leaves behind fails that assertion —
 * including the ownership marker `claimLogicalDb` writes per DB. It passed only
 * because the flush here is `afterEach` and the assertion is not the first test,
 * which is not a property anyone would preserve on purpose.
 *
 * db1 specifically: db0 is kept clear, and db2 belongs to the backend parity
 * matrix.
 */
const TEST_DB = 1;

describeIfRedis("handler lifecycle against a real Redis", () => {
  let redis: Redis;

  beforeAll(async () => {
    redis = new Redis(`${REDIS_URL}/${TEST_DB}`);
    await redis.ping();
  });

  afterEach(async () => {
    await redis.flushdb();
  });

  afterAll(async () => {
    await redis.quit();
  });

  const newHandler = (prefix: string) =>
    new RequestHandler({
      key: "0123456789abcdef0123456789abcdef",
      backend: redisBackend(redis),
      keyPrefix: prefix,
    });

  it("starts with no environment variables configured", async () => {
    // The point of injecting Redis and the logger: no REDIS_URL, SERVICE_NAME,
    // or LOG_LEVEL is read from the environment during start().
    const saved = { ...process.env };
    delete process.env.SERVICE_NAME;
    delete process.env.LOG_LEVEL;

    const handler = newHandler("nostate");
    try {
      await handler.start();
      expect(handler.getMetadata().status).toBe("started");
    } finally {
      await handler.stop();
      process.env = saved;
    }
  });

  it("registers its instance and default client in Redis", async () => {
    const handler = newHandler("reg");
    try {
      await handler.start();

      const instances = await redis.smembers("reg:requestHandler:instances");
      expect(instances).toContain(handler.getInstanceId());
      expect(handler.getLoadedClients().map((c) => c.name)).toContain(
        "default"
      );
    } finally {
      await handler.stop();
    }
  });

  it("builds a client from a template once credentials arrive", async () => {
    const handler = newHandler("tmpl");
    try {
      await handler.registerClientTemplate("probe" as never, (creds) => [
        {
          name: `probe:_:${(creds as { instanceId: string }).instanceId}`,
          rateLimit: {
            type: "requestLimit",
            interval: 1000,
            tokensToAdd: 5,
            maxTokens: 5,
          },
        },
      ]);
      await handler.start();
      await handler.addTemplateClient(
        "probe" as never,
        {
          instanceId: "live",
        } as never
      );

      expect(handler.getRegisteredTemplates()).toContain("probe");
      expect(handler.getLoadedClients().map((c) => c.name)).toContain(
        "probe:_:live"
      );
      expect(handler.getTemplateClientNames("probe", "live")).toEqual([
        "probe:_:live",
      ]);
    } finally {
      await handler.stop();
    }
  });

  it("stores template credentials encrypted, not in plaintext", async () => {
    const handler = newHandler("enc");
    try {
      await handler.registerClientTemplate("probe" as never, (creds) => [
        {
          name: `probe:_:${(creds as { instanceId: string }).instanceId}`,
          rateLimit: { type: "noLimit" },
        },
      ]);
      await handler.start();
      await handler.addTemplateClient(
        "probe" as never,
        {
          instanceId: "live",
          clientSecret: "super-secret-value",
        } as never
      );

      const stored = await redis.get("enc:requestHandler:template:probe::live");
      expect(stored).toBeTruthy();
      expect(stored).not.toContain("super-secret-value");
    } finally {
      await handler.stop();
    }
  });

  it("namespaces every key by the configured prefix", async () => {
    const handler = newHandler("scoped");
    try {
      await handler.start();
      const keys = await redis.keys("*");
      expect(keys.length).toBeGreaterThan(0);
      expect(keys.every((k) => k.startsWith("scoped:"))).toBe(true);
    } finally {
      await handler.stop();
    }
  });

  it("leaves the caller's connection open after stop()", async () => {
    // The handler closes only the duplicate it created for pub/sub. Quitting
    // the injected connection would break unrelated work in the same process.
    const handler = newHandler("own");
    await handler.start();
    await handler.stop();

    await expect(redis.ping()).resolves.toBe("PONG");
    expect(redis.status).toBe("ready");
  });

  it("releases the event loop so a process can exit after stop()", async () => {
    // A library that keeps the loop alive silently hangs every CLI, script and
    // test suite that uses it. Assert on the handles the handler owns rather
    // than on process exit, so the check works inside the runner.
    const own = new Redis(`${REDIS_URL}/${TEST_DB}`);
    const handler = new RequestHandler({
      key: "0123456789abcdef0123456789abcdef",
      backend: redisBackend(own),
      keyPrefix: "loop",
    });
    await handler.start();

    const before = process
      .getActiveResourcesInfo()
      .filter((r) => r === "Timeout").length;
    await handler.stop();
    const after = process
      .getActiveResourcesInfo()
      .filter((r) => r === "Timeout").length;

    // Heartbeat and per-client health-check timers must all be cleared.
    expect(after).toBeLessThanOrEqual(before);
    // And the caller's own connection is still theirs to close.
    await expect(own.ping()).resolves.toBe("PONG");
    await own.quit();
  });

  it("arms no heartbeat when an election resolves after stop()", async () => {
    // A peer's departure arrives on pub/sub and elects here, and an election
    // writes its registration before arming the heartbeat. Hold that write
    // until stop() has cleared the interval, and the election resumes into a
    // handler with none — arming one nothing will ever clear, so the handler
    // heartbeats for the life of the process and the loop never empties.
    //
    // The gate makes that interleaving the test rather than a race it hopes to
    // hit: two handlers stopping together reach it only sometimes.
    let release!: () => void;
    const gated = new Promise<void>((resolve) => {
      release = resolve;
    });
    let gateArmed = false;
    let gateHit = false;

    const own = new Redis(`${REDIS_URL}/${TEST_DB}`);
    const inner = redisBackend(own);
    const backend = new Proxy(inner, {
      get(target, prop, receiver) {
        if (prop !== "batch") return Reflect.get(target, prop, receiver);
        return async (ops: BatchOp[], options?: unknown) => {
          const registers = ops.some(
            (op) => op.op === "set" && op.key.includes(":instance:")
          );
          if (registers && gateArmed) {
            gateHit = true;
            await gated;
          }
          return (target.batch as DianemoBackend["batch"]).call(
            target,
            ops,
            options as never
          );
        };
      },
    });

    const handler = new RequestHandler({
      key: "0123456789abcdef0123456789abcdef",
      backend,
      keyPrefix: "election",
    });
    await handler.start();

    gateArmed = true;
    const election = handler.scheduleClientRoles(true);
    // Let it reach the gated write before shutdown clears the interval.
    await new Promise((resolve) => setTimeout(resolve, 50));

    await handler.stop();
    release();
    await election;

    expect(gateHit).toBe(true);
    const heartbeat = (handler as unknown as { heartbeatInterval?: unknown })
      .heartbeatInterval;
    expect(heartbeat).toBeUndefined();

    await own.quit();
  });

  it("deregisters its instance on stop()", async () => {
    const handler = newHandler("dereg");
    await handler.start();
    const id = handler.getInstanceId();
    await handler.stop();

    const instances = await redis.smembers("dereg:requestHandler:instances");
    expect(instances).not.toContain(id);
  });

  it("treats start() and stop() as idempotent", async () => {
    const handler = newHandler("idem");
    try {
      await Promise.all([handler.start(), handler.start()]);
      expect(handler.getMetadata().status).toBe("started");

      const instances = await redis.smembers("idem:requestHandler:instances");
      expect(
        instances.filter((i) => i === handler.getInstanceId())
      ).toHaveLength(1);
    } finally {
      await handler.stop();
      await handler.stop();
    }
  });

  it("surfaces a misconfigured client name as ClientNotFoundError", async () => {
    const handler = newHandler("missing");
    try {
      await handler.start();
      await expect(
        handler.handleRequest({
          clientName: "nope:_:nope",
          requestName: "test.missing",
          method: "GET",
          url: "/",
        })
      ).rejects.toMatchObject({ code: "client_not_found", statusCode: 404 });
    } finally {
      await handler.stop();
    }
  }, 15_000);

  it("installs subscriptions on a start() that follows a failed one", async () => {
    // Redis unreachable at boot is ordinary startup ordering. The failure this
    // guards left `subscriptionsStarted` set, so the retry skipped subscribing and
    // reached "started" deaf: every cross-replica requestReady missed, forever,
    // with nothing logged. Single-replica traffic still worked, which is why the
    // rest of the suite passed over it.
    const unreachable = new Redis("redis://127.0.0.1:1", {
      maxRetriesPerRequest: 1,
      retryStrategy: () => null,
      lazyConnect: true,
    });
    unreachable.on("error", () => {});
    const handler = new RequestHandler({
      key: "0123456789abcdef0123456789abcdef",
      backend: redisBackend(unreachable),
      keyPrefix: "deaf",
    });
    await expect(handler.start()).rejects.toThrow();
    expect(handler.subscriptionsStarted).toBe(false);
    unreachable.disconnect();

    const working = new Redis(`${REDIS_URL}/${TEST_DB}`);
    const retried = new RequestHandler({
      key: "0123456789abcdef0123456789abcdef",
      backend: redisBackend(working),
      keyPrefix: "deaf",
    });
    try {
      await retried.start();
      const subscribed = await redis.pubsub("CHANNELS", "deaf:*");
      expect(subscribed.length).toBeGreaterThan(0);
    } finally {
      await retried.stop();
      await working.quit();
    }
  }, 20_000);

  it("closes the pub/sub connection when start() fails", async () => {
    // `stop()` returns early while the status is "stopped", so the duplicate the
    // backend had already created was never quit — it kept reconnecting and kept
    // the event loop alive after the caller had given up.
    const unreachable = new Redis("redis://127.0.0.1:1", {
      maxRetriesPerRequest: 1,
      retryStrategy: () => null,
      lazyConnect: true,
    });
    unreachable.on("error", () => {});
    const backend = redisBackend(unreachable);
    const handler = new RequestHandler({
      key: "0123456789abcdef0123456789abcdef",
      backend,
      keyPrefix: "leak",
    });
    await expect(handler.start()).rejects.toThrow();
    // Nothing left for a later close to quit.
    await expect(backend.close()).resolves.toBeUndefined();
    unreachable.disconnect();
  }, 20_000);
});
