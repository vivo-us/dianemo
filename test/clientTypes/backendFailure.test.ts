import type { DianemoBackend } from "../../packages/core/src/backend/types.js";
import { fire as fireOn, startUpstream, withReplicas } from "./harness.js";
import { redisBackend } from "../../packages/backend-redis/src/index.js";
import { describe, expect, it } from "vitest";
import { createServer } from "node:http";
import type { AxiosError } from "axios";
import crypto from "node:crypto";
import { Redis } from "ioredis";
import net from "node:net";
import RequestHandler, {
  RequestTimeoutError,
} from "../../packages/core/src/index.js";

/**
 * What the library does when the BACKEND misbehaves.
 *
 * Every other suite assumes Redis answers. These tests take that away — script
 * cache flushed, connection killed, server unresponsive, a key holding the
 * wrong type — and pin down the two things that decide whether this is safe to
 * run in front of a real vendor:
 *
 *   1. admission fails CLOSED. A rate limiter that lets a request out without
 *      spending budget is worse than one that refuses it, so every test here
 *      that breaks the backend also asserts what the upstream saw.
 *   2. the client recovers on its own once Redis comes back, rather than
 *      staying wedged until someone restarts the process.
 *
 * Redis only, on logical DB 3 — the memory backend cannot fail this way.
 *
 * DB 3 rather than 4: `noLimit.test.ts` already claims 4 via `harnesses(4)`, and
 * `withRig` flushes its DB on every rig. `fileParallelism: false` means the two
 * files never overlap in one invocation, so nothing was actually corrupted, but
 * sharing a DB with another file is one variable this file does not need.
 * 0-3 are unclaimed; 4-15 are all taken by some suite.
 */

const REDIS_URL = process.env.REDIS_URL;
const HAS_REDIS = Boolean(REDIS_URL);
const DB = 3;
const KEY = "0123456789abcdef0123456789abcdef";
const RUN = crypto.randomBytes(3).toString("hex");

/**
 * Retry options that make an outage decide itself in milliseconds.
 *
 * ioredis defaults — `maxRetriesPerRequest: 20` with a backoff that grows to
 * 2s — mean a command issued during an outage waits somewhere between 11s and
 * 40s before it is rejected, which is real behaviour but not testable
 * behaviour. Squeezing the same state machine into ~100ms changes when the
 * failure surfaces, not which failure surfaces.
 */
const FAST_FAIL = { maxRetriesPerRequest: 1, retryStrategy: () => 50 };

/** `cleanupTimeout` where a test asserts against the admission budget itself. */
const ADMISSION_MS = 1000;

/**
 * `ABANDONMENT_PUBLISH_TIMEOUT_MS` in
 * packages/core/src/client/methods/handleRequest.ts, mirrored because a caller
 * failing admission during an outage waits for both budgets in sequence and the
 * bound asserted below is their sum. If that constant changes, this one has to.
 */
const ABANDONMENT_PUBLISH_BUDGET_MS = 2000;

// ---------------------------------------------------------------- TCP proxy

/**
 * A TCP proxy in front of Redis, so a test can take the backend away without
 * touching the server the rest of the run shares.
 *
 * The three ways a backend goes away are not interchangeable, and ioredis
 * treats them differently enough that the library's behaviour differs too:
 *
 * - `blackhole()` — socket open, no bytes delivered. A saturated or paused
 *   Redis, or one running `DEBUG SLEEP`. ioredis queues rather than fails.
 * - `drop()` — connections accepted and immediately destroyed. A failover, a
 *   proxy with no backend, a node at `maxclients`. ioredis reconnects
 *   successfully every time, so its retry counter keeps resetting.
 * - `refuse()` — nothing listening at all: ECONNREFUSED. A crashed Redis.
 *   Only here does ioredis's retry counter climb to its limit.
 *
 * All three are reversible, which is the only way recovery is testable.
 */
interface Proxy {
  url: string;
  blackhole: () => void;
  drop: () => void;
  refuse: () => Promise<void>;
  heal: () => Promise<void>;
  close: () => Promise<void>;
}

async function startProxy(target: string): Promise<Proxy> {
  const parsed = new URL(target);
  const host = parsed.hostname;
  const port = Number(parsed.port || 6379);

  let mode: "forward" | "blackhole" | "drop" = "forward";
  const links = new Set<{ a: net.Socket; b: net.Socket; flush: () => void }>();

  const server = net.createServer((client) => {
    if (mode === "drop") {
      client.destroy();
      return;
    }
    const upstream = net.connect(port, host);
    // Held rather than dropped: dropping bytes mid-RESP would corrupt the
    // stream on restore, and then "did not recover" would mean the fixture.
    const held: Array<[net.Socket, Buffer]> = [];
    const pipe = (from: net.Socket, to: net.Socket) => {
      from.on("data", (chunk: Buffer) => {
        if (mode === "blackhole") held.push([to, chunk]);
        else to.write(chunk);
      });
      from.on("error", () => to.destroy());
      from.on("close", () => to.destroy());
    };
    pipe(client, upstream);
    pipe(upstream, client);
    const link = {
      a: client,
      b: upstream,
      flush: () => {
        for (const [to, chunk] of held.splice(0)) to.write(chunk);
      },
    };
    links.add(link);
    client.on("close", () => links.delete(link));
  });

  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const address = server.address();
  const localPort = typeof address === "object" && address ? address.port : 0;
  let listening = true;

  const destroyAll = () => {
    for (const link of links) {
      link.a.destroy();
      link.b.destroy();
    }
    links.clear();
  };

  return {
    url: `redis://127.0.0.1:${localPort}`,
    blackhole: () => {
      mode = "blackhole";
    },
    drop: () => {
      mode = "drop";
      destroyAll();
    },
    refuse: async () => {
      mode = "drop";
      destroyAll();
      if (!listening) return;
      listening = false;
      await new Promise<void>((r) => server.close(() => r()));
    },
    heal: async () => {
      mode = "forward";
      if (!listening) {
        listening = true;
        await new Promise<void>((r) =>
          server.listen(localPort, "127.0.0.1", () => r())
        );
      }
      for (const link of links) link.flush();
    },
    close: async () => {
      mode = "drop";
      destroyAll();
      if (!listening) return;
      listening = false;
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

// ------------------------------------------------------------------- the rig

interface Spec {
  name: string;
  rateLimit: Record<string, unknown>;
  requestOptions?: Record<string, unknown>;
  retryOptions?: Record<string, unknown>;
  healthCheckIntervalMs?: number;
}

interface Rig {
  handler: RequestHandler;
  backend: DianemoBackend;
  /** A direct connection that bypasses the proxy — for SCRIPT FLUSH and inspection. */
  admin: Redis;
  clientName: (spec: string) => string;
  /** Key namespace for a spec, so a test can read the queue and slot ledger. */
  namespaceOf: (spec: string) => string;
}

/**
 * One handler whose connection to Redis the test controls.
 *
 * `withHandler` builds its connection from REDIS_URL internally, so it cannot
 * be pointed at a proxy or given non-default retry options — which is the whole
 * subject here. Everything else matches it: one flush up front, one template
 * per spec, one instance.
 */
async function withRig(
  options: {
    baseURL: string;
    clients: Spec[];
    keyPrefix: string;
    /** Defaults to REDIS_URL. A proxy url is what makes the backend failable. */
    url?: string;
    redisOptions?: Record<string, unknown>;
  },
  fn: (rig: Rig) => Promise<void>
): Promise<void> {
  const keyPrefix = `${options.keyPrefix}-${RUN}`;
  const admin = new Redis(`${REDIS_URL}/${DB}`);
  await admin.flushdb();

  const redis = new Redis(`${options.url ?? REDIS_URL}/${DB}`, {
    ...options.redisOptions,
  });
  const backend = redisBackend(redis);
  const handler = new RequestHandler({ key: KEY, backend, keyPrefix });
  // A shutdown that waits ten seconds on a backend the test has just taken
  // away turns every teardown into a timeout.
  handler.setDrainTimeout(500);

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
          ...(spec.healthCheckIntervalMs
            ? { healthCheckIntervalMs: spec.healthCheckIntervalMs }
            : {}),
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
  const namespaceOf = (spec: string) =>
    `${keyPrefix}:requestHandler:${clientName(spec)}`;

  try {
    await fn({ handler, backend, admin, clientName, namespaceOf });
  } finally {
    await withTimeout(handler.stop(), 4000).catch(() => {});
    await withTimeout(backend.close(), 2000).catch(() => {});
    redis.disconnect();
    admin.disconnect();
  }
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T | void> {
  let timer: NodeJS.Timeout;
  return Promise.race([
    promise.finally(() => clearTimeout(timer)),
    new Promise<void>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`timed out after ${ms}ms`)),
        ms
      );
    }),
  ]);
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * An upstream that fails, so a test can ask what a caller sees when the vendor
 * and the backend fail together. `startUpstream` always answers 200.
 */
async function startFailingUpstream(status: number, delayMs: number) {
  let served = 0;
  let inFlight = 0;
  const server = createServer(async (_req, res) => {
    served++;
    inFlight++;
    if (delayMs > 0) await sleep(delayMs);
    inFlight--;
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end('{"error":true}');
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  return {
    baseURL: `http://127.0.0.1:${port}`,
    servedCount: () => served,
    inFlight: () => inFlight,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

/**
 * Watches a promise without consuming its rejection, so a test can assert that
 * a request is STILL PENDING — which is the whole question when the backend is
 * unresponsive rather than broken.
 */
function track<T>(promise: Promise<T>) {
  const state = {
    settled: false,
    value: undefined as T | undefined,
    error: undefined as unknown,
  };
  promise.then(
    (value) => {
      state.settled = true;
      state.value = value;
    },
    (error: unknown) => {
      state.settled = true;
      state.error = error;
    }
  );
  return state;
}

/**
 * Waits until the upstream is actually holding a request open.
 *
 * The tests that break the backend "after the send, before the completion
 * publish" used a fixed sleep for this, which is a race: on a loaded machine the
 * sleep overshoots the upstream's delay, the publish goes out over a healthy
 * connection, and the test then measures nothing. Whether the request has
 * reached the upstream is observable, so observe it.
 */
async function waitForInFlight(
  upstream: { inFlight: () => number },
  timeoutMs = 5000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (upstream.inFlight() === 0) {
    if (Date.now() > deadline) {
      throw new Error("upstream never received the request");
    }
    await sleep(10);
  }
}

/** Waits until a request has actually taken its place in the queue. */
async function waitForQueued(
  admin: Redis,
  queueKey: string,
  timeoutMs = 5000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while ((await admin.zcard(queueKey)) === 0) {
    if (Date.now() > deadline) {
      throw new Error(`nothing was queued at ${queueKey}`);
    }
    await sleep(10);
  }
}

/** Waits for a tracked request to settle, so a test never sleeps longer than it must. */
async function settle(
  state: { settled: boolean },
  timeoutMs = 20_000
): Promise<number> {
  const started = Date.now();
  while (!state.settled && Date.now() - started < timeoutMs) await sleep(50);
  return Date.now() - started;
}

const fire = (handler: RequestHandler, clientName: string) =>
  handler.handleRequest({
    clientName,
    requestName: "t.noop",
    method: "GET",
    url: "/",
  });

const outcome = (promise: Promise<unknown>) =>
  promise.then(
    () => "served" as const,
    () => "rejected" as const
  );

// -------------------------------------------------------------------- tests

describe.skipIf(!HAS_REDIS)("backend failure: script cache", () => {
  /**
   * Scripts are invoked by SHA. A Redis restart or an operator's `SCRIPT FLUSH`
   * empties the cache, and every EVALSHA in flight comes back NOSCRIPT — which
   * on the hot path is every request. Nothing in this repo handles NOSCRIPT, so
   * this is really a test of the claim in `defineCommands`
   * (packages/backend-redis/src/context.ts) that ioredis reloads the script
   * for us.
   */
  it("reloads its Lua scripts after a SCRIPT FLUSH mid-traffic", async () => {
    const upstream = await startUpstream(0);
    try {
      await withRig(
        {
          baseURL: upstream.baseURL,
          keyPrefix: "bf-noscript",
          clients: [
            {
              name: "bfns",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 200,
                tokensToAdd: 200,
                interval: 1000,
              },
            },
          ],
        },
        async ({ handler, admin, clientName }) => {
          const name = clientName("bfns");
          const inFlight: Array<Promise<unknown>> = [];
          for (let i = 0; i < 40; i++) {
            inFlight.push(fire(handler, name));
            // Mid-traffic, not between requests: the flush has to land while
            // scripts are actually being invoked for NOSCRIPT to happen at all.
            if (i % 8 === 0) await admin.script("FLUSH");
            await sleep(5);
          }
          const settled = await Promise.allSettled(inFlight);
          expect(
            settled
              .filter((s) => s.status === "rejected")
              .map((s) => String((s as PromiseRejectedResult).reason))
          ).toEqual([]);
          expect(upstream.servedCount()).toBe(40);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  /**
   * The script cache is per-server but the "have I loaded this?" bookkeeping is
   * per-connection, so a flush has to be survived by every replica's own
   * connection independently. One replica recovering proves nothing about four.
   */
  it("reloads scripts on every replica's connection after a SCRIPT FLUSH", async () => {
    const upstream = await startUpstream(0);
    const admin = new Redis(`${REDIS_URL}/${DB}`);
    try {
      await withReplicas(
        {
          count: 3,
          baseURL: upstream.baseURL,
          clients: [
            {
              name: "bfnsr",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 100,
                tokensToAdd: 100,
                interval: 1000,
              },
            },
          ],
          redisDb: DB,
          keyPrefix: "bf-noscript-replicas",
        },
        async ({ replicas }) => {
          const name = "bfnsr:_:a";
          // Every replica has now used its connection at least once, so each
          // one believes the script is cached — which is what the flush breaks.
          for (const replica of replicas) {
            expect((await fireOn(replica, name)).status).toBe(200);
          }
          await admin.script("FLUSH");
          upstream.resetCounters();

          const results = await Promise.all(
            replicas.map((replica) => fireOn(replica, name))
          );
          expect(results.map((r) => r.status)).toEqual([200, 200, 200]);
          expect(upstream.servedCount()).toBe(3);
        }
      );
    } finally {
      admin.disconnect();
      await upstream.close();
    }
  }, 60_000);
});

describe.skipIf(!HAS_REDIS)("backend failure: unreachable Redis", () => {
  /**
   * The fail-open test. An unresponsive Redis is the case ioredis answers by
   * queueing the command rather than failing it, so admission neither succeeds
   * nor errors — and the question is whether the request goes out anyway.
   *
   * It must not, and it does not. The cost of that safety is recorded here too:
   * the wait is unbounded. Nothing in `handleRequest` puts a deadline on
   * admission itself — `cleanupTimeout` only covers a request that reached the
   * queue — so a caller with no `signal` and no axios timeout waits as long as
   * Redis stays quiet.
   */
  it("sends nothing while Redis is unresponsive, and sends it once Redis answers", async () => {
    const upstream = await startUpstream(0);
    const proxy = await startProxy(REDIS_URL!);
    try {
      await withRig(
        {
          baseURL: upstream.baseURL,
          keyPrefix: "bf-hang",
          url: proxy.url,
          clients: [
            {
              name: "bfh",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 50,
                tokensToAdd: 50,
                interval: 1000,
              },
            },
          ],
        },
        async ({ handler, clientName }) => {
          const name = clientName("bfh");
          // The rig works before it is broken, or "sent nothing" proves nothing.
          expect((await fire(handler, name)).status).toBe(200);
          upstream.resetCounters();

          proxy.blackhole();
          const pending = track(fire(handler, name));
          await sleep(2000);
          // The two halves of fail-closed: the request did not go out, and the
          // caller was not told it had.
          expect(upstream.servedCount()).toBe(0);
          expect(pending.settled).toBe(false);

          await proxy.heal();
          await settle(pending, 5000);
          expect(pending.error).toBeUndefined();
          expect(upstream.servedCount()).toBe(1);
        }
      );
    } finally {
      await proxy.heal();
      await proxy.close();
      await upstream.close();
    }
  }, 60_000);

  /**
   * A crashed Redis, rather than a slow one: with nothing listening ioredis
   * eventually gives up, and the rejection has to reach the caller rather than
   * releasing the request.
   *
   * How long "eventually" is belongs to the connection, not to dianemo. On
   * ioredis defaults it is a first wave at roughly 11s and, for a command that
   * arrives just after one wave, up to ~40s for the next — because the queue is
   * flushed only on every 21st reconnect attempt and the backoff is capped at
   * 2s. Nothing in the library shortens that; see the `drop()` test below for
   * the case where it never happens at all.
   */
  it("rejects a request while Redis refuses connections, then serves again once it returns", async () => {
    const upstream = await startUpstream(0);
    const proxy = await startProxy(REDIS_URL!);
    try {
      await withRig(
        {
          baseURL: upstream.baseURL,
          keyPrefix: "bf-down",
          url: proxy.url,
          redisOptions: FAST_FAIL,
          clients: [
            {
              name: "bfd",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 50,
                tokensToAdd: 50,
                interval: 1000,
              },
              retryOptions: { maxRetries: 0 },
            },
          ],
        },
        async ({ handler, clientName }) => {
          const name = clientName("bfd");
          expect((await fire(handler, name)).status).toBe(200);
          upstream.resetCounters();

          await proxy.refuse();
          const during = track(fire(handler, name));
          await settle(during, 20_000);
          expect(during.error).toBeDefined();
          expect(upstream.servedCount()).toBe(0);

          await proxy.heal();
          // Recovery has to be automatic: no restart, no re-registration. A
          // client that stayed wedged would fail this line.
          const deadline = Date.now() + 10_000;
          let recovered = false;
          while (!recovered && Date.now() < deadline) {
            recovered = (await outcome(fire(handler, name))) === "served";
            if (!recovered) await sleep(250);
          }
          expect(recovered).toBe(true);
          expect(upstream.servedCount()).toBeGreaterThan(0);
        }
      );
    } finally {
      await proxy.heal();
      await proxy.close();
      await upstream.close();
    }
  }, 60_000);

  /**
   * Admission has its own deadline, so a Redis that accepts connections and
   * drops them cannot hold a caller open indefinitely.
   *
   * This mode is the one where the connection layer never rescues anyone.
   * `tryAdmitImmediately` is the first backend call every request makes, and
   * each reconnect here SUCCEEDS, which resets ioredis's `retryAttempts` to 0 —
   * so the count never reaches the multiple of `maxRetriesPerRequest + 1` at
   * which queued commands are rejected, and the command waits forever. That is
   * what a failover with a listener but no backend looks like, or a node at
   * `maxclients`. FAST_FAIL is deliberately in force: even a connection
   * configured to give up after one retry never does.
   *
   * The deadline in `waitForRequestReady`
   * (packages/core/src/client/methods/handleRequest.ts:259) is therefore the
   * only thing that ends this wait, which is why the timing bound is asserted
   * and not just the error type. Fail-closed is unaffected: the upstream sees
   * nothing either way.
   */
  it("gives up on admission at its own deadline while Redis accepts and drops connections", async () => {
    const upstream = await startUpstream(0);
    const proxy = await startProxy(REDIS_URL!);
    try {
      await withRig(
        {
          baseURL: upstream.baseURL,
          keyPrefix: "bf-flap",
          url: proxy.url,
          redisOptions: FAST_FAIL,
          clients: [
            {
              name: "bff",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 50,
                tokensToAdd: 50,
                interval: 1000,
              },
              requestOptions: { cleanupTimeout: ADMISSION_MS },
              retryOptions: { maxRetries: 0 },
            },
          ],
        },
        async ({ handler, clientName }) => {
          const name = clientName("bff");
          expect((await fire(handler, name)).status).toBe(200);
          upstream.resetCounters();

          proxy.drop();
          const started = Date.now();
          const pending = track(fire(handler, name));
          // Generous: eight times the budget. What is being tested is that it
          // settles at all in a mode where the connection layer never gives up.
          await settle(pending, ADMISSION_MS * 8);
          const elapsed = Date.now() - started;

          // A backend rejection may beat the deadline to it: a command already
          // written to a socket is rejected when that socket dies, and whether
          // this one was written or was still queued is a race with the drop.
          // Either answer is correct — what must hold is that SOMETHING ends the
          // wait promptly, which before the deadline existed nothing did.
          expect(pending.error).toBeInstanceOf(Error);
          if (pending.error instanceof RequestTimeoutError) {
            // If it was the deadline, it was the configured budget and not some
            // other timer further down the line.
            expect(elapsed).toBeGreaterThanOrEqual(ADMISSION_MS - 100);
          }
          // The bound that matters: bounded by the budget, not by ioredis's
          // retry schedule, which measured 43s on a default connection.
          expect(elapsed).toBeLessThan(ADMISSION_MS * 5);
          expect(upstream.servedCount()).toBe(0);

          // And it still recovers once Redis is back.
          await proxy.heal();
          const deadline = Date.now() + 10_000;
          let recovered = false;
          while (!recovered && Date.now() < deadline) {
            recovered = (await outcome(fire(handler, name))) === "served";
            if (!recovered) await sleep(250);
          }
          expect(recovered).toBe(true);
        }
      );
    } finally {
      await proxy.heal();
      await proxy.close();
      await upstream.close();
    }
  }, 60_000);

  /**
   * The admission deadline reaches the caller even when the announcement of its
   * own abandonment cannot be delivered.
   *
   * This is the harder half of the two outage shapes. `withDeadline` ends the
   * admission wait on time either way, but that rejection then passes through
   * `releaseAbandonedRequest` → `publishAbandoned`
   * (packages/core/src/client/methods/handleRequest.ts:180), which publishes on
   * the same dead connection. When that publish was awaited without a bound, an
   * unresponsive Redis held the caller's answer indefinitely — the deadline
   * fired and could not escape. It now carries its own
   * `ABANDONMENT_PUBLISH_TIMEOUT_MS` (:28), so the announcement is best-effort
   * and cannot outlive the decision to give up.
   *
   * The `drop()` test above does not cover this: there a command already written
   * to a dying socket is rejected promptly, so the publish fails fast on its
   * own. Only a connection that stays up and answers nothing leaves it hanging,
   * and that is the more common outage — a saturated Redis, a swapping node,
   * `DEBUG SLEEP`, a paused container.
   *
   * The bound asserted is therefore the SUM of the two budgets, because the
   * caller pays both in sequence: admission gives up at `cleanupTimeout`, then
   * the announcement spends up to its own 2s. That total is what a caller can
   * count on, and it is bounded and knowable, which is the whole point.
   *
   * Fail-closed is unaffected throughout: the upstream sees nothing either way.
   */
  it("gives up at its own deadline even when the abandonment publish cannot be delivered", async () => {
    const upstream = await startUpstream(0);
    const proxy = await startProxy(REDIS_URL!);
    try {
      await withRig(
        {
          baseURL: upstream.baseURL,
          keyPrefix: "bf-abandon",
          url: proxy.url,
          redisOptions: FAST_FAIL,
          clients: [
            {
              name: "bfab",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 50,
                tokensToAdd: 50,
                interval: 1000,
              },
              requestOptions: { cleanupTimeout: ADMISSION_MS },
              retryOptions: { maxRetries: 0 },
            },
          ],
        },
        async ({ handler, clientName }) => {
          const name = clientName("bfab");
          expect((await fire(handler, name)).status).toBe(200);
          upstream.resetCounters();

          proxy.blackhole();
          const started = Date.now();
          const pending = track(fire(handler, name));

          // Well past both budgets, so a regression to the old unbounded wait
          // shows up as a failure here rather than as a suite timeout.
          await settle(pending, ADMISSION_MS * 12);
          const settledAt = Date.now() - started;

          expect(pending.error).toBeInstanceOf(RequestTimeoutError);
          // Not before the admission budget — the deadline is what ends this,
          // not something faster upstream of it.
          expect(settledAt).toBeGreaterThanOrEqual(ADMISSION_MS - 100);
          // And not after both budgets have been spent in sequence. Redis never
          // answers in this window, so the announcement always spends its full
          // allowance; measured ~3s for a 1s admission budget plus the 2s
          // announcement budget. The slack covers scheduling, not another timer.
          expect(settledAt).toBeLessThan(
            ADMISSION_MS + ABANDONMENT_PUBLISH_BUDGET_MS + 1500
          );
          expect(upstream.servedCount()).toBe(0);

          // Still no wedge: ordinary traffic resumes once Redis answers.
          await proxy.heal();
          const deadline = Date.now() + 10_000;
          let recovered = false;
          while (!recovered && Date.now() < deadline) {
            recovered = (await outcome(fire(handler, name))) === "served";
            if (!recovered) await sleep(250);
          }
          expect(recovered).toBe(true);
        }
      );
    } finally {
      await proxy.heal();
      await proxy.close();
      await upstream.close();
    }
  }, 60_000);

  /**
   * Connection churn rather than a clean outage: the connection dies and comes
   * back repeatedly while requests are being admitted, which is what a Redis
   * failover or a restarting node actually looks like.
   *
   * The budget is three tokens with a refill a minute away, so the upstream may
   * see at most three requests no matter how the connection behaves. A backend
   * error read as "allowed" would show up here as a fourth.
   *
   * The churn is done by cycling the proxy rather than with `CLIENT KILL TYPE
   * normal`, which this test used to do. That command is server-wide: on a Redis
   * shared with anything else — another suite, another agent's run — it kills
   * connections that have nothing to do with this test, and the reconnect storm
   * lands in someone else's measurements. Cycling the proxy churns exactly the
   * connections this rig owns.
   */
  it("never exceeds the budget while the connection is repeatedly cut", async () => {
    const upstream = await startUpstream(0);
    const proxy = await startProxy(REDIS_URL!);
    try {
      await withRig(
        {
          baseURL: upstream.baseURL,
          keyPrefix: "bf-churn",
          url: proxy.url,
          redisOptions: FAST_FAIL,
          clients: [
            {
              name: "bfch",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 3,
                tokensToAdd: 3,
                interval: 60_000,
              },
              requestOptions: { cleanupTimeout: 1500 },
              retryOptions: { maxRetries: 0 },
              healthCheckIntervalMs: 500,
            },
          ],
        },
        async ({ handler, clientName }) => {
          const name = clientName("bfch");
          const inFlight = Array.from({ length: 12 }, () =>
            outcome(fire(handler, name))
          );
          for (let i = 0; i < 5; i++) {
            await sleep(100);
            proxy.drop();
            await sleep(60);
            await proxy.heal();
          }
          const results = await Promise.all(inFlight);
          const served = results.filter((r) => r === "served").length;

          expect(upstream.servedCount()).toBeLessThanOrEqual(3);
          expect(upstream.servedCount()).toBe(served);
          // A budget that admitted nothing at all would pass the line above
          // while proving the opposite of what this file is for.
          expect(served).toBeGreaterThan(0);
        }
      );
    } finally {
      await proxy.heal();
      await proxy.close();
      await upstream.close();
    }
  }, 60_000);

  /**
   * Nothing here may take the host process down. Several paths swallow backend
   * errors deliberately for exactly this reason — heartbeats, health ticks,
   * post-freeze timers, pub/sub dispatch — and this is the test that says so
   * while all of them are failing at once.
   */
  it("raises no unhandled rejection while the backend is unreachable", async () => {
    const upstream = await startUpstream(0);
    const proxy = await startProxy(REDIS_URL!);
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);
    try {
      await withRig(
        {
          baseURL: upstream.baseURL,
          keyPrefix: "bf-unhandled",
          url: proxy.url,
          redisOptions: FAST_FAIL,
          clients: [
            {
              name: "bfu",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 1,
                tokensToAdd: 1,
                interval: 60_000,
              },
              requestOptions: { cleanupTimeout: 1000 },
              retryOptions: { maxRetries: 0 },
              // Fast enough that several ticks — role reconciliation, orphan
              // cleanup, a drain pass — land inside the outage.
              healthCheckIntervalMs: 300,
            },
          ],
        },
        async ({ handler, admin, clientName, namespaceOf }) => {
          const name = clientName("bfu");
          expect((await fire(handler, name)).status).toBe(200);

          const queued = track(fire(handler, name));
          await waitForQueued(admin, `${namespaceOf("bfu")}:queue`);
          await proxy.refuse();
          await settle(queued, 8000);
          // Long enough for several health ticks and heartbeats to fail.
          await sleep(2000);
          await proxy.heal();
          await sleep(1000);
          expect(unhandled.map(String)).toEqual([]);
        }
      );
      await sleep(200);
      expect(unhandled.map(String)).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
      await proxy.heal();
      await proxy.close();
      await upstream.close();
    }
  }, 60_000);
});

describe.skipIf(!HAS_REDIS)("backend failure: errors from a script", () => {
  /**
   * A key holding the wrong type is what a namespace collision looks like — two
   * services sharing a Redis, one of them using `<prefix>:...:queue` as a
   * string. The Lua error has to reach the caller rather than releasing the
   * request, and fixing the key has to be enough to recover.
   */
  it("surfaces a WRONGTYPE without sending the request, and recovers when the key is fixed", async () => {
    const upstream = await startUpstream(0);
    try {
      await withRig(
        {
          baseURL: upstream.baseURL,
          keyPrefix: "bf-wrongtype",
          clients: [
            {
              name: "bfw",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 50,
                tokensToAdd: 50,
                interval: 1000,
              },
              retryOptions: { maxRetries: 0 },
            },
          ],
        },
        async ({ handler, admin, clientName, namespaceOf }) => {
          const name = clientName("bfw");
          const queueKey = `${namespaceOf("bfw")}:queue`;
          await admin.set(queueKey, "not-a-zset");

          await expect(fire(handler, name)).rejects.toThrow(/WRONGTYPE/);
          expect(upstream.servedCount()).toBe(0);

          await admin.del(queueKey);
          expect((await fire(handler, name)).status).toBe(200);
          // The failed attempt claimed nothing it did not hand back: one token
          // spent for one request served.
          const stats = await handler.getClientStats(name);
          expect(stats.rateLimit).toMatchObject({ tokens: 49 });
          expect(await admin.zcard(queueKey)).toBe(0);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);
});

describe.skipIf(!HAS_REDIS)("backend failure: partial lifecycles", () => {
  /**
   * The completion publish is bookkeeping, and bookkeeping must not change what
   * the caller is told happened.
   *
   * `handleResponse` announces completion through `publishRequestDone`
   * (packages/core/src/client/methods/handleRequest.ts:374), which retries and
   * then gives up with a log rather than propagating. Before that, a connection
   * dying between the send and the publish converted a 200 into a rejection —
   * and a caller that cannot classify an error retries it, which for a
   * non-idempotent request is a duplicate operation against the vendor.
   *
   * The queue entry and any capacity the request held are still lost in this
   * window; that is the unfixed half, pinned by the two tests below.
   */
  it("returns the response when the completion publish cannot be delivered", async () => {
    const upstream = await startUpstream(400);
    const proxy = await startProxy(REDIS_URL!);
    try {
      await withRig(
        {
          baseURL: upstream.baseURL,
          keyPrefix: "bf-published",
          url: proxy.url,
          redisOptions: FAST_FAIL,
          clients: [
            {
              name: "bfp",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 50,
                tokensToAdd: 50,
                interval: 1000,
              },
              retryOptions: { maxRetries: 0 },
            },
          ],
        },
        async ({ handler, clientName }) => {
          const name = clientName("bfp");
          const inFlight = track(fire(handler, name));
          // Admission is done and the upstream is holding the request open, so
          // the only backend call left is the completion publish.
          await waitForInFlight(upstream);
          await proxy.refuse();
          await settle(inFlight, 15_000);

          // The side effect happened exactly once, and the caller was told so.
          expect(upstream.servedCount()).toBe(1);
          expect(inFlight.error).toBeUndefined();
          expect((inFlight.value as { status: number }).status).toBe(200);

          await proxy.heal();
          const deadline = Date.now() + 10_000;
          let recovered = false;
          while (!recovered && Date.now() < deadline) {
            recovered = (await outcome(fire(handler, name))) === "served";
            if (!recovered) await sleep(250);
          }
          expect(recovered).toBe(true);
        }
      );
    } finally {
      await proxy.heal();
      await proxy.close();
      await upstream.close();
    }
  }, 60_000);

  /**
   * The other half of the same fix: an upstream failure must survive a backend
   * failure intact.
   *
   * `handleError` publishes the completion before rethrowing the AxiosError, so
   * a publish that threw replaced the vendor's status with an ioredis error —
   * and status-based handling in consumer code (`if (err.response.status ===
   * 503) …`) silently stopped working for the duration of the blip. That is the
   * more insidious direction of the bug, because nothing looks broken: the
   * caller sees an error either way, just not the right one.
   */
  it("keeps the vendor's status when the completion publish cannot be delivered", async () => {
    const upstream = await startFailingUpstream(503, 400);
    const proxy = await startProxy(REDIS_URL!);
    try {
      await withRig(
        {
          baseURL: upstream.baseURL,
          keyPrefix: "bf-published-error",
          url: proxy.url,
          redisOptions: FAST_FAIL,
          clients: [
            {
              name: "bfpe",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 50,
                tokensToAdd: 50,
                interval: 1000,
              },
              // No retries, so the 503 is final and reaches the caller.
              retryOptions: { maxRetries: 0 },
            },
          ],
        },
        async ({ handler, clientName }) => {
          const name = clientName("bfpe");
          const inFlight = track(fire(handler, name));
          await waitForInFlight(upstream);
          await proxy.refuse();
          await settle(inFlight, 15_000);

          expect(upstream.servedCount()).toBe(1);
          expect(inFlight.error).toBeDefined();
          // The vendor's answer, not the backend's.
          expect((inFlight.error as AxiosError).response?.status).toBe(503);
        }
      );
    } finally {
      await proxy.heal();
      await proxy.close();
      await upstream.close();
    }
  }, 60_000);

  /** Owner-side cleanup survives an outage that loses the pub/sub announcement. */
  it("retries abandonment cleanup after the backend recovers", async () => {
    const upstream = await startUpstream(0);
    const proxy = await startProxy(REDIS_URL!);
    try {
      await withRig(
        {
          baseURL: upstream.baseURL,
          keyPrefix: "bf-strand",
          url: proxy.url,
          redisOptions: FAST_FAIL,
          clients: [
            {
              name: "bfst",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 1,
                tokensToAdd: 1,
                interval: 500,
              },
              requestOptions: { cleanupTimeout: 1000 },
              retryOptions: { maxRetries: 0 },
              healthCheckIntervalMs: 500,
            },
          ],
        },
        async ({ handler, admin, clientName, namespaceOf }) => {
          const name = clientName("bfst");
          const queueKey = `${namespaceOf("bfst")}:queue`;
          // Spend the only token so the next request has to queue.
          expect((await fire(handler, name)).status).toBe(200);

          const queued = track(fire(handler, name));
          await sleep(200);
          expect(await admin.zcard(queueKey)).toBe(1);

          await proxy.refuse();
          await settle(queued, 8000);
          expect(queued.error).toBeInstanceOf(Error);
          // Held past the point where ioredis gives up on the queued publish,
          // so the announcement is genuinely lost rather than merely delayed.
          await sleep(1000);
          await proxy.heal();

          // Several health ticks — role reconciliation, orphan cleanup and a
          // drain pass — with Redis fully healthy again.
          await sleep(2500);
          const cleanupDeadline = Date.now() + 5000;
          while (
            (await admin.zcard(queueKey)) !== 0 &&
            Date.now() < cleanupDeadline
          ) {
            await sleep(100);
          }
          expect(await admin.zcard(queueKey)).toBe(0);

          // New traffic is served and the queue returns to its fast-path state.
          expect((await fire(handler, name)).status).toBe(200);
          expect(await admin.zcard(queueKey)).toBe(0);
        }
      );
    } finally {
      await proxy.heal();
      await proxy.close();
      await upstream.close();
    }
  }, 60_000);

  /** A lost announcement does not prevent owner-side slot cleanup after recovery. */
  it("retries concurrency-slot cleanup after the backend recovers", async () => {
    const upstream = await startUpstream(300);
    const proxy = await startProxy(REDIS_URL!);
    const slotTtl = 2500;
    try {
      await withRig(
        {
          baseURL: upstream.baseURL,
          keyPrefix: "bf-slot",
          url: proxy.url,
          redisOptions: FAST_FAIL,
          clients: [
            {
              name: "bfsl",
              rateLimit: { type: "concurrencyLimit", maxConcurrency: 1 },
              requestOptions: {
                cleanupTimeout: 1000,
                concurrencySlotTtl: slotTtl,
              },
              retryOptions: { maxRetries: 0 },
              healthCheckIntervalMs: 500,
            },
          ],
        },
        async ({ handler, backend, clientName, namespaceOf }) => {
          const name = clientName("bfsl");
          const slotKey = `${namespaceOf("bfsl")}:concurrency`;

          const inFlight = track(fire(handler, name));
          await waitForInFlight(upstream);
          await proxy.refuse();
          await settle(inFlight, 15_000);
          // The upstream success remains visible while cleanup waits for Redis.
          expect(upstream.servedCount()).toBe(1);
          expect(inFlight.error).toBeUndefined();

          await proxy.heal();
          const cleanupDeadline = Date.now() + 5000;
          let state = await backend.getConcurrencyState(slotKey, slotTtl);
          while (
            state.currentConcurrency !== 0 &&
            Date.now() < cleanupDeadline
          ) {
            await sleep(100);
            state = await backend.getConcurrencyState(slotKey, slotTtl);
          }
          expect(state.currentConcurrency).toBe(0);
          expect(upstream.inFlight()).toBe(0);

          upstream.resetCounters();
          expect(await outcome(fire(handler, name))).toBe("served");
          expect(upstream.servedCount()).toBe(1);
        }
      );
    } finally {
      await proxy.heal();
      await proxy.close();
      await upstream.close();
    }
  }, 90_000);
});
