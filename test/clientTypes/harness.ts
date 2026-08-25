import type { DianemoBackend } from "../../packages/core/src/backend/types.js";
import { memoryBackend } from "../../packages/core/src/backend/memory.js";
import { redisBackend } from "../../packages/backend-redis/src/index.js";
import RequestHandler from "../../packages/core/src/index.js";
import { createServer, type Server } from "node:http";
import crypto from "node:crypto";
import { Redis } from "ioredis";

/**
 * Shared rig for the per-client-type suites.
 *
 * Each client type gets its own file because they promise different things:
 * a token bucket promises a rate, a concurrency limit promises an occupancy,
 * `sharedLimit` promises that two credentials draw on one budget. Testing them
 * together would mean testing none of them properly.
 *
 * Every suite runs on both backends. The memory backend always runs; Redis runs
 * when REDIS_URL is set.
 */

const REDIS_URL = process.env.REDIS_URL;
const KEY = "0123456789abcdef0123456789abcdef";

/**
 * Unique per vitest invocation, so two runs against one Redis cannot collide.
 *
 * Namespacing keys per invocation keeps two runs from reading each other's
 * state. It does NOT protect against the flush: `create()` and `withReplicas`
 * both call `flushdb()`, which deletes every key in the logical DB whatever its
 * prefix — so two concurrent invocations on one DB remain mutually destructive
 * with exactly the symptom `harnesses` warns about. The prefix would only close
 * that if the flush were narrowed to it, or dropped because the prefix makes it
 * unnecessary — and narrowing it alone would not be enough, because every prefix
 * outside `withReplicas` is a literal (`"rls"`, `"ct"`, `"r2wedge"`), so two
 * invocations of one file would still share them. Until both are done:
 * one invocation per Redis instance, with `claimLogicalDb` below to say so when
 * it is not.
 */
export const RUN_ID = crypto.randomBytes(4).toString("hex");

/** A key prefix nothing else in this or any concurrent run will touch. */
export function uniquePrefix(base: string): string {
  return `${base}-${RUN_ID}`;
}

/**
 * Which vitest INVOCATION this process belongs to.
 *
 * Deliberately not `RUN_ID`: that is per-process, and vitest forks a process per
 * test file, so the files of ONE invocation each hold a different `RUN_ID` and
 * would look to the guard below like a crowd of rival runs. `ppid` is the vitest
 * main process — shared by every file it forks, different for every invocation.
 * The fallback keeps the guard degraded-but-working rather than throwing if a
 * future pool configuration runs files in-process.
 */
const INVOCATION_ID = process.ppid ? `pid${process.ppid}` : `run${RUN_ID}`;

/** Where the current owner of a logical DB is recorded. */
const ownerKey = (redisDb: number) => `dianemo:test:owner:db${redisDb}`;

/**
 * Long enough to be garbage collection rather than a deadline.
 *
 * Nothing about the check depends on this number: a lapsed marker reads as "no
 * owner", which is benign for the reasons below. It exists only so that an
 * abandoned DB does not keep a marker for ever. Sizing it as a freshness window
 * instead — short enough to expire between runs, long enough to survive the gap
 * between two `create()` calls — cannot be done safely: a single test may take
 * 60s, so any TTL short enough to clear reliably between back-to-back runs is
 * also short enough to lapse mid-run, and the check must not depend on that
 * race.
 */
const OWNER_TTL_SECONDS = 900;

/**
 * Logical DBs this process has already stamped.
 *
 * The reason the FIRST use of a DB never reports a conflict. A marker left by a
 * previous run is not evidence of a live rival — nothing deletes it at the end of
 * a run — so treating it as one would turn a rare hang into a permanent failure
 * for the next invocation, which is strictly worse. First use overwrites; only a
 * marker that changes UNDER us is a signal.
 */
const claimedDbs = new Set<number>();

/**
 * Flushes a logical DB, having first checked that nobody else has claimed it.
 *
 * BEST EFFORT, and it has to be read that way. It converts the common case of the
 * hazard described on `harnesses` below into an immediate error naming the DB; it
 * cannot promise to catch every case. Two invocations flush each other's marker,
 * so which one notices depends on where the flushes fall relative to the checks,
 * and a collision that begins after this file's last `create()` is never seen at
 * all. The value is that the FIRST symptom becomes a sentence rather than a
 * 30-second hang in an unrelated test.
 *
 * Only a FOREIGN owner is an error. A MISSING owner is not, for two reasons that
 * would both fire every run otherwise. The TTL above can lapse. And ten suites
 * flush a logical DB directly rather than coming through here — the map on
 * `harnesses` below marks each of them `direct` — every one but
 * `lifecycle.integration` sharing its DB with a suite that does claim, so their
 * flushes delete the marker without writing one. A genuine second invocation
 * rewrites the marker on every `create()`, which is why the foreign-owner case
 * is the one that actually catches it.
 */
async function claimLogicalDb(redis: Redis, redisDb: number): Promise<void> {
  const key = ownerKey(redisDb);
  if (claimedDbs.has(redisDb)) {
    const owner = await redis.get(key);
    if (owner && owner !== INVOCATION_ID) {
      throw new Error(
        `Redis logical DB ${redisDb} is in use by another vitest invocation ` +
          `(marker "${owner}", this run "${INVOCATION_ID}"). Both flush it, so ` +
          `each wipes the other's queue entries and freeze state mid-test — which ` +
          `surfaces as an unrelated test hanging until its timeout, on Redis only, ` +
          `in a test that passes in isolation. Give each invocation its own Redis ` +
          `via REDIS_URL, or run them one at a time.`
      );
    }
  }
  await redis.flushdb();
  await redis.set(key, INVOCATION_ID, "EX", OWNER_TTL_SECONDS);
  claimedDbs.add(redisDb);
}

export interface Harness {
  name: string;
  create: () => Promise<{
    backend: DianemoBackend;
    cleanup: () => Promise<void>;
  }>;
}

/**
 * A distinct logical DB per suite keeps parallel files from colliding.
 *
 * Keep `redisDb` under 16. Redis ships with exactly 16 logical databases and
 * the CI service container does not override that, so a higher index is
 * rejected with "DB index is out of range" — which ioredis surfaces as an
 * unhandled error event rather than a failure. These suites asked for 20-23 and
 * their Redis halves therefore never ran anywhere, quietly reducing every
 * "runs on both backends" claim to memory only.
 *
 * DO NOT RUN TWO VITEST INVOCATIONS AGAINST ONE REDIS. `create()` below calls
 * `flushdb()`, so a second process on the same logical DB wipes the freeze state
 * and queue entries of whichever test is mid-flight in the first. A queued
 * request whose entry has been flushed is never nominated again — there is no
 * entry to select and nothing to wake the drain — so it waits out its admission
 * timeout. The symptom is a HANG with no assertion failure, on Redis only, in a
 * test that passes every time in isolation, and it lands on whichever test
 * happened to be running. It looks exactly like a lost wake-up in the library.
 * Give each concurrent invocation its own Redis, or serialise them.
 *
 * `claimLogicalDb` above now reports that collision instead of leaving it to be
 * diagnosed from the hang — best effort, so the warning above still stands. What
 * it does NOT cover: the suites marked `direct` below, which flush a DB without
 * claiming it, and the indices more than one suite lives on. Sequential file
 * execution is what keeps both safe, so `fileParallelism: false` in
 * `vitest.config.ts` is load-bearing, not a performance preference.
 *
 * All sixteen indices are taken, so a new suite shares one. db11 carries three
 * claimants; db3, db5, db6, db7, db10, db12, db13 and db14 carry two.
 *
 *    0  memoryAtomicity
 *    1  lifecycle.integration (direct)
 *    2  backendParity
 *    3  backendFailure (direct too) · coreRecovery (direct)
 *    4  noLimit.test · multiLimit
 *    5  requestLimit.test · redisOps (direct)
 *    6  concurrencyLimit.test · luaGuards (direct)
 *    7  auditFixes · sharedLimit.test
 *    8  regressions
 *    9  auditRound2
 *   10  auditRound2 · requestLimit.scenarios
 *   11  auditRound2 · concurrencyLimit.scenarios · redisClock (direct)
 *   12  sharedLimit.scenarios · redisKeyTtl (direct)
 *   13  conformance (direct) · noLimit.scenarios (direct too)
 *   14  handler.integration (direct) · replicas.smoke
 *   15  replicas.failover
 *
 * `direct too` means the suite claims through here AND flushes on its own
 * elsewhere. db1 is `lifecycle.integration` alone because a marker written by
 * anyone else fails its assertion that every key in the DB carries the handler's
 * prefix.
 */
export function harnesses(redisDb: number): Harness[] {
  if (!Number.isInteger(redisDb) || redisDb < 0 || redisDb > 15) {
    throw new Error(
      `harnesses(${redisDb}): Redis has databases 0-15; pick an index in range or the Redis half of this suite will silently not run.`
    );
  }
  const list: Harness[] = [
    {
      name: "memory",
      create: async () => {
        const backend = memoryBackend();
        return { backend, cleanup: () => backend.close() };
      },
    },
  ];
  if (REDIS_URL) {
    list.push({
      name: "redis",
      create: async () => {
        const redis = new Redis(`${REDIS_URL}/${redisDb}`);
        try {
          await claimLogicalDb(redis, redisDb);
        } catch (error) {
          // A create that throws must not leave its connection open: an open
          // handle keeps the worker alive and turns this explicit error back
          // into the hang it exists to replace.
          await redis.quit().catch(() => undefined);
          throw error;
        }
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
  return list;
}

/** An upstream whose latency the test controls. */
export async function startUpstream(delayMs = 0) {
  let delay = delayMs;
  let inFlight = 0;
  let peakInFlight = 0;
  let served = 0;

  const server = createServer(async (_req, res) => {
    served++;
    inFlight++;
    peakInFlight = Math.max(peakInFlight, inFlight);
    if (delay > 0) await new Promise((r) => setTimeout(r, delay));
    inFlight--;
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end('{"ok":true}');
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;

  return {
    baseURL: `http://127.0.0.1:${port}`,
    setDelay: (ms: number) => {
      delay = ms;
    },
    /** The most requests the upstream ever had open at once. */
    peakInFlight: () => peakInFlight,
    /** Open right now. Should be 0 between tests; anything else is leakage. */
    inFlight: () => inFlight,
    servedCount: () => served,
    resetCounters: () => {
      peakInFlight = 0;
      served = 0;
    },
    close: async (server_: Server = server) => {
      server_.closeAllConnections();
      await new Promise<void>((r) => server_.close(() => r()));
    },
  };
}

export interface ClientSpec {
  name: string;
  /** Always a list, as the public API takes it — one limit or several. */
  rateLimit: Record<string, unknown>[];
  /** For a suite asserting what one attempt did, rather than what four did. */
  retryOptions?: Record<string, unknown>;
}

/**
 * Builds a handler with the given clients registered, runs `fn`, and tears
 * everything down — including the backend, so a leaked timer fails the run.
 */
export async function withHandler(
  harness: Harness,
  baseURL: string,
  clients: ClientSpec[],
  fn: (handler: RequestHandler, backend: DianemoBackend) => Promise<void>,
  keyPrefix = "ct"
) {
  const { backend, cleanup } = await harness.create();
  const handler = new RequestHandler({ key: KEY, backend, keyPrefix });

  for (const spec of clients) {
    await handler.registerClientTemplate(
      spec.name as never,
      ((creds: { instanceId: string }) => [
        {
          name: `${spec.name}:_:${creds.instanceId}`,
          rateLimit: spec.rateLimit,
          requestOptions: { defaults: { baseURL } },
          ...(spec.retryOptions ? { retryOptions: spec.retryOptions } : {}),
        },
      ]) as never
    );
  }
  await handler.start();
  for (const spec of clients) {
    await handler.addTemplateClient(
      spec.name as never,
      {
        instanceId: "a",
      } as never
    );
  }

  try {
    await fn(handler, backend);
  } finally {
    await handler.stop();
    await cleanup();
  }
}

/**
 * Two or more handlers over ONE Redis, which is the only way to exercise the
 * paths that make this a distributed rate limiter rather than a local one.
 *
 * `withHandler` builds a single handler, so `ownerId === instanceId` always and
 * the cross-process half of admission never runs: `requestReady:<ownerId>` is
 * handed over in process instead of published, role election has nothing to
 * elect between, and orphan recovery has no dead owner to recover from. Every
 * per-type audit flagged that gap and none could close it, because closing it
 * means changing this shared file.
 *
 * Replicas are separate `RequestHandler` instances with separate ioredis
 * connections and separate backend objects, sharing a keyspace — which is what
 * makes them distinct participants in the election. Replica 0 registers the
 * template; the rest load it from the backend as they start.
 *
 * WHICH replica ends up controller is NOT `replicas[0]` and is not start order.
 * `getClientsBefore` sorts by priority then by id DESCENDING, and ids are random
 * UUIDs, so the controller is whichever replica drew the largest one — measured
 * at 0/5/3 out of 8 trials for replicas 0/1/2. Always ask `controllerFor()` /
 * `workersFor()`; never assume an index.
 *
 * Fidelity limits worth knowing before trusting a result from this rig:
 * - The replicas share one event loop, so JS runs to completion between awaits
 *   and genuine instruction-level interleaving cannot happen. This is strong
 *   evidence for routing, ownership and metering; it is weak evidence for race
 *   safety. Force a contended state directly rather than racing for it.
 * - Module-scoped state IS shared, `inFlightRefreshes` in `authenticate.ts`
 *   being the one that matters: OAuth refreshes coalesce in process before the
 *   Redis lock is reached. A test asserting "the lock stops replicas duplicating
 *   a token call" would pass here even with the lock broken.
 * - `sharedLimit` clients are forced to `worker` by the election, so this rig
 *   cannot host one as a top-level spec — nothing would ever be its controller.
 *
 * Redis only. The memory backend is single-process by definition, so a
 * multi-replica test against it would prove nothing.
 */
export async function withReplicas(
  options: {
    count: number;
    baseURL: string;
    clients: ClientSpec[];
    redisDb: number;
    /** Distinct per test. Combined with `RUN_ID` so concurrent runs cannot collide. */
    keyPrefix: string;
    instanceId?: string;
    /** Admission budget. Defaults below the test timeout so failures name themselves. */
    cleanupTimeout?: number;
    /** Override the concurrency crash-recovery lease for failover tests. */
    concurrencySlotTtl?: number;
  },
  fn: (context: {
    replicas: RequestHandler[];
    backends: DianemoBackend[];
    /** The replica currently holding the controller role for `clientName`. */
    controllerFor: (clientName: string) => RequestHandler | undefined;
    /** Replicas that are NOT the controller for `clientName`. */
    workersFor: (clientName: string) => RequestHandler[];
    keyPrefix: string;
  }) => Promise<void>
): Promise<void> {
  if (!REDIS_URL) {
    throw new Error(
      "withReplicas requires REDIS_URL. Multi-replica behaviour cannot be tested on the memory backend, which is single-process by definition — guard the test with `if (!process.env.REDIS_URL) return`, or skip it."
    );
  }
  if (options.count < 2) {
    throw new Error(
      `withReplicas({ count: ${options.count} }): use withHandler for a single handler.`
    );
  }
  const shared = options.clients.find(
    (c) => c.rateLimit.length === 1 && c.rateLimit[0].type === "sharedLimit"
  );
  if (shared) {
    throw new Error(
      `withReplicas cannot host "${shared.name}" as a top-level spec: the election forces sharedLimit clients to "worker", so no replica ever becomes its controller and the wait below would time out with a misleading "election did not settle" error. Register the PARENT here and add the child through the handler.`
    );
  }

  const keyPrefix = uniquePrefix(options.keyPrefix);
  const instanceId = options.instanceId ?? "a";
  const connections: Redis[] = [];
  const backends: DianemoBackend[] = [];
  const replicas: RequestHandler[] = [];

  // One flush before anyone starts. Doing it per replica would wipe the peers
  // that came up first, which is the same hazard concurrent invocations create.
  // Routed through the same guard as `harnesses`, because the two flush sites
  // collide with each other as readily as with a rival run — `replicas.smoke`
  // and `handler.integration` both live on db14.
  const cleaner = new Redis(`${REDIS_URL}/${options.redisDb}`);
  try {
    await claimLogicalDb(cleaner, options.redisDb);
  } finally {
    await cleaner.quit().catch(() => undefined);
  }

  try {
    for (let i = 0; i < options.count; i++) {
      const redis = new Redis(`${REDIS_URL}/${options.redisDb}`);
      connections.push(redis);
      const backend = redisBackend(redis);
      backends.push(backend);
      const handler = new RequestHandler({ key: KEY, backend, keyPrefix });
      replicas.push(handler);

      for (const spec of options.clients) {
        await handler.registerClientTemplate(
          spec.name as never,
          ((creds: { instanceId: string }) => [
            {
              name: `${spec.name}:_:${creds.instanceId}`,
              rateLimit: spec.rateLimit,
              requestOptions: {
                // Below vitest's default test timeout, so a lost admission
                // surfaces as the library's own `RequestTimeoutError` ("timed
                // out waiting for controller") instead of a bare
                // "Test timed out in 30000ms" that says nothing about why.
                cleanupTimeout: options.cleanupTimeout ?? 8000,
                concurrencySlotTtl: options.concurrencySlotTtl,
                defaults: { baseURL: options.baseURL },
              },
            },
          ]) as never
        );
      }
      await handler.start();

      // Only the first replica registers the instance; the others discover it
      // from the backend as they start, which is what produces one controller
      // and N-1 workers rather than a race between equals.
      if (i === 0) {
        for (const spec of options.clients) {
          await handler.addTemplateClient(
            spec.name as never,
            {
              instanceId,
            } as never
          );
        }
      }
    }

    // Election settles asynchronously, and a test that fires before it has is
    // testing the race rather than the behaviour.
    await waitForSingleController(
      replicas,
      options.clients.map((s) => `${s.name}:_:${instanceId}`)
    );

    const controllerFor = (clientName: string) =>
      replicas.find((r) => r.getMetadata().ownedClients.includes(clientName));
    const workersFor = (clientName: string) =>
      replicas.filter(
        (r) => !r.getMetadata().ownedClients.includes(clientName)
      );

    await fn({ replicas, backends, controllerFor, workersFor, keyPrefix });
  } finally {
    for (const handler of replicas) {
      await handler.stop().catch(() => {});
    }
    for (const backend of backends) {
      await backend.close().catch(() => {});
    }
    for (const redis of connections) {
      await redis.quit().catch(() => {});
    }
  }
}

/** Fails loudly rather than letting a test run against an unsettled election. */
async function waitForSingleController(
  replicas: RequestHandler[],
  clientNames: string[],
  timeoutMs = 5000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let last = "";
  while (Date.now() < deadline) {
    const counts = clientNames.map(
      (name) =>
        replicas.filter((r) => r.getMetadata().ownedClients.includes(name))
          .length
    );
    if (counts.every((c) => c === 1)) return;
    last = clientNames
      .map((name, i) => `${name}=${counts[i]} controllers`)
      .join(", ");
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error(
    `Role election did not settle to exactly one controller per client within ${timeoutMs}ms: ${last}`
  );
}

export const fire = (handler: RequestHandler, clientName: string) =>
  handler.handleRequest({
    clientName,
    requestName: "t.noop",
    method: "GET",
    url: "/",
  });

export const fireMany = (
  handler: RequestHandler,
  clientName: string,
  count: number
) =>
  Promise.all(Array.from({ length: count }, () => fire(handler, clientName)));
