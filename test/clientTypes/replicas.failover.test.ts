import type { DianemoBackend } from "../../packages/core/src/backend/types.js";
import type RequestHandler from "../../packages/core/src/index.js";
import { fire, startUpstream, withReplicas } from "./harness.js";
import { createServer, type ServerResponse } from "node:http";
import { describe, expect, it } from "vitest";
import { Redis } from "ioredis";

/**
 * Failover: what happens to a client's queue when the replica draining it goes
 * away, and whether one budget survives the handover.
 *
 * The smoke suite proves the rig elects one controller and that two replicas
 * meter against one budget. This suite attacks the handover itself — controller
 * death mid-drain, an owner that dies between admission and delivery, a live
 * replica whose registration lapses, and a deliberate split brain — and measures
 * the result at the UPSTREAM rather than by timing. Over-admission is the
 * headline risk, so every budget here is one a per-replica budget would visibly
 * exceed: a token bucket whose refill is a minute away, or a concurrency cap the
 * upstream itself reports occupancy against.
 *
 * Redis only. Multi-replica behaviour cannot be tested on the memory backend.
 */
const HAS_REDIS = Boolean(process.env.REDIS_URL);
const REDIS_URL = process.env.REDIS_URL ?? "";
/** 14 is the smoke suite's; 4-13 belong to the per-type suites. */
const REDIS_DB = 15;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * The parts of a replica a crash reaches but the public API does not.
 *
 * A real crash stops the heartbeat, the health tick and every subscription at
 * once. `stop()` cannot stand in for it: it drains, deregisters and announces
 * the departure, which is the graceful case and the one failover does NOT have
 * to guess about. Nothing here changes library behaviour — it only removes this
 * replica's ability to act, the way SIGKILL would.
 */
interface HandlerInternals {
  heartbeatInterval?: NodeJS.Timeout;
  heartbeatTimeouts: Map<string, NodeJS.Timeout>;
  clients: Map<
    string,
    {
      removeHealthCheckInterval: () => void;
      clearDrainTimer: () => void;
      updateRole: (role: "controller" | "worker") => Promise<void>;
      /** The client's axios instance, so a kill can cut its outbound calls too. */
      http?: { request: (...args: unknown[]) => Promise<unknown> };
    }
  >;
}

const internals = (handler: RequestHandler) =>
  handler as unknown as HandlerInternals;

/**
 * Makes a replica stop participating, without announcing anything.
 *
 * Timers first, then the pub/sub connection, then the registration key — which
 * is what an expired TTL leaves behind. The instance stays in the `instances`
 * SET, exactly as a killed process would: only a peer's election prunes that.
 *
 * Deliberately NOT `client.destroy()`: that cancels the replica's parked
 * requests, and a cancelled request publishes `requestDone` so the controller
 * releases its queue entry. That is the graceful path — it announces the
 * abandonment — and using it here quietly cleaned up the very state a crash is
 * supposed to leave behind, which made a dead-owner test measure nothing.
 *
 * WHAT THIS DOES NOT STOP: publishing. `backend.close()` closes only what the
 * backend opened for itself, and the ioredis handle the harness injected belongs
 * to the caller — so this replica's HTTP calls still settle and still announce
 * `requestDone`, and the controller acts on it. Any test measuring what a crash
 * STRANDS must add `silenceReplica` below, or it measures the response time of a
 * request that reported itself: a slot expected to need orphan cleanup came back
 * at +5986 ms against a 6000 ms upstream, which read as a working fix.
 */
async function hardKill(
  handler: RequestHandler,
  backend: DianemoBackend,
  raw: Redis
): Promise<void> {
  const inner = internals(handler);
  // The harness still calls stop() at teardown, and its drain would otherwise
  // poke this replica's clients back into life for the full drain budget.
  handler.setDrainTimeout(0);
  clearInterval(inner.heartbeatInterval);
  for (const timeout of inner.heartbeatTimeouts.values()) clearTimeout(timeout);
  for (const client of inner.clients.values()) {
    client.removeHealthCheckInterval();
    client.clearDrainTimer();
  }
  await backend.close();
  await raw.del(
    `${handler.getNamespace()}:instance:${handler.getInstanceId()}`
  );
}

/**
 * The other half of a kill: a dead process announces nothing and sends nothing.
 *
 * Separate from `hardKill` so the tests that predate it keep their exact
 * behaviour. Use both together whenever the state under test is state a crash
 * leaves BEHIND — a queue entry, a concurrency slot — rather than state a crash
 * merely fails to advance.
 *
 * The HTTP half matters as much as the publish: a killed replica's in-flight
 * request does not come back a second later on a retry, and leaving that alive
 * lets the corpse re-enter admission, take a slot on the shared ledger and reach
 * the upstream again, in the middle of the measurement.
 */
function silenceReplica(
  handler: RequestHandler,
  backend: DianemoBackend
): void {
  backend.publish = async () => {};
  for (const client of internals(handler).clients.values()) {
    if (client.http) {
      client.http.request = () => Promise.reject(new Error("replica is gone"));
    }
  }
}

/**
 * An upstream that holds a request open until the test says otherwise, and can
 * drop the connection the way a killed client process does.
 *
 * `startUpstream` answers after a delay, which cannot express either half of
 * this: a request has to still be open when its owner dies, and the vendor's own
 * occupancy has to fall when the owner's process goes away — otherwise a cap
 * assertion cannot tell "the request is gone" from "the request is still
 * running".
 */
async function startHoldableUpstream() {
  const held = new Map<
    string,
    Set<{ res: ServerResponse; done: () => void }>
  >();
  let inFlight = 0;
  let peakInFlight = 0;

  const server = createServer(async (req, res) => {
    const path = (req.url ?? "/").split("?")[0];
    inFlight++;
    peakInFlight = Math.max(peakInFlight, inFlight);
    try {
      await new Promise<void>((resolve) => {
        const entry = { res, done: () => resolve() };
        if (!held.has(path)) held.set(path, new Set());
        held.get(path)!.add(entry);
      });
      if (!res.writableEnded) {
        res.writeHead(200, { "content-type": "application/json" });
        res.end('{"ok":true}');
      }
    } finally {
      inFlight--;
    }
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;

  const release = (path?: string) => {
    for (const [key, entries] of held) {
      if (path && key !== path) continue;
      for (const entry of [...entries]) {
        entries.delete(entry);
        entry.done();
      }
    }
  };

  return {
    baseURL: `http://127.0.0.1:${port}`,
    openOn: (path: string) => held.get(path)?.size ?? 0,
    inFlight: () => inFlight,
    peakInFlight: () => peakInFlight,
    release,
    /** Kills the connections on `path`, as the vendor sees it when a client dies. */
    drop: (path: string) => {
      const entries = held.get(path);
      if (!entries) return 0;
      let dropped = 0;
      for (const entry of [...entries]) {
        entry.res.socket?.destroy();
        entries.delete(entry);
        entry.done();
        dropped++;
      }
      return dropped;
    },
    close: async () => {
      release();
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

/**
 * Ordinary peer broadcasts, used to reach the state a real deployment reaches on
 * its own timers — a peer joining or leaving makes every replica re-elect, and a
 * departure makes the controller sweep for orphans. Waiting for the 10 s health
 * tick and the 12 s peer timeout instead would only make these tests slower.
 * The payload is an id nobody owns, so no replica treats it as its own.
 */
const NUDGE = "failover-test-nudge";

async function forceElection(raw: Redis, namespace: string): Promise<void> {
  await raw.publish(`${namespace}:instanceUpdated`, NUDGE);
}

async function forceOrphanCleanup(
  raw: Redis,
  namespace: string
): Promise<void> {
  await raw.publish(`${namespace}:instanceStopped`, NUDGE);
}

/**
 * Two controllers for one client, held for the length of a measurement.
 *
 * The health tick is stopped on every replica first. It re-runs election, and a
 * re-election landing mid-measurement demotes one of the two — which measures
 * convergence rather than split brain. The settle before promoting is for the
 * same reason: the harness returns as soon as exactly one replica claims the
 * client, which can be before a peer's own start-up election has finished.
 */
async function forceSplitBrain(
  replicas: RequestHandler[],
  clientName: string
): Promise<void> {
  for (const replica of replicas) {
    for (const client of internals(replica).clients.values()) {
      client.removeHealthCheckInterval();
    }
  }
  await sleep(1000);
  for (const replica of replicas) {
    await internals(replica).clients.get(clientName)?.updateRole("controller");
  }
}

/**
 * Puts a replica outside the alive set while it keeps running.
 *
 * This is what a stall longer than the 10 s registration TTL looks like from a
 * peer: the `instance:<id>` key is gone at the moment that peer elects, and
 * election is what SREMs the id out of the `instances` set. The replica's own
 * heartbeat restores the key a beat later — it never stopped running — but
 * nothing re-adds the set membership, so the eviction sticks.
 *
 * The loop is a race with that heartbeat: the delete has to land before the next
 * beat, and the election has to run before the beat after it. The heartbeat then
 * puts both the key and the membership back, which is the stall ending — pass
 * `stillStalled` to keep the replica out, for the case where it has not.
 */
async function evictFromAliveSet(
  raw: Redis,
  namespace: string,
  instanceId: string,
  stillStalled?: RequestHandler
): Promise<void> {
  if (stillStalled) clearInterval(internals(stillStalled).heartbeatInterval);
  const instancesKey = `${namespace}:instances`;
  for (let attempt = 0; attempt < 40; attempt++) {
    await raw.del(`${namespace}:instance:${instanceId}`);
    await forceElection(raw, namespace);
    await sleep(50);
    const members = await raw.smembers(instancesKey);
    if (!members.includes(instanceId)) return;
  }
  throw new Error(`Could not evict ${instanceId} from ${instancesKey}`);
}

async function waitFor(
  label: string,
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 20_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await sleep(25);
  }
  throw new Error(`Timed out after ${timeoutMs}ms waiting for ${label}`);
}

/** Where a client's queue lives, for reading queue state behind the library's back. */
const queueKeyFor = (handler: RequestHandler, clientName: string) =>
  `${handler.getNamespace()}:${clientName}:queue`;

const metadataKeyFor = (
  handler: RequestHandler,
  clientName: string,
  requestId: string
) => `${handler.getNamespace()}:${clientName}:request:${requestId}`;

/** Fires without leaving an unhandled rejection behind when the request is abandoned. */
function fireCounted(
  handler: RequestHandler,
  clientName: string,
  tally: { ok: number; failed: number; statuses: number[] }
): Promise<void> {
  return fire(handler, clientName).then(
    (response) => {
      tally.ok++;
      tally.statuses.push(response.status);
    },
    () => {
      tally.failed++;
    }
  );
}

describe.skipIf(!HAS_REDIS)("multi-replica failover", () => {
  it("wakes the controller promptly when a worker-owned request completes", async () => {
    const upstream = await startUpstream(300);
    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              name: "remote-done",
              rateLimit: [{ type: "concurrencyLimit", maxConcurrency: 1 }],
            },
          ],
          redisDb: REDIS_DB,
          keyPrefix: "remote-done",
        },
        async ({ controllerFor, workersFor }) => {
          const client = "remote-done:_:a";
          const controller = controllerFor(client);
          const worker = workersFor(client)[0];
          expect(controller).toBeDefined();
          expect(worker).toBeDefined();
          if (!controller || !worker) return;

          const first = fire(worker, client);
          await waitFor(
            "the worker request to reach upstream",
            () => upstream.inFlight() === 1
          );
          const second = fire(controller, client);
          await waitFor(
            "remote completion to wake the controller",
            () => upstream.servedCount() === 2,
            3_000
          );
          await Promise.all([first, second]);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  it("keeps the concurrency cap across a controller death mid-drain, and loses nothing", async () => {
    const upstream = await startUpstream(250);
    const raw = new Redis(`${REDIS_URL}/${REDIS_DB}`);
    try {
      await withReplicas(
        {
          count: 3,
          baseURL: upstream.baseURL,
          clients: [
            {
              name: "fo1",
              rateLimit: [{ type: "concurrencyLimit", maxConcurrency: 2 }],
            },
          ],
          redisDb: REDIS_DB,
          keyPrefix: "fo1",
        },
        async ({ replicas, backends, controllerFor, workersFor }) => {
          const client = "fo1:_:a";
          const controller = controllerFor(client);
          expect(controller).toBeDefined();
          if (!controller) return;
          const workers = workersFor(client);
          const namespace = controller.getNamespace();

          // Every request is submitted through a worker, so admission has to
          // cross the process boundary for all of them.
          const tally = { ok: 0, failed: 0, statuses: [] as number[] };
          const all = Array.from({ length: 24 }, (_, i) =>
            fireCounted(workers[i % workers.length], client, tally)
          );

          await waitFor(
            "the controller to start draining",
            () => upstream.servedCount() >= 3
          );
          const servedAtKill = upstream.servedCount();

          await hardKill(
            controller,
            backends[replicas.indexOf(controller)],
            raw
          );
          await forceElection(raw, namespace);

          await waitFor("a survivor to take the controller role", () =>
            workers.some((w) => w.getMetadata().ownedClients.includes(client))
          );
          await waitFor(
            "the survivor to admit work the dead controller had not",
            () => upstream.servedCount() > servedAtKill,
            30_000
          );

          await waitFor(
            "every request to settle",
            () => tally.ok + tally.failed === 24,
            40_000
          );
          await Promise.all(all);

          // The headline: the cap is a fleet-wide cap. A per-replica cap, or a
          // slot released while its request was still open, would show 3+ here.
          expect(upstream.peakInFlight()).toBeLessThanOrEqual(2);
          // And the handover dropped nothing.
          expect(tally.failed).toBe(0);
          expect(tally.ok).toBe(24);
          expect(upstream.servedCount()).toBe(24);
        }
      );
    } finally {
      await raw.quit();
      await upstream.close();
    }
  }, 90_000);

  it("does not hand the survivor a fresh token budget when the controller dies", async () => {
    const upstream = await startUpstream(0);
    const raw = new Redis(`${REDIS_URL}/${REDIS_DB}`);
    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              // 200 tokens and a refill a minute away: everything served in
              // this test comes out of one bucket, so a 201st request is
              // unambiguous over-admission rather than a timing artefact. The
              // budget is large so that draining it takes long enough for the
              // kill below to land while the controller is still admitting.
              name: "fo2",
              rateLimit: [
                {
                  type: "requestLimit",
                  maxTokens: 200,
                  tokensToAdd: 200,
                  interval: 60_000,
                },
              ],
            },
          ],
          redisDb: REDIS_DB,
          keyPrefix: "fo2",
        },
        async ({ replicas, backends, controllerFor, workersFor }) => {
          const client = "fo2:_:a";
          const controller = controllerFor(client);
          expect(controller).toBeDefined();
          if (!controller) return;
          const worker = workersFor(client)[0];
          const namespace = controller.getNamespace();

          // 260 submitted against a budget of 200, so 60 are queued with no
          // capacity behind them at the moment the controller dies.
          const tally = { ok: 0, failed: 0, statuses: [] as number[] };
          for (let i = 0; i < 260; i++) void fireCounted(worker, client, tally);

          await waitFor(
            "the controller to start draining",
            () => upstream.servedCount() >= 5
          );
          const servedAtKill = upstream.servedCount();

          await hardKill(
            controller,
            backends[replicas.indexOf(controller)],
            raw
          );
          await forceElection(raw, namespace);
          await waitFor("the survivor to take the controller role", () =>
            worker.getMetadata().ownedClients.includes(client)
          );

          // Long enough for the new controller to drain everything it can, and
          // far short of the 60 s refill.
          await sleep(4000);

          const served = upstream.servedCount();
          // A survivor starting from a full local bucket would admit 200 more
          // out of the 60 still queued — i.e. all of them.
          expect(served).toBeLessThanOrEqual(200);
          // The survivor kept draining what the dead controller had not.
          expect(served).toBeGreaterThan(servedAtKill);
          // Nothing was lost beyond the one admission the dying controller could
          // have been part-way through.
          expect(served).toBeGreaterThanOrEqual(199);
          expect(tally.ok).toBe(served);

          const stats = await worker.getClientStats(client);
          expect(stats.rateLimit[0]).toMatchObject({ tokens: 0 });
        }
      );
    } finally {
      await raw.quit();
      await upstream.close();
    }
  }, 90_000);

  it("holds one token budget while two replicas both believe they are controller", async () => {
    const upstream = await startUpstream(0);
    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              name: "fo3",
              rateLimit: [
                {
                  type: "requestLimit",
                  maxTokens: 4,
                  tokensToAdd: 4,
                  interval: 60_000,
                },
              ],
            },
          ],
          redisDb: REDIS_DB,
          keyPrefix: "fo3",
        },
        async ({ replicas }) => {
          const client = "fo3:_:a";
          // Both replicas believe they own the client — the state a partition,
          // or a registration that lapsed while its holder kept running, leaves
          // behind.
          await forceSplitBrain(replicas, client);
          for (const replica of replicas) {
            expect(replica.getMetadata().ownedClients).toContain(client);
            // 16 of the 20 below can never be admitted, and each replica owns
            // half of them — so without this, teardown spends its whole 10 s
            // drain budget per replica waiting on its own unservable work.
            replica.setDrainTimeout(300);
          }

          const tally = { ok: 0, failed: 0, statuses: [] as number[] };
          for (let i = 0; i < 10; i++) {
            void fireCounted(replicas[0], client, tally);
            void fireCounted(replicas[1], client, tally);
          }

          await sleep(2500);

          // Two drain loops, one bucket. Two buckets would serve 8.
          expect(upstream.servedCount()).toBe(4);
          expect(tally.ok).toBe(4);
          // The split brain was still live while that was measured.
          for (const replica of replicas) {
            expect(replica.getMetadata().ownedClients).toContain(client);
          }
        }
      );
    } finally {
      await upstream.close();
    }
  }, 90_000);

  it("holds one concurrency cap while two replicas both believe they are controller", async () => {
    const upstream = await startUpstream(250);
    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              name: "fo4",
              rateLimit: [{ type: "concurrencyLimit", maxConcurrency: 2 }],
            },
          ],
          redisDb: REDIS_DB,
          keyPrefix: "fo4",
        },
        async ({ replicas }) => {
          const client = "fo4:_:a";
          await forceSplitBrain(replicas, client);
          for (const replica of replicas) {
            expect(replica.getMetadata().ownedClients).toContain(client);
          }

          const tally = { ok: 0, failed: 0, statuses: [] as number[] };
          const all: Promise<void>[] = [];
          for (let i = 0; i < 12; i++) {
            all.push(fireCounted(replicas[i % 2], client, tally));
          }
          await waitFor(
            "every request to settle",
            () => tally.ok + tally.failed === 12,
            40_000
          );
          await Promise.all(all);

          // Two drain loops nominating from one queue and claiming from one
          // ledger. Anything above 2 means both admitted the same capacity.
          expect(upstream.peakInFlight()).toBeLessThanOrEqual(2);
          expect(tally.ok).toBe(12);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 90_000);

  it("keeps a briefly evicted live replica's queued work, and re-registers it", async () => {
    const upstream = await startUpstream(0);
    const raw = new Redis(`${REDIS_URL}/${REDIS_DB}`);
    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              // One token, refilling once well after the sweep below: the
              // survival of the queue entry is measured while the budget is
              // still empty, and the request is then served on the refill.
              name: "fo5",
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
          keyPrefix: "fo5",
        },
        async ({ controllerFor, workersFor }) => {
          const client = "fo5:_:a";
          const controller = controllerFor(client);
          expect(controller).toBeDefined();
          if (!controller) return;
          const worker = workersFor(client)[0];
          const namespace = controller.getNamespace();
          const workerId = worker.getInstanceId();
          const instancesKey = `${namespace}:instances`;
          const workerKey = `${namespace}:instance:${workerId}`;
          const queueKey = queueKeyFor(controller, client);

          // Spend the only token, so the worker's request has to queue.
          const first = await fire(controller, client);
          expect(first.status).toBe(200);

          const tally = { ok: 0, failed: 0, statuses: [] as number[] };
          void fireCounted(worker, client, tally);
          await waitFor(
            "the worker's request to reach the queue",
            async () => (await raw.zcard(queueKey)) === 1
          );

          // A stall longer than the 10 s registration TTL, observed by a peer:
          // the registration key is gone at the moment the peer elects, and
          // election is what SREMs the id out of the alive set.
          await evictFromAliveSet(raw, namespace, workerId);
          expect(await raw.smembers(instancesKey)).not.toContain(workerId);

          // The stall is over, and the replica never stopped running. Its
          // heartbeat now restores BOTH the registration key and the set
          // membership, within a beat or two. Membership used to be restored by
          // nothing at all — the only SADD ran at startup — so a replica that
          // missed ~10 beats was evicted for good and the controller deleted
          // everything it queued from then on.
          await waitFor(
            "the heartbeat to restore the worker's registration",
            async () => (await raw.exists(workerKey)) === 1,
            5000
          );
          await waitFor(
            "the heartbeat to restore the worker's alive-set membership",
            async () => (await raw.smembers(instancesKey)).includes(workerId),
            3000
          );

          // So the controller's next orphan sweep leaves its work alone.
          await forceOrphanCleanup(raw, namespace);
          await sleep(1000);
          expect(await raw.zcard(queueKey)).toBe(1);
          expect(tally.failed).toBe(0);

          // And the caller is served, on the refill, as if nothing had happened.
          await waitFor(
            "the surviving request to be served",
            () => tally.ok === 1,
            10_000
          );
          expect(upstream.servedCount()).toBe(2);
          expect(tally.statuses).toEqual([200]);
        }
      );
    } finally {
      await raw.quit();
      await upstream.close();
    }
  }, 90_000);

  it("burns one token when the owner dies between admission and delivery, and reclaims the entry exactly once", async () => {
    const upstream = await startUpstream(0);
    const raw = new Redis(`${REDIS_URL}/${REDIS_DB}`);
    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              // A refill soon enough that the controller admits the dead
              // owner's request during the test.
              name: "fo6",
              rateLimit: [
                {
                  type: "requestLimit",
                  maxTokens: 1,
                  tokensToAdd: 1,
                  interval: 1200,
                },
              ],
            },
          ],
          redisDb: REDIS_DB,
          keyPrefix: "fo6",
        },
        async ({ replicas, backends, controllerFor, workersFor }) => {
          const client = "fo6:_:a";
          const controller = controllerFor(client);
          expect(controller).toBeDefined();
          if (!controller) return;
          const worker = workersFor(client)[0];
          const namespace = controller.getNamespace();
          const queueKey = queueKeyFor(controller, client);

          const first = await fire(controller, client);
          expect(first.status).toBe(200);

          const tally = { ok: 0, failed: 0, statuses: [] as number[] };
          void fireCounted(worker, client, tally);
          await waitFor(
            "the worker's request to reach the queue",
            async () => (await raw.zcard(queueKey)) === 1
          );
          const [requestId] = await raw.zrange(queueKey, 0, -1);
          expect(requestId).toBeDefined();

          await hardKill(worker, backends[replicas.indexOf(worker)], raw);

          // The refill arrives and the controller admits a request whose owner
          // cannot receive `requestReady`.
          await waitFor(
            "the controller to admit the orphaned request",
            async () =>
              (await raw.hget(
                metadataKeyFor(controller, client, requestId),
                "status"
              )) === "inProgress",
            8000
          );
          await sleep(500);
          // Admitted, so its token is spent, but nobody ran it.
          expect(upstream.servedCount()).toBe(1);

          // ONE departure notice is enough. `handleInstanceStopped` re-runs
          // election before sweeping, and election is what prunes a crashed id
          // out of the `instances` set, so the sweep that follows it already
          // sees this owner as dead. Sweeping first meant the first notice
          // reclaimed nothing and recovery waited for the next health tick.
          //
          // The entry is reclaimed, and it is not handed to anyone else: a
          // reclaimed request is a lost request, never a second dispatch.
          await forceOrphanCleanup(raw, namespace);
          await waitFor(
            "the orphaned entry to be reclaimed",
            async () => (await raw.zcard(queueKey)) === 0,
            10_000
          );
          await sleep(1500);
          expect(upstream.servedCount()).toBe(1);
          expect(
            await raw.exists(metadataKeyFor(controller, client, requestId))
          ).toBe(0);
        }
      );
    } finally {
      await raw.quit();
      await upstream.close();
    }
  }, 90_000);

  /**
   * A missing heartbeat cannot prove the upstream request ended: it can mean a
   * dead process or only a stalled event loop. Queue metadata is safe to prune,
   * but capacity remains reserved until the slot lease expires. This chooses
   * vendor-cap safety over faster recovery from a definite crash.
   */
  it("recovers a killed owner's concurrency slot through its lease without over-admitting", async () => {
    const upstream = await startHoldableUpstream();
    const raw = new Redis(`${REDIS_URL}/${REDIS_DB}`);
    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              // One at a time, so "the survivor got the capacity" and "the cap
              // was not exceeded" are both single-request questions.
              name: "fo10",
              rateLimit: [{ type: "concurrencyLimit", maxConcurrency: 1 }],
            },
          ],
          redisDb: REDIS_DB,
          keyPrefix: "fo10",
          // Long: every request here is held open deliberately, and a request
          // that gave up on admission mid-measurement would look like a lost
          // wake-up.
          cleanupTimeout: 40_000,
          concurrencySlotTtl: 2500,
        },
        async ({ replicas, backends, controllerFor, workersFor }) => {
          const client = "fo10:_:a";
          const controller = controllerFor(client);
          expect(controller).toBeDefined();
          if (!controller) return;
          const worker = workersFor(client)[0];
          const namespace = controller.getNamespace();
          const slotsKey = `${namespace}:${client}:concurrency:default`;
          const queueKey = queueKeyFor(controller, client);

          const fireTo = (handler: RequestHandler, url: string) =>
            handler
              .handleRequest({
                clientName: client,
                requestName: "t.hold",
                method: "GET",
                url,
              })
              .catch(() => undefined);

          // A: the controller takes the only slot on the fast path.
          const a = fireTo(controller, "/a");
          await waitFor(
            "A to occupy the slot",
            () => upstream.openOn("/a") === 1
          );

          // B: queued, because the slot is taken.
          const b = fireTo(worker, "/b");
          await waitFor(
            "B to be queued",
            async () => (await raw.zcard(queueKey)) === 1
          );
          const [requestId] = await raw.zrange(queueKey, 0, -1);

          // Freeing the slot makes the controller admit B for the worker, which
          // is what gives B a queue entry while it runs.
          upstream.release("/a");
          await a;
          await waitFor("B to be in flight", () => upstream.openOn("/b") === 1);
          await waitFor(
            "B's entry to be marked in progress",
            async () =>
              (await raw.hget(
                metadataKeyFor(controller, client, requestId),
                "status"
              )) === "inProgress"
          );
          expect(await raw.zcard(slotsKey)).toBe(1);

          // B's owner is killed with B open at the upstream. The socket dies with
          // the process, so the vendor's own occupancy falls to zero — which is
          // what makes the capacity genuinely free rather than merely unclaimed.
          await hardKill(worker, backends[replicas.indexOf(worker)], raw);
          silenceReplica(worker, backends[replicas.indexOf(worker)]);
          expect(upstream.drop("/b")).toBe(1);
          await waitFor(
            "the upstream to see B end",
            () => upstream.inFlight() === 0
          );

          await forceElection(raw, namespace);
          await forceOrphanCleanup(raw, namespace);

          // Orphan cleanup must not release the slot based only on a lapsed
          // registration. A survivor queues behind it until the short test
          // lease expires; admission then reaps the stale slot atomically.
          expect(await raw.zcard(slotsKey)).toBe(1);
          const c = fireTo(controller, "/c");
          await waitFor(
            "a survivor to use capacity after the dead slot lease expires",
            () => upstream.openOn("/c") === 1,
            10_000
          );
          // The upstream never saw two at once.
          expect(upstream.peakInFlight()).toBe(1);
          // B's orphaned entry is reclaimed. C's is NOT: it is deliberately
          // still open at the upstream, and an admitted request keeps its queue
          // entry while it runs — asserting an empty queue here would measure C
          // rather than the reclamation this test is about.
          //
          // Awaited rather than sampled: `forceOrphanCleanup` publishes, and the
          // sweep it wakes runs on the controller's own schedule. Reading the
          // queue at one instant asserts *when* cleanup ran, which is not a
          // property this library offers — only that it does.
          await waitFor(
            "B's orphaned entry to be reclaimed",
            async () => !(await raw.zrange(queueKey, 0, -1)).includes(requestId)
          );
          // And C's survives it, which is the half a blanket sweep would break.
          expect(await raw.zrange(queueKey, 0, -1)).toHaveLength(1);

          upstream.release();
          await c;
          await waitFor(
            "the queue to drain once C completes",
            async () => (await raw.zcard(queueKey)) === 0
          );
          void b;
        }
      );
    } finally {
      await raw.quit();
      await upstream.close();
    }
  }, 90_000);

  /**
   * Reaching this state needs an owner that is alive but outside the alive set,
   * which is a genuine ≥10 s heartbeat outage: the eviction below also stops the
   * replica's heartbeat, because a stall that ended would re-register within a
   * beat and the sweep would correctly leave the request alone.
   */
  it("preserves a live slot when orphan cleanup removes a stalled owner's queue entry", async () => {
    const upstream = await startUpstream(4000);
    const raw = new Redis(`${REDIS_URL}/${REDIS_DB}`);
    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              // One at a time, so a second open request at the upstream is
              // over-admission with nothing to argue about.
              name: "fo9",
              rateLimit: [{ type: "concurrencyLimit", maxConcurrency: 1 }],
            },
          ],
          redisDb: REDIS_DB,
          keyPrefix: "fo9",
        },
        async ({ controllerFor, workersFor }) => {
          const client = "fo9:_:a";
          const controller = controllerFor(client);
          expect(controller).toBeDefined();
          if (!controller) return;
          const worker = workersFor(client)[0];
          const namespace = controller.getNamespace();
          const queueKey = queueKeyFor(controller, client);

          const tally = { ok: 0, failed: 0, statuses: [] as number[] };
          // A takes the only slot on the fast path, so it leaves no queue entry.
          // B therefore has to queue, and is admitted by the CONTROLLER when A
          // finishes — which is what gives it a queue entry while it runs.
          void fireCounted(worker, client, tally);
          await waitFor(
            "A to occupy the slot",
            () => upstream.inFlight() === 1
          );
          void fireCounted(worker, client, tally);
          await waitFor(
            "B to be queued",
            async () => (await raw.zcard(queueKey)) === 1
          );
          // `servedCount()` counts arrivals, not completions, so two arrivals
          // with one open is exactly "A finished, B is now the one in flight".
          await waitFor(
            "B to be admitted and in flight",
            () => upstream.servedCount() === 2 && upstream.inFlight() === 1,
            20_000
          );

          // B's owner stalls past its registration TTL and a peer prunes it. B
          // itself is untouched: still open at the upstream, still holding the
          // only slot — a stalled event loop does not close a socket the vendor
          // is already serving.
          await evictFromAliveSet(
            raw,
            namespace,
            worker.getInstanceId(),
            worker
          );
          await forceOrphanCleanup(raw, namespace);
          await waitFor(
            "the controller to reclaim B",
            async () => (await raw.zcard(queueKey)) === 0,
            10_000
          );

          // Queue cleanup cannot prove the upstream request ended, so the slot
          // remains until B finishes or its crash-recovery TTL expires.
          expect(upstream.inFlight()).toBe(1);
          const c = fire(controller, client);
          await c;
          expect(upstream.peakInFlight()).toBe(1);
        }
      );
    } finally {
      await raw.quit();
      await upstream.close();
    }
  }, 90_000);

  /** Completion cleanup remains owner-driven during the election gap. */
  it("cleans completions on the owner while no replica holds the controller role", async () => {
    const upstream = await startUpstream(400);
    const raw = new Redis(`${REDIS_URL}/${REDIS_DB}`);
    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              name: "fo8",
              rateLimit: [{ type: "concurrencyLimit", maxConcurrency: 2 }],
            },
          ],
          redisDb: REDIS_DB,
          keyPrefix: "fo8",
        },
        async ({ replicas, backends, controllerFor, workersFor }) => {
          const client = "fo8:_:a";
          const controller = controllerFor(client);
          expect(controller).toBeDefined();
          if (!controller) return;
          const worker = workersFor(client)[0];
          const namespace = controller.getNamespace();
          const concurrencyKey = `${namespace}:${client}:concurrency:default`;
          const queueKey = queueKeyFor(controller, client);

          const tally = { ok: 0, failed: 0, statuses: [] as number[] };
          for (let i = 0; i < 6; i++) void fireCounted(worker, client, tally);

          await waitFor(
            "the cap to be fully occupied",
            () => upstream.inFlight() === 2
          );
          expect(await raw.zcard(concurrencyKey)).toBe(2);

          // No election nudge: this is the gap a real crash opens, between the
          // controller dying and its peers noticing (a peer timeout, or a health
          // tick — 10-12 s by default). Every request that completes inside it
          // reports `requestDone` to a fleet where every replica is a worker,
          // and a worker returns from that handler immediately.
          await hardKill(
            controller,
            backends[replicas.indexOf(controller)],
            raw
          );
          await waitFor(
            "the two in-flight requests to finish inside the gap",
            () => upstream.inFlight() === 0 && upstream.servedCount() === 2
          );

          // Now let the survivor take over, and give it plenty of time.
          await forceElection(raw, namespace);
          await waitFor("the survivor to take the controller role", () =>
            worker.getMetadata().ownedClients.includes(client)
          );
          await sleep(3000);

          await waitFor(
            "the survivor to drain all requests after election",
            () => upstream.servedCount() === 6,
            15_000
          );
          expect(upstream.inFlight()).toBe(0);
          expect(await raw.zcard(concurrencyKey)).toBe(0);
          expect(upstream.servedCount()).toBe(6);
          expect(tally.ok).toBe(6);
          expect(await raw.zcard(queueKey)).toBe(0);
        }
      );
    } finally {
      await raw.quit();
      await upstream.close();
    }
  }, 90_000);

  it("fails a stopping replica's queued work with a reason and lets the controller reclaim it", async () => {
    const upstream = await startUpstream(0);
    const raw = new Redis(`${REDIS_URL}/${REDIS_DB}`);
    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              name: "fo7",
              rateLimit: [
                {
                  type: "requestLimit",
                  maxTokens: 1,
                  tokensToAdd: 1,
                  interval: 60_000,
                },
              ],
            },
          ],
          redisDb: REDIS_DB,
          keyPrefix: "fo7",
        },
        async ({ controllerFor, workersFor }) => {
          const client = "fo7:_:a";
          const controller = controllerFor(client);
          expect(controller).toBeDefined();
          if (!controller) return;
          const worker = workersFor(client)[0];
          const queueKey = queueKeyFor(controller, client);

          const first = await fire(controller, client);
          expect(first.status).toBe(200);

          const queued = [fire(worker, client), fire(worker, client)];
          const settled = Promise.allSettled(queued);
          await waitFor(
            "both requests to reach the queue",
            async () => (await raw.zcard(queueKey)) === 2
          );

          // The shutdown drain watches the SHARED queue, so without a shorter
          // budget this waits the full 10 s for work it cannot serve.
          worker.setDrainTimeout(300);
          const started = Date.now();
          await worker.stop();
          const results = await settled;
          const elapsed = Date.now() - started;

          // Told why, promptly — not left to the 60 s admission timeout.
          expect(results.map((r) => r.status)).toEqual([
            "rejected",
            "rejected",
          ]);
          for (const result of results) {
            if (result.status !== "rejected") continue;
            expect(String(result.reason?.message)).toMatch(/stopped/i);
          }
          expect(elapsed).toBeLessThan(20_000);

          // And the departure it announced lets the controller clear the
          // entries, rather than leaving them to pin the queue above empty.
          await waitFor(
            "the controller to reclaim the stopped replica's entries",
            async () => (await raw.zcard(queueKey)) === 0,
            15_000
          );
          expect(upstream.servedCount()).toBe(1);
          expect(controller.getMetadata().ownedClients).toContain(client);
        }
      );
    } finally {
      await raw.quit();
      await upstream.close();
    }
  }, 90_000);
});
