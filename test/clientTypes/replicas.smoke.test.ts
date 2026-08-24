import { fire, startUpstream, withReplicas } from "./harness.js";
import { describe, expect, it } from "vitest";

/**
 * Proves the multi-replica rig itself works, so a failure in a real
 * multi-replica test means the library rather than the fixture.
 *
 * The rig is the thing every per-type audit said it needed and none could build,
 * because it lives in a shared file. These four cases are the properties the
 * rest of the distributed tests will rely on: exactly one controller, work
 * accepted through a worker, the cross-process `requestReady` route actually
 * taken, and one budget honoured across replicas.
 */
const HAS_REDIS = Boolean(process.env.REDIS_URL);

describe.skipIf(!HAS_REDIS)("multi-replica rig", () => {
  it("elects exactly one controller across three replicas", async () => {
    const upstream = await startUpstream(0);
    try {
      await withReplicas(
        {
          count: 3,
          baseURL: upstream.baseURL,
          clients: [{ name: "mr1", rateLimit: { type: "noLimit" } }],
          redisDb: 14,
          keyPrefix: "mr1",
        },
        async ({ replicas, controllerFor, workersFor }) => {
          const client = "mr1:_:a";
          expect(replicas).toHaveLength(3);
          expect(controllerFor(client)).toBeDefined();
          expect(workersFor(client)).toHaveLength(2);
          // Every replica knows the client, or a worker could not enqueue for it.
          for (const replica of replicas) {
            expect(replica.getMetadata().registeredClients).toContain(client);
          }
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  it("serves a request submitted through a worker, not the controller", async () => {
    const upstream = await startUpstream(0);
    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              name: "mr2",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 10,
                tokensToAdd: 10,
                interval: 1000,
              },
            },
          ],
          redisDb: 14,
          keyPrefix: "mr2",
        },
        async ({ workersFor, backends, keyPrefix }) => {
          const client = "mr2:_:a";
          const worker = workersFor(client)[0];
          expect(worker).toBeDefined();

          const response = await fire(worker, client);
          expect(response.status).toBe(200);
          expect(upstream.servedCount()).toBe(1);
          // An ample budget and an empty queue means the WORKER admitted this
          // itself on the fast path, without involving the controller at all —
          // an untested property in its own right, and the thing this test
          // actually exercises. Asserting only status/served would have been
          // satisfied by either mechanism, so it pinned neither.
          expect(
            await backends[0].getQueueLength(
              `${keyPrefix}:requestHandler:${client}:queue`
            )
          ).toBe(0);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  it("routes admission across the process boundary when the budget forces queueing", async () => {
    const upstream = await startUpstream(0);
    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              // One token per 400ms, so everything after the first must queue
              // and be admitted by the OTHER replica via
              // `requestReady:<ownerId>` rather than an in-process handover.
              name: "mr3",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 1,
                tokensToAdd: 1,
                interval: 400,
              },
            },
          ],
          redisDb: 14,
          keyPrefix: "mr3",
        },
        async ({ workersFor }) => {
          const client = "mr3:_:a";
          const worker = workersFor(client)[0];

          const started = Date.now();
          const results = await Promise.all([
            fire(worker, client),
            fire(worker, client),
            fire(worker, client),
          ]);
          const elapsed = Date.now() - started;

          expect(results.map((r) => r.status)).toEqual([200, 200, 200]);
          expect(upstream.servedCount()).toBe(3);
          // Three requests at one per 400ms cannot finish sooner, so the pacing
          // is the shared budget rather than three independent local ones.
          expect(elapsed).toBeGreaterThan(700);
          // And an UPPER bound, which is what gives this test teeth. With only
          // the lower bound, disabling `scheduleDrain` entirely still passed:
          // the requests fell back to the 10s health tick and finished in 20s,
          // which is greater than 700. The honest window here is ~800ms.
          expect(elapsed).toBeLessThan(2000);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  it("meters both replicas against one budget", async () => {
    const upstream = await startUpstream(0);
    try {
      await withReplicas(
        {
          count: 2,
          baseURL: upstream.baseURL,
          clients: [
            {
              name: "mr4",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 4,
                tokensToAdd: 4,
                interval: 60_000,
              },
            },
          ],
          redisDb: 14,
          keyPrefix: "mr4",
        },
        async ({ replicas }) => {
          const client = "mr4:_:a";
          // A refill a minute away, so the balance only moves by what is spent.
          // Two of four tokens spent through replica 0, then the budget is read
          // from replica 1: one shared bucket shows 2 left. Per-replica buckets
          // would show replica 1 still holding all 4 — the falsifiable half.
          await fire(replicas[0], client);
          await fire(replicas[0], client);

          const seenByPeer = await replicas[1].getClientStats(client);
          expect(seenByPeer.rateLimit).toMatchObject({ tokens: 2 });
          expect(upstream.servedCount()).toBe(2);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);
});
