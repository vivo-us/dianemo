import { fireMany, harnesses, startUpstream, withHandler } from "./harness.js";
import type { DianemoBackend } from "../../packages/core/src/backend/types.js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import RequestHandler from "../../packages/core/src/index.js";
import { createServer, type ServerResponse } from "node:http";
import type { Harness } from "./harness.js";

/**
 * `concurrencyLimit` — the outcomes the happy-path suite does not reach.
 *
 * `concurrencyLimit.test.ts` proves the cap holds while a backlog of 200s
 * drains. That is the easy half. A slot is claimed during admission and given
 * back on a `requestDone` broadcast, so every way a request can *stop* — a 4xx,
 * a retry, a freeze, an abort, an admission timeout, a socket that dies, a
 * response that never comes, a shutdown — is a separate opportunity to hand the
 * slot back twice, or never. This file walks those paths and asserts, for each
 * one, both what the caller receives and what the slot ledger looks like
 * afterwards.
 *
 * Two of these started life pinning bugs this file found — a rebuild wiping the
 * fleet-wide slot ledger out from under in-flight requests, and an aborted
 * request announcing its abandonment twice. Both are fixed; the tests now assert
 * the correct behaviour and stand as the regression guards.
 */

const KEY = "0123456789abcdef0123456789abcdef";
const PREFIX = "cs";

// ---------------------------------------------------------------- upstream

type Route = (res: ServerResponse) => void | Promise<void>;

/** Sentinel: hold the request open until `releaseHeld()`. */
const HOLD: Route = () => {};

/**
 * An upstream whose reply is chosen per path.
 *
 * `startUpstream` in the shared harness only ever answers 200, and what a slot
 * does on every *other* answer is the whole point here. Occupancy is counted at
 * this end, where a breach is what the vendor would actually see, and in cost
 * units as well as request counts — a limiter that forgot cost weighting would
 * keep request occupancy under the cap while doubling the real load.
 */
async function startScriptedUpstream() {
  const routes = new Map<string, Route>();
  const holders = new Set<() => void>();
  const arrivals: string[] = [];
  const hits = new Map<string, number>();
  let inFlight = 0;
  let peakInFlight = 0;
  let costInFlight = 0;
  let peakCostInFlight = 0;

  const server = createServer(async (req, res) => {
    const path = (req.url ?? "/").split("?")[0];
    // Requests carry their own cost so occupancy can be measured the way the
    // limiter meters it.
    const cost = Number(req.headers["x-cost"] ?? 1) || 1;
    inFlight++;
    costInFlight += cost;
    peakInFlight = Math.max(peakInFlight, inFlight);
    peakCostInFlight = Math.max(peakCostInFlight, costInFlight);
    arrivals.push(path);
    hits.set(path, (hits.get(path) ?? 0) + 1);
    try {
      const route = routes.get(path);
      if (route === HOLD) {
        await new Promise<void>((resolve) => {
          const done = () => {
            holders.delete(done);
            resolve();
          };
          holders.add(done);
        });
        if (!res.writableEnded) {
          res.writeHead(200, { "content-type": "application/json" });
          res.end('{"ok":true}');
        }
      } else if (route) {
        await route(res);
      } else {
        res.writeHead(200, { "content-type": "application/json" });
        res.end('{"ok":true}');
      }
    } finally {
      inFlight--;
      costInFlight -= cost;
    }
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;

  return {
    baseURL: `http://127.0.0.1:${port}`,
    route: (path: string, handler: Route) => routes.set(path, handler),
    peakInFlight: () => peakInFlight,
    peakCostInFlight: () => peakCostInFlight,
    inFlight: () => inFlight,
    arrivals: () => [...arrivals],
    hits: (path: string) => hits.get(path) ?? 0,
    resetPeaks: () => {
      peakInFlight = 0;
      peakCostInFlight = 0;
    },
    /** Lets every held request finish, so the in-flight count comes back down. */
    releaseHeld: () => {
      for (const done of [...holders]) done();
    },
    close: async () => {
      for (const done of [...holders]) done();
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

const reply =
  (status: number, delayMs = 0, body = '{"ok":true}'): Route =>
  async (res) => {
    if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
    res.writeHead(status, { "content-type": "application/json" });
    res.end(body);
  };

/** Kills the socket mid-request, which axios reports as ECONNRESET. */
const killSocket = (): Route => (res) => {
  res.socket?.destroy();
};

// ------------------------------------------------------------------ rig

/**
 * `withHandler` fixes the client shape, so it cannot express retry options, a
 * slot TTL, an admission timeout or OAuth2 grants — and every one of those is a
 * scenario below. Same rig, with the client definition handed in whole.
 */
async function withClient(
  harness: Harness,
  clientData: Record<string, unknown>,
  fn: (
    handler: RequestHandler,
    backend: DianemoBackend,
    clientName: string
  ) => Promise<void>
) {
  const { backend, cleanup } = await harness.create();
  const handler = new RequestHandler({ key: KEY, backend, keyPrefix: PREFIX });
  const clientName = "cs:_:a";

  await handler.registerClientTemplate(
    "cs" as never,
    (() => [{ ...clientData, name: clientName }]) as never
  );
  await handler.start();
  await handler.addTemplateClient(
    "cs" as never,
    {
      instanceId: "a",
    } as never
  );

  try {
    await fn(handler, backend, clientName);
  } finally {
    await handler.stop().catch(() => undefined);
    await cleanup().catch(() => undefined);
  }
}

const SLOTS = `${PREFIX}:requestHandler:cs:_:a:concurrency:default`;
const grantSlots = (grantId: string) =>
  `${PREFIX}:requestHandler:cs:_:a:grant:${grantId}:concurrency:default`;

const occupancy = async (backend: DianemoBackend, key = SLOTS) =>
  (await backend.getConcurrencyState(key, 600_000)).currentConcurrency;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Occupancy once the ledger settles, for asserting that slots came back.
 *
 * A slot is released on the `requestDone` broadcast, which on Redis is a real
 * round trip over a second connection. Sleeping a fixed amount and reading once
 * measures the machine as much as the code — it flaked here at 250 ms under
 * load. Waiting for the figure keeps the assertion's teeth (a slot that is
 * never released never arrives, and the read below still fails) while giving up
 * orders of magnitude sooner than any real leak would resolve.
 *
 * Only for the "it came back" direction. Asserting that a slot is *held* is a
 * claim about a stable state and stays a direct read, since polling for it
 * could pass on a transient.
 */
const occupancyReaches = async (
  backend: DianemoBackend,
  expected: number,
  key = SLOTS,
  timeoutMs = 5_000
) => {
  const deadline = Date.now() + timeoutMs;
  let seen = await occupancy(backend, key);
  while (seen !== expected && Date.now() < deadline) {
    await sleep(25);
    seen = await occupancy(backend, key);
  }
  return seen;
};

const CLIENT_FREEZE = `${PREFIX}:requestHandler:cs:_:a:freezeState`;
const grantFreeze = (grantId: string) =>
  `${PREFIX}:requestHandler:cs:_:a:grant:${grantId}:freezeState`;

/**
 * Polls until `check` holds, so a pub/sub round trip is not a race.
 *
 * A freeze is armed by the CONTROLLER, from the `requestDone` broadcast — which
 * on Redis is a real round trip over a second connection. The caller's rejection
 * therefore says nothing about whether the fleet has registered the 429 yet, and
 * anything that measures behaviour "during the freeze" has to wait for it.
 */
const waitUntil = async (check: () => Promise<boolean>, timeoutMs = 5_000) => {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await check()) return true;
    if (Date.now() >= deadline) return false;
    await sleep(25);
  }
};

type Settled<T> =
  { ok: true; value: T } | { ok: false; error: Error & { code?: string } };

const settle = <T>(p: Promise<T>): Promise<Settled<T>> =>
  p.then(
    (value) => ({ ok: true as const, value }),
    (error: Error & { code?: string }) => ({ ok: false as const, error })
  );

/** Records every `requestDone` payload without displacing a real subscriber. */
function spyRequestDone(backend: DianemoBackend) {
  const seen: Array<{
    requestId: string;
    responseStatus: string;
    fastPath?: boolean;
    /** The retry's own local wait, and the fleet-wide freeze. They differ. */
    waitTime?: number;
    freezeTime?: number;
  }> = [];
  const original = backend.publish.bind(backend);
  backend.publish = async (channel: string, message: string) => {
    if (channel.endsWith(":requestDone")) {
      seen.push(
        JSON.parse(message) as {
          requestId: string;
          responseStatus: string;
          fastPath?: boolean;
          waitTime?: number;
          freezeTime?: number;
        }
      );
    }
    return original(channel, message);
  };
  return seen;
}

// ------------------------------------------------------------------ suite

describe.each(harnesses(11))(
  "concurrencyLimit scenarios — $name",
  (harness) => {
    let up: Awaited<ReturnType<typeof startScriptedUpstream>>;

    beforeEach(async () => {
      up = await startScriptedUpstream();
      up.route("/ok", reply(200, 25));
      up.route("/hold", HOLD);
      up.route(
        "/token",
        reply(200, 0, '{"access_token":"t","expires_in":3600}')
      );
    });
    afterEach(async () => {
      await up.close();
    });

    /** A concurrency client pointed at the scripted upstream. */
    const spec = (
      maxConcurrency: number,
      extra: Record<string, unknown> = {}
    ) => ({
      rateLimit: { type: "concurrencyLimit", maxConcurrency },
      requestOptions: { defaults: { baseURL: up.baseURL } },
      retryOptions: { maxRetries: 0, retryBackoffBaseTime: 20 },
      ...extra,
    });

    const send = (
      handler: RequestHandler,
      clientName: string,
      url: string,
      extra: Record<string, unknown> = {}
    ) =>
      handler.handleRequest({
        clientName,
        requestName: "t.scenario",
        method: "GET",
        url,
        headers: { "x-cost": String(extra.cost ?? 1) },
        ...extra,
      });

    // ------------------------------------------------------- response outcomes

    it("200 on the fast path leaves no queue entry and no slot", async () => {
      await withClient(
        harness,
        spec(3),
        async (handler, backend, clientName) => {
          const seen = spyRequestDone(backend);
          const res = await send(handler, clientName, "/ok");
          expect(res.status).toBe(200);
          await sleep(150);

          // The fast path is the claim that an uncontended request skips the queue
          // entirely. `fastPath` on the completion event is how the controller
          // knows there is no entry to remove, so it is the thing to assert.
          expect(seen).toHaveLength(1);
          expect(seen[0].fastPath).toBe(true);
          expect(seen[0].responseStatus).toBe("success");
          expect(await occupancyReaches(backend, 0)).toBe(0);

          const stats = await handler.getClientStats(clientName);
          expect(stats.requestsInQueue.count).toBe(0);
          expect(stats.requestsInProgress.count).toBe(0);
        }
      );
    }, 30_000);

    it.each([400, 401, 404, 409])(
      "%i is not retried, surfaces to the caller, and gives the slot back",
      async (status) => {
        up.route(`/${status}`, reply(status));
        await withClient(
          harness,
          spec(3),
          async (handler, backend, clientName) => {
            const r = await settle(send(handler, clientName, `/${status}`));
            expect(r.ok).toBe(false);
            if (r.ok) return;
            // Axios' own error, unwrapped — the limiter does not swallow the status.
            expect(r.error.message).toContain(String(status));

            // Exactly one attempt. A default-retryable 4xx would be a behaviour
            // change the caller pays for at the vendor.
            expect(up.hits(`/${status}`)).toBe(1);
            await sleep(150);
            expect(await occupancyReaches(backend, 0)).toBe(0);
            const stats = await handler.getClientStats(clientName);
            expect(
              stats.requestsInQueue.count + stats.requestsInProgress.count
            ).toBe(0);
          }
        );
      },
      30_000
    );

    it("409 opted into retryStatusCodes retries and releases a slot per attempt", async () => {
      up.route("/409", reply(409));
      await withClient(
        harness,
        spec(1, {
          retryOptions: {
            maxRetries: 2,
            retryBackoffBaseTime: 20,
            retryStatusCodes: [409],
          },
        }),
        async (handler, backend, clientName) => {
          const r = await settle(send(handler, clientName, "/409"));
          expect(r.ok).toBe(false);
          // One initial attempt plus two retries.
          expect(up.hits("/409")).toBe(3);
          // A consumer-requested retry is not a statement about the upstream, so
          // it must not freeze the client for everyone else.
          const stats = await handler.getClientStats(clientName);
          expect(stats.isFrozen).toBe(false);
          await sleep(200);
          expect(await occupancyReaches(backend, 0)).toBe(0);
          expect(
            (await handler.getClientStats(clientName)).requestsInQueue.count
          ).toBe(0);
        }
      );
    }, 30_000);

    it.each([500, 502, 503])(
      "%i is retried and the slot is returned after every attempt",
      async (status) => {
        up.route(`/${status}`, reply(status));
        await withClient(
          harness,
          spec(1, {
            retryOptions: { maxRetries: 2, retryBackoffBaseTime: 20 },
          }),
          async (handler, backend, clientName) => {
            const r = await settle(send(handler, clientName, `/${status}`));
            expect(r.ok).toBe(false);
            expect(up.hits(`/${status}`)).toBe(3);
            await sleep(250);
            // Three attempts each claimed the single slot; if any attempt had
            // failed to release, the client would be permanently full.
            expect(await occupancyReaches(backend, 0)).toBe(0);
            // And it can still serve, which is what a leaked slot would prevent.
            expect((await send(handler, clientName, "/ok")).status).toBe(200);
          }
        );
      },
      30_000
    );

    it("a dead socket (ECONNRESET) is retried and never strands a slot", async () => {
      up.route("/reset", killSocket());
      await withClient(
        harness,
        spec(1, { retryOptions: { maxRetries: 1, retryBackoffBaseTime: 20 } }),
        async (handler, backend, clientName) => {
          const r = await settle(send(handler, clientName, "/reset"));
          expect(r.ok).toBe(false);
          if (r.ok) return;
          expect(r.error.code).toBe("ECONNRESET");
          expect(up.hits("/reset")).toBe(2);
          await sleep(200);
          expect(await occupancyReaches(backend, 0)).toBe(0);
          expect((await send(handler, clientName, "/ok")).status).toBe(200);
        }
      );
    }, 30_000);

    it("an axios timeout (ECONNABORTED) is retried and never strands a slot", async () => {
      up.route("/slow", reply(200, 600));
      await withClient(
        harness,
        spec(1, {
          axiosOptions: { timeout: 120 },
          retryOptions: { maxRetries: 1, retryBackoffBaseTime: 20 },
        }),
        async (handler, backend, clientName) => {
          const r = await settle(send(handler, clientName, "/slow"));
          expect(r.ok).toBe(false);
          if (r.ok) return;
          expect(r.error.code).toBe("ECONNABORTED");
          expect(up.hits("/slow")).toBe(2);
          await sleep(300);
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 30_000);

    it("a 429 freezes the client, the thaw probe recovers it, and no slot is lost", async () => {
      let n = 0;
      up.route("/mixed", async (res) => {
        n++;
        if (n === 1) {
          res.writeHead(429);
          res.end("{}");
          return;
        }
        res.writeHead(200, { "content-type": "application/json" });
        res.end('{"ok":true}');
      });
      await withClient(
        harness,
        spec(3, {
          retryOptions: {
            maxRetries: 3,
            retryBackoffBaseTime: 250,
            thawRequestCount: 3,
          },
        }),
        async (handler, backend, clientName) => {
          const inFlight = settle(send(handler, clientName, "/mixed"));
          // Mid-freeze: the retry is backing off and its slot is already back.
          await sleep(120);
          const frozen = await handler.getClientStats(clientName);
          expect(frozen.isFrozen).toBe(true);
          expect(await occupancyReaches(backend, 0)).toBe(0);

          const r = await inFlight;
          expect(r.ok).toBe(true);
          expect(up.hits("/mixed")).toBe(2);
          await sleep(250);
          // Recovered: thawing, not frozen, and the ledger is clean.
          const thawed = await handler.getClientStats(clientName);
          expect(thawed.isFrozen).toBe(false);
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 40_000);

    /**
     * `retryBackoffBaseTime: 0` means "retry with no local delay". It must NOT
     * mean "do not freeze".
     *
     * This type reads the configured back-off for the retry itself without a
     * floor, so a zero is honoured there — but the freeze derives from the same
     * field, and `getFreezeBaseTime` in the base class floors it at 1000ms. A
     * zero-length freeze is read as no freeze at all by the arming gate, so
     * without that floor a client configured this way got no fleet-wide pacing
     * from a 429 at all: every other replica kept sending into a closed door,
     * and the operator had no way to see it from the setting's name.
     *
     * Both halves are asserted, and the second is the one more likely to break.
     * A test that only pinned the freeze duration would stay green if someone
     * "fixed the inconsistency" by extending the floor to the retry path too —
     * which looks like tidying, and would silently add a second of latency to
     * every retry on a client that asked for none. The pair is what
     * `docs/rate-limits/concurrency-limit.md` promises.
     *
     * The floor lives in `BaseClient`, so this also guards it for `noLimit`,
     * which inherits the same method. A per-type override would escape it.
     */
    it("a zero back-off still arms a floored freeze, without delaying the retry", async () => {
      up.route("/zero", reply(429));
      await withClient(
        harness,
        spec(3, {
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 0,
            thawRequestCount: 3,
          },
        }),
        async (handler, backend, clientName) => {
          const done = spyRequestDone(backend);
          const r = await settle(send(handler, clientName, "/zero"));
          expect(r.ok).toBe(false);

          // Armed by the controller off the broadcast, so it lands after the
          // caller's rejection.
          expect(
            await waitUntil(
              async () => (await backend.getFreezeState(CLIENT_FREEZE)) !== null
            )
          ).toBe(true);
          const state = (await backend.getFreezeState(CLIENT_FREEZE))!;
          const ahead = state.frozenUntil - (await backend.now());

          // A range, not an equality: `frozenUntil` is stamped from the
          // backend's clock when the freeze is armed and read a round trip
          // later, so the remainder is always a little under the floor —
          // measured at 988ms on Redis, 1000ms on memory. The shortfall is that
          // latency, not a second floor; there is only one, MIN_FREEZE_BASE_MS.
          expect(ahead).toBeGreaterThan(800);
          expect(ahead).toBeLessThanOrEqual(1000);
          // A 429 is the freeze that budgets probes, floor or no floor.
          expect(state.thawRequestCount).toBe(3);

          // ...and the request itself was told to wait for nothing.
          const failure = done.find((d) => d.freezeTime !== undefined);
          expect(failure?.waitTime).toBe(0);
        }
      );
    }, 40_000);

    it("a response that never arrives holds exactly its slot and releases it on completion", async () => {
      await withClient(
        harness,
        spec(2, {
          requestOptions: {
            defaults: { baseURL: up.baseURL },
            cleanupTimeout: 400,
          },
        }),
        async (handler, backend, clientName) => {
          const held = [
            settle(send(handler, clientName, "/hold")),
            settle(send(handler, clientName, "/hold")),
          ];
          // Held well past `cleanupTimeout`, and that is the whole point of the
          // number. A slot is reaped only inside an admission attempt, and only
          // when it is older than the slot TTL — so if this waited 200ms, the two
          // slots would still be younger than 400ms when the request below tries
          // to get in, and a client that (wrongly) used `cleanupTimeout` as its
          // slot TTL would behave identically. It did, and this test passed
          // anyway: 400ms of hold is what makes the reap reachable and the
          // assertions below mean what their comments say.
          await sleep(1000);
          // Both slots occupied, and no more — a hung request must not be reaped
          // early, and must not claim more than it asked for.
          expect(await occupancy(backend)).toBe(2);

          // Anything behind it waits for admission and then gives up, rather than
          // being admitted over the top of the hung pair.
          const blocked = await settle(send(handler, clientName, "/ok"));
          expect(
            blocked.ok,
            "the third request was ADMITTED over two requests that are still open at the upstream: a live slot was reaped, so the slot TTL is too short — check that it is `concurrencySlotTtl` and not `cleanupTimeout`"
          ).toBe(false);
          if (!blocked.ok) {
            expect(blocked.error.constructor.name).toBe("RequestTimeoutError");
          }
          // The timed-out request never reached the upstream.
          expect(
            up.hits("/ok"),
            "the abandoned request reached the upstream, so the cap was exceeded at the vendor"
          ).toBe(0);
          // …and the abandonment did not release someone else's slot.
          expect(await occupancy(backend)).toBe(2);

          up.releaseHeld();
          await Promise.all(held);
          await sleep(250);
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 40_000);

    // -------------------------------------------------------- admission paths

    it("a queued request is admitted the moment a slot frees", async () => {
      await withClient(
        harness,
        spec(1, {
          requestOptions: {
            defaults: { baseURL: up.baseURL },
            cleanupTimeout: 10_000,
          },
        }),
        async (handler, backend, clientName) => {
          const blocker = settle(send(handler, clientName, "/hold"));
          await sleep(150);
          const queued = settle(send(handler, clientName, "/ok"));
          await sleep(150);
          // Still waiting: the only slot is taken.
          expect(up.hits("/ok")).toBe(0);
          expect(await occupancy(backend)).toBe(1);

          up.releaseHeld();
          const r = await queued;
          expect(r.ok).toBe(true);
          expect(up.hits("/ok")).toBe(1);
          // Serialised, never overlapping.
          expect(up.peakInFlight()).toBe(1);
          await blocker;
          await sleep(200);
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 40_000);

    it("a request queued when the client freezes still completes once it thaws", async () => {
      let n = 0;
      up.route("/mixed", async (res) => {
        n++;
        if (n === 1) {
          await sleep(80);
          res.writeHead(429);
          res.end("{}");
          return;
        }
        res.writeHead(200, { "content-type": "application/json" });
        res.end('{"ok":true}');
      });
      await withClient(
        harness,
        spec(1, {
          requestOptions: {
            defaults: { baseURL: up.baseURL },
            cleanupTimeout: 10_000,
          },
          retryOptions: { maxRetries: 2, retryBackoffBaseTime: 150 },
        }),
        async (handler, backend, clientName) => {
          const first = settle(send(handler, clientName, "/mixed"));
          // Queued behind the first while the 429 lands and freezes the client.
          await sleep(20);
          const behind = [
            settle(send(handler, clientName, "/mixed")),
            settle(send(handler, clientName, "/mixed")),
          ];
          const results = await Promise.all([first, ...behind]);
          // A freeze delays queued work; it must not fail it.
          expect(results.every((r) => r.ok)).toBe(true);
          await sleep(300);
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 40_000);

    it("aborting a queued request frees its place without touching anyone's slot", async () => {
      await withClient(
        harness,
        spec(1, {
          requestOptions: {
            defaults: { baseURL: up.baseURL },
            cleanupTimeout: 10_000,
          },
        }),
        async (handler, backend, clientName) => {
          const blocker = settle(send(handler, clientName, "/hold"));
          await sleep(150);
          const ac = new AbortController();
          const queued = settle(
            send(handler, clientName, "/ok", { signal: ac.signal })
          );
          await sleep(120);
          ac.abort();

          const r = await queued;
          expect(r.ok).toBe(false);
          if (!r.ok) {
            expect(r.error.constructor.name).toBe("RequestAbortedError");
          }
          await sleep(200);
          // The blocker still owns the one slot; the abort released only the
          // queue place. Releasing a slot the aborted request never held would
          // show up here as 0.
          expect(await occupancy(backend)).toBe(1);
          expect(
            (await handler.getClientStats(clientName)).requestsInQueue.count
          ).toBe(0);
          expect(up.hits("/ok")).toBe(0);

          up.releaseHeld();
          await blocker;
          await sleep(200);
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 40_000);

    it("aborting a queued request announces the abandonment exactly once", async () => {
      // `requestDone` is a public channel, so a consumer metering completions
      // counts every event on it. `onAbort` publishes the abandonment itself and
      // then rejects, and that rejection lands in the catch that calls
      // `releaseAbandonedRequest` — so unless `onAbort` sets
      // `request.abandonmentPublished` (handleRequest.ts:250), the same abort is
      // announced twice and every aborted request is double-counted. It was, for
      // a while; this is the regression guard.
      await withClient(
        harness,
        spec(1, {
          requestOptions: {
            defaults: { baseURL: up.baseURL },
            cleanupTimeout: 10_000,
          },
        }),
        async (handler, backend, clientName) => {
          const seen = spyRequestDone(backend);
          const blocker = settle(send(handler, clientName, "/hold"));
          await sleep(150);
          const ac = new AbortController();
          const queued = settle(
            send(handler, clientName, "/ok", { signal: ac.signal })
          );
          await sleep(120);
          const before = seen.length;
          ac.abort();
          await queued;
          await sleep(200);

          const forAbort = seen.slice(before);
          expect(forAbort).toHaveLength(1);
          expect(forAbort[0].responseStatus).toBe("failure");

          up.releaseHeld();
          await blocker;
        }
      );
    }, 40_000);

    /**
     * An abort that lands DURING the enqueue, which used to kill the process.
     *
     * `waitForRequestReady` builds its wait promise — installing the abort
     * listener — and then awaits `addRequestToQueue` before returning it. Nothing
     * is attached to that promise in between, so an abort inside the window
     * rejected a promise with no handler, and Node's default
     * `--unhandled-rejections=throw` TERMINATED THE HOST PROCESS. A caller with a
     * correct try/catch died too: its own promise was never the one rejecting.
     * The window is one backend round trip wide, so it is ordinary on Redis.
     *
     * The `void readyPromise.catch(() => undefined)` sink in `handleRequest.ts` is
     * what closes it. IF YOU DELETE THAT SINK, READ THIS: the caller-facing
     * assertions below still pass, because the caller's outcome was never what
     * broke. What breaks is the process, and under vitest that surfaces as
     * "Vitest caught 1 unhandled error" beside a green test rather than as a
     * failure — which is exactly why this was invisible until someone ran the
     * library outside a test runner. Hence the rejection counter: vitest's own
     * `unhandledRejection` handler stops Node exiting, but every listener still
     * runs, so counting them here turns the half that kills a host process into
     * an ordinary assertion.
     *
     * The hook aborts from inside `addRequest` rather than racing a timer at it:
     * the window opens when that call is issued and closes when it resolves, so
     * this is the window, entered deterministically, with no artificial delay.
     */
    it("aborting while the enqueue is in flight fails the caller without an unobserved rejection", async () => {
      const unobserved: unknown[] = [];
      const onUnhandled = (reason: unknown) => unobserved.push(reason);
      process.on("unhandledRejection", onUnhandled);
      try {
        await withClient(
          harness,
          spec(1, {
            requestOptions: {
              defaults: { baseURL: up.baseURL },
              cleanupTimeout: 10_000,
            },
          }),
          async (handler, backend, clientName) => {
            const seen = spyRequestDone(backend);
            // Occupies the only slot, so the raced request has to enqueue.
            const blocker = settle(send(handler, clientName, "/hold"));
            await sleep(150);

            const ac = new AbortController();
            const realAdd = backend.addRequest.bind(backend);
            let raced = false;
            backend.addRequest = async (queueKey, prefix, request, ttl) => {
              const inFlight = realAdd(queueKey, prefix, request, ttl);
              if (!raced && request.requestName === "t.raceEnqueue") {
                raced = true;
                // ISSUED FIRST, then aborted — the real sequence, and the reason
                // the entry below is still cleaned up: the enqueue is ahead of
                // the abort's `requestDone` on the same connection, so the
                // controller's removal cannot overtake the creation. Aborting
                // before issuing it would invert that on the memory backend and
                // strand the entry, which is a different bug from this one.
                ac.abort();
                // One macrotask boundary while the enqueue is outstanding. Node
                // reports an unhandled rejection only after the microtask queue
                // drains, so on Redis the real round trip spans that boundary by
                // itself and this line changes nothing — but the memory backend
                // resolves the add without yielding, which would let a deleted
                // sink pass the rejection assertion on the no-Redis CI job.
                await new Promise((resolve) => setImmediate(resolve));
              }
              return inFlight;
            };

            const before = seen.length;
            const r = await settle(
              send(handler, clientName, "/ok", {
                requestName: "t.raceEnqueue",
                signal: ac.signal,
              })
            );
            backend.addRequest = realAdd;

            expect(raced).toBe(true);
            expect(r.ok).toBe(false);
            if (!r.ok) {
              expect(r.error.constructor.name).toBe("RequestAbortedError");
            }
            expect(up.hits("/ok")).toBe(0);
            // One announcement, as for any other abandonment.
            expect(seen.slice(before)).toHaveLength(1);

            // The entry the add created is still released, even though the caller
            // was told before it existed: the removal is ordered behind it because
            // the add is issued first on the same connection.
            await sleep(300);
            expect(
              (await handler.getClientStats(clientName)).requestsInQueue.count
            ).toBe(0);
            // The blocker keeps its slot throughout; the raced request never had one.
            expect(await occupancy(backend)).toBe(1);

            up.releaseHeld();
            await blocker;
            expect(await occupancyReaches(backend, 0)).toBe(0);
          }
        );
        // Settles after the withClient teardown, so a rejection raised during
        // shutdown is counted too.
        await sleep(50);
      } finally {
        process.off("unhandledRejection", onUnhandled);
      }

      // The half that ends a host process. Filtered to this library's own error
      // so an unrelated rejection elsewhere in the run cannot fail this test.
      const orphaned = unobserved.filter(
        (reason) =>
          (reason as { code?: string } | null)?.code === "request_aborted"
      );
      expect(orphaned).toEqual([]);
    }, 40_000);

    it("shutting the handler down fails queued work with a reason, not a timeout", async () => {
      await withClient(
        harness,
        spec(1, {
          requestOptions: {
            defaults: { baseURL: up.baseURL },
            cleanupTimeout: 30_000,
          },
        }),
        async (handler, _backend, clientName) => {
          const blocker = settle(send(handler, clientName, "/hold"));
          await sleep(150);
          const queued = settle(send(handler, clientName, "/ok"));
          await sleep(120);

          handler.setDrainTimeout(200);
          const stopping = handler.stop();
          const r = await queued;
          expect(r.ok).toBe(false);
          if (!r.ok) {
            expect(r.error.constructor.name).toBe("ClientUnavailableError");
            expect(r.error.message).toMatch(/stopped/i);
          }
          // A request already dispatched is not cancelled by shutdown.
          up.releaseHeld();
          expect((await blocker).ok).toBe(true);
          await stopping;
        }
      );
    }, 40_000);

    it("a retry hands its slot back for the length of the backoff", async () => {
      let n = 0;
      up.route("/retry", async (res) => {
        n++;
        res.writeHead(n === 1 ? 500 : 200, {
          "content-type": "application/json",
        });
        res.end('{"ok":true}');
      });
      await withClient(
        harness,
        spec(1, {
          requestOptions: {
            defaults: { baseURL: up.baseURL },
            cleanupTimeout: 10_000,
          },
          retryOptions: { maxRetries: 3, retryBackoffBaseTime: 400 },
        }),
        async (handler, backend, clientName) => {
          const retrying = settle(send(handler, clientName, "/retry"));
          // Backing off after the 500: the slot must be free, not parked.
          await sleep(150);
          expect(await occupancyReaches(backend, 0)).toBe(0);
          // …and genuinely usable by someone else, which is the point of freeing it.
          expect((await send(handler, clientName, "/ok")).status).toBe(200);

          expect((await retrying).ok).toBe(true);
          expect(up.hits("/retry")).toBe(2);
          await sleep(250);
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 40_000);

    // ------------------------------------------------------------------ cost

    it("a cost above the ceiling is refused up front rather than queued", async () => {
      await withClient(
        harness,
        spec(4),
        async (handler, backend, clientName) => {
          const r = await settle(send(handler, clientName, "/ok", { cost: 5 }));
          expect(r.ok).toBe(false);
          if (!r.ok) {
            expect(r.error.constructor.name).toBe(
              "RequestCostExceedsBudgetError"
            );
          }
          // Never dispatched, never queued, no slot taken.
          expect(up.hits("/ok")).toBe(0);
          expect(await occupancyReaches(backend, 0)).toBe(0);
          expect(
            (await handler.getClientStats(clientName)).requestsInQueue.count
          ).toBe(0);
        }
      );
    }, 30_000);

    it("a cost equal to the ceiling is admitted, one at a time", async () => {
      await withClient(
        harness,
        spec(4, {
          requestOptions: {
            defaults: { baseURL: up.baseURL },
            cleanupTimeout: 20_000,
          },
        }),
        async (handler, backend, clientName) => {
          const all = await Promise.all(
            Array.from({ length: 4 }, () =>
              settle(send(handler, clientName, "/ok", { cost: 4 }))
            )
          );
          expect(all.every((r) => r.ok)).toBe(true);
          // Each one fills the client on its own, so they can never overlap.
          expect(up.peakInFlight()).toBe(1);
          await sleep(200);
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 40_000);

    it("occupancy is metered in cost, not in requests", async () => {
      await withClient(
        harness,
        spec(4, {
          requestOptions: {
            defaults: { baseURL: up.baseURL },
            cleanupTimeout: 20_000,
          },
        }),
        async (handler, backend, clientName) => {
          const mixed = [
            ...Array.from({ length: 4 }, () =>
              settle(send(handler, clientName, "/ok", { cost: 2 }))
            ),
            ...Array.from({ length: 8 }, () =>
              settle(send(handler, clientName, "/ok", { cost: 1 }))
            ),
          ];
          const results = await Promise.all(mixed);
          expect(results.every((r) => r.ok)).toBe(true);
          // 16 cost units of work through a 4-unit client. Counting requests
          // instead of cost would let two cost-2 requests plus two cost-1
          // requests run together — 6 units — and this is what catches it.
          expect(up.peakCostInFlight()).toBeLessThanOrEqual(4);
          // And the weighting is real: a cost-2 request must exclude a third peer.
          expect(up.peakInFlight()).toBeLessThanOrEqual(4);
          await sleep(250);
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 40_000);

    // -------------------------------------------------------------- priority

    it("a higher-priority arrival is admitted ahead of queued lower-priority work", async () => {
      await withClient(
        harness,
        spec(1, {
          requestOptions: {
            defaults: { baseURL: up.baseURL },
            cleanupTimeout: 20_000,
          },
        }),
        async (handler, _backend, clientName) => {
          const blocker = settle(send(handler, clientName, "/hold"));
          await sleep(150);
          // Three at the bottom of the band (docs/concepts.md: 0–10, higher first).
          const low = [0, 1, 2].map((i) =>
            settle(send(handler, clientName, `/low${i}`, { priority: 0 }))
          );
          await sleep(80);
          // Arrives last, outranks all three.
          const high = settle(
            send(handler, clientName, "/high", { priority: 10 })
          );
          await sleep(80);

          up.releaseHeld();
          await Promise.all([blocker, high, ...low]);

          const order = up.arrivals();
          expect(order[0]).toBe("/hold");
          // Priority preempts queue order, but not a request already dispatched.
          expect(order[1]).toBe("/high");
          expect(order.slice(2).sort()).toEqual(["/low0", "/low1", "/low2"]);
        }
      );
    }, 40_000);

    // ---------------------------------------------------------------- grants

    const oauthSpec = (
      maxConcurrency: number,
      extra: Record<string, unknown> = {}
    ) =>
      spec(maxConcurrency, {
        requestOptions: {
          defaults: { baseURL: up.baseURL },
          cleanupTimeout: 20_000,
        },
        authentication: {
          type: "oauth2",
          grantType: "authorization_code",
          grantRateLimitBehavior: "isolated",
          clientId: "id",
          clientSecret: "secret",
          tokenUrl: `${up.baseURL}/token`,
        },
        ...extra,
      });

    const seedGrant = (
      handler: RequestHandler,
      clientName: string,
      grantId: string
    ) =>
      handler.setGrantTokens(clientName, grantId, {
        accessToken: "access",
        refreshToken: "refresh",
        expiresAt: Date.now() + 3_600_000,
        refreshTokenExpiresAt: Date.now() + 30 * 24 * 3_600_000,
        tokenType: "Bearer",
      });

    it("an isolated grant is capped against its own slots", async () => {
      await withClient(
        harness,
        oauthSpec(2),
        async (handler, backend, clientName) => {
          await seedGrant(handler, clientName, "ga");
          up.resetPeaks();
          const all = await Promise.all(
            Array.from({ length: 10 }, () =>
              settle(send(handler, clientName, "/ok", { grantId: "ga" }))
            )
          );
          expect(all.every((r) => r.ok)).toBe(true);
          expect(up.peakInFlight()).toBeLessThanOrEqual(2);
          await sleep(250);
          // Accounted under the grant's key, and fully released.
          expect(await occupancyReaches(backend, 0, grantSlots("ga"))).toBe(0);
          // The shared key is not where grant traffic is metered.
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 40_000);

    it("one grant saturating its slots does not stall another", async () => {
      await withClient(
        harness,
        oauthSpec(2),
        async (handler, backend, clientName) => {
          await seedGrant(handler, clientName, "ga");
          await seedGrant(handler, clientName, "gb");
          const heldA = Array.from({ length: 2 }, () =>
            settle(send(handler, clientName, "/hold", { grantId: "ga" }))
          );
          await sleep(250);
          expect(await occupancy(backend, grantSlots("ga"))).toBe(2);

          // Grant B is idle, so it goes straight through while A is full. If the
          // two shared one ledger this would block until A finished.
          const b = await settle(
            send(handler, clientName, "/ok", { grantId: "gb" })
          );
          expect(b.ok).toBe(true);
          expect(await occupancy(backend, grantSlots("ga"))).toBe(2);

          up.releaseHeld();
          await Promise.all(heldA);
          await sleep(250);
          expect(await occupancyReaches(backend, 0, grantSlots("ga"))).toBe(0);
          expect(await occupancyReaches(backend, 0, grantSlots("gb"))).toBe(0);
        }
      );
    }, 40_000);

    it("a grant frozen by a 429 does not freeze its healthy neighbours", async () => {
      up.route("/429", reply(429));
      await withClient(
        harness,
        oauthSpec(2, {
          retryOptions: { maxRetries: 0, retryBackoffBaseTime: 2_000 },
        }),
        async (handler, backend, clientName) => {
          await seedGrant(handler, clientName, "ga");
          await seedGrant(handler, clientName, "gb");

          const a = await settle(
            send(handler, clientName, "/429", { grantId: "ga" })
          );
          expect(a.ok).toBe(false);

          // Wait for the freeze to exist ANYWHERE before timing grant B. The
          // caller's rejection travels back from axios; the freeze is armed by the
          // controller from the `requestDone` broadcast, and on Redis that is a
          // round trip that lands after. Timing B without this waited on nothing:
          // with the freeze wrongly keyed client-wide, B was admitted before the
          // freeze existed and the test passed on Redis while failing on memory —
          // a guard that only bites on one backend is worse than none, because it
          // will be believed.
          const frozen = await waitUntil(async () =>
            Boolean(
              (await backend.getFreezeState(grantFreeze("ga"))) ??
              (await backend.getFreezeState(CLIENT_FREEZE))
            )
          );
          expect(frozen).toBe(true);
          // And it is grant A's freeze, not the client's. Read directly rather
          // than polled: this is a claim about which key was written, so a poll
          // could only turn a wrong answer into a slow right one.
          expect(
            await backend.getFreezeState(grantFreeze("ga"))
          ).not.toBeNull();
          expect(await backend.getFreezeState(CLIENT_FREEZE)).toBeNull();

          // A two-second freeze is on grant A. Grant B must not wait it out.
          const started = Date.now();
          const b = await settle(
            send(handler, clientName, "/ok", { grantId: "gb" })
          );
          expect(b.ok).toBe(true);
          expect(Date.now() - started).toBeLessThan(1_000);

          await sleep(200);
          expect(await occupancyReaches(backend, 0, grantSlots("ga"))).toBe(0);
          expect(await occupancyReaches(backend, 0, grantSlots("gb"))).toBe(0);
        }
      );
    }, 40_000);

    // ------------------------------------------------------- slot accounting

    it("the cap holds at the upstream through a workload of mixed failures", async () => {
      // Everything that can go wrong, going wrong, while the ceiling is watched
      // where it matters. Each of these outcomes takes a different route back
      // through `handleRequestDone`, and any one of them releasing a slot it does
      // not own is a breach.
      let i = 0;
      up.route("/mix", async (res) => {
        const k = i++ % 6;
        await sleep(25);
        if (k === 4) {
          res.socket?.destroy();
          return;
        }
        const status = [200, 400, 500, 429, 0, 404][k];
        res.writeHead(status, { "content-type": "application/json" });
        res.end('{"ok":true}');
      });
      await withClient(
        harness,
        spec(3, {
          requestOptions: {
            defaults: { baseURL: up.baseURL },
            cleanupTimeout: 30_000,
          },
          retryOptions: { maxRetries: 2, retryBackoffBaseTime: 25 },
        }),
        async (handler, backend, clientName) => {
          await Promise.all(
            Array.from({ length: 36 }, () =>
              settle(send(handler, clientName, "/mix"))
            )
          );
          expect(up.hits("/mix")).toBeGreaterThan(36); // retries really happened
          expect(up.peakInFlight()).toBeLessThanOrEqual(3);
          expect(up.peakCostInFlight()).toBeLessThanOrEqual(3);

          await sleep(500);
          expect(await occupancyReaches(backend, 0)).toBe(0);
          const stats = await handler.getClientStats(clientName);
          expect(stats.requestsInQueue.count).toBe(0);
          expect(stats.requestsInProgress.count).toBe(0);
        }
      );
    }, 60_000);

    it("the cap is what holds the peak down, not the upstream's speed", async () => {
      // A ceiling assertion is worthless unless the same workload would breach it
      // with the ceiling raised. This runs the identical backlog twice and only
      // trusts the low-cap result because the high-cap control overshoots it.
      const plain = await startUpstream(40);
      try {
        let capped = 0;
        await withHandler(
          harness,
          plain.baseURL,
          [
            {
              name: "cw",
              rateLimit: { type: "concurrencyLimit", maxConcurrency: 3 },
            },
          ],
          async (handler) => {
            plain.resetCounters();
            await fireMany(handler, "cw:_:a", 30);
            capped = plain.peakInFlight();
          },
          "cw"
        );

        let uncapped = 0;
        await withHandler(
          harness,
          plain.baseURL,
          [
            {
              name: "cw",
              rateLimit: { type: "concurrencyLimit", maxConcurrency: 25 },
            },
          ],
          async (handler) => {
            plain.resetCounters();
            await fireMany(handler, "cw:_:a", 30);
            uncapped = plain.peakInFlight();
          },
          "cw"
        );

        expect(capped).toBeLessThanOrEqual(3);
        expect(uncapped).toBeGreaterThan(3);
      } finally {
        await plain.close();
      }
    }, 60_000);

    it("a slot TTL comfortably above the response time keeps the cap intact", async () => {
      // The counterpart to the TTL breach below: sized correctly, a slow upstream
      // does not cost you the ceiling.
      await withClient(
        harness,
        spec(2, {
          requestOptions: {
            defaults: { baseURL: up.baseURL },
            cleanupTimeout: 30_000,
            concurrencySlotTtl: 30_000,
          },
        }),
        async (handler, backend, clientName) => {
          up.route("/slow", reply(200, 200));
          const all = await Promise.all(
            Array.from({ length: 8 }, () =>
              settle(send(handler, clientName, "/slow"))
            )
          );
          expect(all.every((r) => r.ok)).toBe(true);
          expect(up.peakInFlight()).toBeLessThanOrEqual(2);
          await sleep(250);
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 60_000);

    it("DOCUMENTED BREACH: a slot TTL under the response time lets the cap be exceeded", async () => {
      // docs/rate-limits/concurrency-limit.md warns that a TTL below the slowest
      // legitimate response makes the effective cap
      // `maxConcurrency × ceil(requestDuration / slotTtl)`. This proves the
      // warning is real rather than theoretical, so the doc cannot quietly drift
      // away from the code: the reaper hands out slots whose holders are still
      // talking to the upstream.
      await withClient(
        harness,
        spec(2, {
          requestOptions: {
            defaults: { baseURL: up.baseURL },
            cleanupTimeout: 30_000,
            concurrencySlotTtl: 150,
          },
        }),
        async (handler, backend, clientName) => {
          const held = Array.from({ length: 2 }, () =>
            settle(send(handler, clientName, "/hold"))
          );
          await sleep(500); // well past the 150 ms TTL
          expect(up.inFlight()).toBe(2);

          // The two live slots are now reapable, so these are admitted on top.
          const more = Array.from({ length: 2 }, () =>
            settle(send(handler, clientName, "/hold"))
          );
          await sleep(400);
          expect(up.inFlight()).toBe(4);
          expect(up.peakInFlight()).toBeGreaterThan(2);

          up.releaseHeld();
          await Promise.all([...held, ...more]);
          await sleep(250);
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 60_000);

    it("the cap survives a client rebuild mid-flight", async () => {
      // The slot ledger is a fleet-wide key; `destroy()` is a local event that
      // `resetClient` (utils/createClients.ts:193) fires for any change to a
      // client's source data — a credential rotation, a rate-limit override, a
      // redeploy shipping an edited template. Clearing the ledger there deleted
      // slots belonging to requests still talking to the upstream, here and on
      // every other replica, and the replacement client then admitted a full
      // `maxConcurrency` on top of them. `handleDestroy` now only clears on a
      // genuine removal, so the rebuild has to be invisible to the cap.
      await withClient(
        harness,
        spec(3),
        async (handler, backend, clientName) => {
          const held = Array.from({ length: 3 }, () =>
            settle(send(handler, clientName, "/hold"))
          );
          await sleep(250);
          expect(await occupancy(backend)).toBe(3);
          expect(up.inFlight()).toBe(3);

          // Any change to the client definition rebuilds it.
          await handler.registerClientTemplate(
            "cs" as never,
            (() => [
              {
                name: clientName,
                rateLimit: { type: "concurrencyLimit", maxConcurrency: 3 },
                requestOptions: { defaults: { baseURL: up.baseURL } },
                retryOptions: { maxRetries: 0, retryBackoffBaseTime: 999 },
              },
            ]) as never
          );
          await handler.addTemplateClient(
            "cs" as never,
            {
              instanceId: "a",
            } as never
          );
          await sleep(200);

          // The three in-flight requests are still counted.
          expect(up.inFlight()).toBe(3);
          expect(await occupancy(backend)).toBe(3);

          // So the replacement client has nothing to give: these must wait rather
          // than going out over the top of the ones already open.
          const more = Array.from({ length: 3 }, () =>
            settle(send(handler, clientName, "/hold"))
          );
          await sleep(400);
          expect(up.inFlight()).toBe(3);
          expect(up.peakInFlight()).toBe(3);

          // Letting the first three go admits the three that were waiting — the
          // rebuild delayed them, it did not lose them.
          up.releaseHeld();
          await Promise.all(held);
          await sleep(300);
          expect(up.inFlight()).toBe(3);
          expect(up.peakInFlight()).toBe(3);

          up.releaseHeld();
          const rest = await Promise.all(more);
          expect(rest.every((r) => r.ok)).toBe(true);
          await sleep(300);
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 60_000);

    /**
     * The double-claim window itself, made deterministic.
     *
     * A completed request's slot can be claimed a second time and then never
     * released: admission excludes a request's own id from the occupancy sum so a
     * resubmit does not compete with itself, which means a claim for an id that
     * has just released always succeeds and silently re-creates its ledger entry.
     * Nothing then releases it — the request is done, its queue entry gone, its
     * `requestDone` consumed — so the slot sits claimed until its TTL reaped it,
     * 120s on the default, and on a small cap that took the cap to zero.
     *
     * The cycle test below reaches this window by chance and, measured on this
     * machine, does not reach it at all: it passes with the guard removed on both
     * backends across 60 iterations. This one constructs the window instead. The
     * hook is not a synthetic failure mode — removing the queue entry after the
     * claim is exactly what a completion does, and the only thing being arranged
     * is that it lands inside the pass rather than beside it.
     */
    it("releases a claim for a request that completed mid-admission", async () => {
      const QUEUE = `${PREFIX}:requestHandler:cs:_:a:queue`;
      const META = `${PREFIX}:requestHandler:cs:_:a:request`;

      await withClient(
        harness,
        spec(1),
        async (handler, backend, clientName) => {
          const realAcquire = backend.acquireQueuedConcurrency!.bind(backend);
          let claimed = false;
          backend.acquireQueuedConcurrency = async (
            key,
            metadataKey,
            cost,
            requestId,
            config
          ) => {
            const result = await realAcquire(
              key,
              metadataKey,
              cost,
              requestId,
              config
            );
            if (!claimed && requestId === "ghost" && result.acquired) {
              claimed = true;
              await backend.removeRequest(QUEUE, META, "ghost");
            }
            return result;
          };

          // A queue entry with no caller behind it: the drain loop selects it,
          // claims for it, and finds it gone — the state a completion leaves.
          await backend.addRequest(QUEUE, META, {
            requestId: "ghost",
            clientName,
            requestName: "t.ghost",
            status: "pending",
            priority: 1,
            cost: 1,
            retries: 0,
            timestamp: Date.now(),
            isThawRequest: false,
            ownerId: "another-instance",
          });
          // The wake-up a real enqueue publishes.
          await backend.publish(
            `${PREFIX}:requestHandler:requestAdded`,
            JSON.stringify({ clientName })
          );
          await sleep(400);

          expect(claimed).toBe(true);
          // The claim came back. Without the release it stays until the slot TTL,
          // and with maxConcurrency 1 that is the whole cap.
          expect(await occupancy(backend)).toBe(0);

          backend.acquireQueuedConcurrency = realAcquire;
          // And the cap still works, which a leaked slot would have prevented.
          const after = await settle(send(handler, clientName, "/ok"));
          expect(after.ok).toBe(true);
          expect(await occupancyReaches(backend, 0)).toBe(0);
        }
      );
    }, 30_000);

    /**
     * DELIBERATELY REPETITIVE — DO NOT CUT THE ITERATION COUNT.
     *
     * A completed request's slot could be claimed a second time and then never
     * released. The drain loop is entered from several places that are not ordered
     * against a completion — the post-freeze timer (client/index.ts:788),
     * `notifyRequestAdded`, `scheduleDrain`, the health tick — and a pass already
     * past `getNextRequest` holds metadata for a request that can finish
     * underneath it. Admission excludes a request's own id from the occupancy sum
     * so a resubmit does not compete with itself, which means a claim for an id
     * that has just released *always* succeeds and silently re-creates its entry.
     * Nothing then releases it: the request is done, its queue entry is gone, its
     * `requestDone` already consumed. The slot sat claimed until its TTL reaped it
     * — 120 s on the default — so the cap ratcheted down, and on a small cap went
     * to zero and wedged the client until the TTL lapsed.
     *
     * Measured at 7 leaks / 40 cycles on Redis and 0 / 40 on memory before the
     * fix, so a single cycle catches it about one time in six and one iteration
     * would be a coin toss. Twenty gives ~98% on Redis. Three numbers here are
     * load-bearing and are not padding:
     *
     *   - the iteration count, for that detection probability;
     *   - `retryBackoffBaseTime: 250`, because it makes the retry re-enter
     *     admission at the same moment the post-freeze timer fires, which is the
     *     collision being hunted;
     *   - a fresh handler per iteration, which is the shape the rate was measured
     *     in.
     *
     * Each iteration asserts the cycle was real — one 429 then one success — so
     * this cannot pass by quietly failing to rate-limit at all.
     */
    it("never strands a slot across repeated 429-and-recover cycles", async () => {
      const ITERATIONS = 20;
      let realCycles = 0;

      for (let i = 0; i < ITERATIONS; i++) {
        const path = `/cycle${i}`;
        let hit = 0;
        up.route(path, async (res) => {
          hit++;
          if (hit === 1) {
            res.writeHead(429);
            res.end("{}");
            return;
          }
          res.writeHead(200, { "content-type": "application/json" });
          res.end('{"ok":true}');
        });

        await withClient(
          harness,
          spec(3, {
            retryOptions: {
              maxRetries: 3,
              retryBackoffBaseTime: 250,
              thawRequestCount: 3,
            },
          }),
          async (handler, backend, clientName) => {
            const r = await settle(send(handler, clientName, path));
            // Named in the failure so a leak points at its own iteration.
            expect(r.ok, `iteration ${i} did not succeed`).toBe(true);
            expect(up.hits(path), `iteration ${i} was not a 429-recover`).toBe(
              2
            );
            realCycles++;
            expect(
              await occupancyReaches(backend, 0),
              `iteration ${i} stranded a slot`
            ).toBe(0);
          }
        );
      }

      expect(realCycles).toBe(ITERATIONS);
    }, 180_000);
  }
);
