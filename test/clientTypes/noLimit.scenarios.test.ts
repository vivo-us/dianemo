import type { DianemoBackend } from "../../packages/core/src/backend/types.js";
import { redisBackend } from "../../packages/backend-redis/src/index.js";
import RequestHandler from "../../packages/core/src/index.js";
import { harnesses, type Harness } from "./harness.js";
import { describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import {
  ClientUnavailableError,
  ConfigurationError,
  RequestAbortedError,
  RequestTimeoutError,
} from "../../packages/core/src/errors.js";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";

/**
 * `noLimit` — everything that happens when the vendor pushes back.
 *
 * `noLimit.test.ts` covers the claim the type is named for: no budget, no
 * queue, no coordination. This file covers the other half, which is where the
 * type actually has the most surface. "No configured limit" is not "no limit
 * the vendor told us about", so a `noLimit` client still freezes on a 429 or an
 * upstream failure, still queues everything that arrives during that freeze,
 * still probes recovery one request at a time, and still has to fail a backlog
 * cleanly on shutdown. Those paths are exercised almost exclusively by this
 * type, because it is the only one that spends most of its life bypassing them.
 *
 * Every freeze here is confirmed present in the backend before the test fires
 * anything at it. A burst issued while the 429 is still in flight proves
 * nothing: the freeze it is supposed to be blocked by does not exist yet.
 */

const REDIS_URL = process.env.REDIS_URL;
const KEY = "0123456789abcdef0123456789abcdef";

/**
 * Bounds the wait for admission in the tests that deliberately queue behind a
 * freeze.
 *
 * Every legitimate wait in them is under three seconds, so this only fires when
 * a wake-up was genuinely lost — and then the request fails with a
 * `RequestTimeoutError` naming it, rather than sitting on the default 60s while
 * the test's own budget runs out first. A hang says nothing about what broke;
 * this says which request was never admitted.
 */
const QUEUED_ADMISSION_TIMEOUT_MS = 12_000;

// ---------------------------------------------------------------- upstream

/** What the upstream should do with one request. */
interface Plan {
  status?: number;
  delayMs?: number;
  /** Never answer, and hold the socket open. */
  hang?: boolean;
  /** Destroy the socket mid-request — ECONNRESET at the client. */
  destroy?: boolean;
}

interface Arrival {
  tag: string;
  /** How many requests the upstream had open when this one arrived. */
  inFlight: number;
  /** `performance.now()` on arrival, for tests that measure the gap between two. */
  at: number;
}

/**
 * An upstream that answers each request however the test says, and records the
 * order and the overlap of what reaches it.
 *
 * The shared harness's `startUpstream` always returns 200, and every outcome
 * below turns on the status; `inFlight` per arrival is what proves single-flight
 * probing, which a peak alone cannot once the probe budget runs out.
 */
async function startScriptedUpstream() {
  const arrivals: Arrival[] = [];
  let plan: (tag: string, n: number) => Plan = () => ({ status: 200 });
  let inFlight = 0;
  let peak = 0;
  let served = 0;
  const held = new Set<ServerResponse>();

  const server = createServer(
    async (req: IncomingMessage, res: ServerResponse) => {
      const tag = new URL(req.url ?? "/", "http://x").searchParams.get("tag");
      const answer = plan(tag ?? "", served + 1);
      served++;
      inFlight++;
      peak = Math.max(peak, inFlight);
      arrivals.push({ tag: tag ?? "", inFlight, at: performance.now() });
      held.add(res);
      if (answer.hang) return;
      if (answer.destroy) {
        inFlight--;
        held.delete(res);
        req.socket.destroy();
        return;
      }
      if (answer.delayMs)
        await new Promise((r) => setTimeout(r, answer.delayMs));
      inFlight--;
      held.delete(res);
      res.writeHead(answer.status ?? 200, {
        "Content-Type": "application/json",
      });
      res.end('{"ok":true}');
    }
  );
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;

  return {
    baseURL: `http://127.0.0.1:${port}`,
    setPlan: (next: (tag: string, n: number) => Plan) => {
      plan = next;
    },
    /** Every request the upstream has seen, in the order it saw them. */
    arrivals,
    tags: () => arrivals.map((a) => a.tag),
    servedCount: () => served,
    peakInFlight: () => peak,
    reset: () => {
      arrivals.length = 0;
      served = 0;
      peak = 0;
    },
    close: async () => {
      for (const res of held) res.destroy();
      held.clear();
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

type Upstream = Awaited<ReturnType<typeof startScriptedUpstream>>;

// ----------------------------------------------------------------- handler

interface NoLimitContext {
  handler: RequestHandler;
  backend: DianemoBackend;
  client: string;
  queueKey: string;
  metaPrefix: string;
  freezeKey: string;
}

/**
 * A handler with one `noLimit` client.
 *
 * `withHandler` fixes `retryOptions`, `requestOptions` and `authentication`,
 * and the freeze paths turn on exactly those: how long the back-off is (which
 * is also how long the freeze lasts), how many probes recovery takes, how long
 * a queued request waits for admission, and whether grants are isolated.
 */
async function withNoLimit(
  harness: Harness,
  options: {
    baseURL: string;
    keyPrefix: string;
    retryOptions?: Record<string, unknown>;
    requestOptions?: Record<string, unknown>;
    authentication?: Record<string, unknown>;
    drainTimeoutMs?: number;
  },
  fn: (context: NoLimitContext) => Promise<void>
) {
  const { backend, cleanup } = await harness.create();
  const handler = new RequestHandler({
    key: KEY,
    backend,
    keyPrefix: options.keyPrefix,
  });
  if (options.drainTimeoutMs !== undefined) {
    handler.setDrainTimeout(options.drainTimeoutMs);
  }
  await handler.registerClientTemplate(
    "nl" as never,
    ((creds: { instanceId: string }) => [
      {
        name: `nl:_:${creds.instanceId}`,
        rateLimit: { type: "noLimit" },
        ...(options.retryOptions ? { retryOptions: options.retryOptions } : {}),
        ...(options.authentication
          ? { authentication: options.authentication }
          : {}),
        requestOptions: {
          ...options.requestOptions,
          defaults: { baseURL: options.baseURL },
        },
      },
    ]) as never
  );
  await handler.start();
  await handler.addTemplateClient("nl" as never, { instanceId: "a" } as never);
  const base = `${options.keyPrefix}:requestHandler:nl:_:a`;

  try {
    await fn({
      handler,
      backend,
      client: "nl:_:a",
      queueKey: `${base}:queue`,
      metaPrefix: `${base}:request`,
      freezeKey: `${base}:freezeState`,
    });
  } finally {
    await handler.stop();
    await cleanup();
  }
}

/** One request, tagged so the upstream's arrival log can identify it. */
function send(
  handler: RequestHandler,
  clientName: string,
  tag: string,
  extra: Record<string, unknown> = {}
) {
  return handler.handleRequest({
    clientName,
    requestName: `t.${tag}`,
    method: "GET",
    url: `/?tag=${encodeURIComponent(tag)}`,
    ...extra,
  });
}

/** Outcome of a request, so a test can assert on a rejection without try/catch. */
type Settled =
  | { ok: true; status: number }
  | {
      ok: false;
      error: Error & { code?: string; response?: { status: number } };
    };

const settle = (p: Promise<{ status: number }>): Promise<Settled> =>
  p.then(
    (value) => ({ ok: true as const, status: value.status }),
    (error: Error) => ({ ok: false as const, error })
  );

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitFor(
  predicate: () => Promise<boolean> | boolean,
  timeoutMs = 8000
) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return true;
    await sleep(10);
  }
  return false;
}

/**
 * Fires one 429 and returns once the freeze is *in the backend*.
 *
 * The freeze is applied by the controller off a `requestDone` broadcast, so it
 * lands strictly after the caller's promise settles. Anything fired before this
 * resolves is racing the freeze rather than being blocked by it.
 */
async function freezeConfirmed(
  context: NoLimitContext,
  upstream: Upstream,
  tag = "trigger"
) {
  const triggered = await settle(send(context.handler, context.client, tag));
  expect(triggered.ok).toBe(false);
  const armed = await waitFor(
    async () =>
      (await context.backend.getFreezeState(context.freezeKey)) !== null
  );
  expect(armed).toBe(true);
  upstream.reset();
  return (await context.backend.getFreezeState(context.freezeKey))!;
}

/**
 * Records the deadline every freeze arm actually WROTE, which is not always the
 * one it asked for — `armFreeze` keeps the longer of the two.
 */
function tapFreezeArms(backend: DianemoBackend) {
  const written: number[] = [];
  const original = backend.armFreeze.bind(backend);
  backend.armFreeze = async (
    key: string,
    frozenUntil: number,
    thawRequestCount: number
  ) => {
    const state = await original(key, frozenUntil, thawRequestCount);
    written.push(state.frozenUntil);
    return state;
  };
  return written;
}

/** Records every `requestDone` this process publishes, without subscribing. */
function tapRequestDone(backend: DianemoBackend) {
  const perRequest = new Map<string, number>();
  const original = backend.publish.bind(backend);
  backend.publish = async (channel: string, message: string) => {
    if (channel.endsWith(":requestDone")) {
      const id = String(
        (JSON.parse(message) as { requestId?: string }).requestId
      );
      perRequest.set(id, (perRequest.get(id) ?? 0) + 1);
    }
    return original(channel, message);
  };
  return perRequest;
}

// -------------------------------------------------------------------- tests

describe.each(harnesses(13))("noLimit scenarios — $name", (harness) => {
  // ------------------------------------------------------ response outcomes

  it("surfaces a non-retryable status to the caller, once, with no freeze", async () => {
    const upstream = await startScriptedUpstream();
    try {
      const statuses: Record<string, number> = {
        bad: 400,
        unauthorized: 401,
        missing: 404,
        conflict: 409,
      };
      upstream.setPlan((tag) => ({ status: statuses[tag] ?? 200 }));
      await withNoLimit(
        harness,
        { baseURL: upstream.baseURL, keyPrefix: "nls1" },
        async (context) => {
          for (const [tag, status] of Object.entries(statuses)) {
            const result = await settle(
              send(context.handler, context.client, tag)
            );
            expect(result.ok).toBe(false);
            if (result.ok) return;
            expect(result.error.response?.status).toBe(status);
          }
          // One attempt each: none of these is retryable by default, and a
          // retry that leaked through would show up here as an extra request.
          expect(upstream.servedCount()).toBe(4);
          expect(upstream.tags()).toEqual([
            "bad",
            "unauthorized",
            "missing",
            "conflict",
          ]);
          await sleep(150);
          expect(
            await context.backend.getFreezeState(context.freezeKey)
          ).toBeNull();
          expect(
            await context.backend.getQueueStats(
              context.queueKey,
              context.metaPrefix
            )
          ).toMatchObject({ pending: 0, inProgress: 0 });
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  it("retries a 409 when retryStatusCodes opts in, without freezing or queueing", async () => {
    const upstream = await startScriptedUpstream();
    try {
      let attempts = 0;
      upstream.setPlan(() => {
        attempts++;
        return { status: attempts < 3 ? 409 : 200 };
      });
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls2",
          retryOptions: {
            maxRetries: 3,
            retryBackoffBaseTime: 60,
            retryBackoffMethod: "linear",
            retryStatusCodes: [409],
          },
        },
        async (context) => {
          const result = await settle(
            send(context.handler, context.client, "flappy")
          );
          expect(result).toMatchObject({ ok: true, status: 200 });
          expect(attempts).toBe(3);
          await sleep(150);
          // A consumer-requested retry says nothing about anyone else, so it
          // must not freeze the client — and with nothing queued, every attempt
          // stays on the fast path.
          expect(
            await context.backend.getFreezeState(context.freezeKey)
          ).toBeNull();
          expect(
            await context.backend.getQueueStats(
              context.queueKey,
              context.metaPrefix
            )
          ).toMatchObject({ pending: 0, inProgress: 0 });
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  it("arms the freeze on a 429 that lands on the final permitted attempt", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan(() => ({ status: 429 }));
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls3",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 1200,
            retryBackoffMethod: "linear",
            thawRequestCount: 4,
          },
        },
        async (context) => {
          const result = await settle(
            send(context.handler, context.client, "rl")
          );
          expect(result.ok).toBe(false);
          if (result.ok) return;
          expect(result.error.response?.status).toBe(429);
          expect(
            await waitFor(
              async () =>
                (await context.backend.getFreezeState(context.freezeKey)) !==
                null
            )
          ).toBe(true);
          const state = await context.backend.getFreezeState(context.freezeKey);
          // A 429 is the one freeze that budgets probes: recovery is single
          // -flight until `thawRequestCount` of them have succeeded.
          expect(state?.thawRequestCount).toBe(4);
          expect(state!.frozenUntil).toBeGreaterThan(
            await context.backend.now()
          );
          // Nothing was queued to arm it — the fast path leaves no entry, and a
          // freeze that depended on one would never arm for this type at all.
          expect(
            await context.backend.getQueueStats(
              context.queueKey,
              context.metaPrefix
            )
          ).toMatchObject({ pending: 0, inProgress: 0 });
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  it("freezes on 500, 502 and 503, with no probe budget", async () => {
    for (const status of [500, 502, 503]) {
      const upstream = await startScriptedUpstream();
      try {
        upstream.setPlan(() => ({ status }));
        const announcements: string[] = [];
        await withNoLimit(
          harness,
          {
            baseURL: upstream.baseURL,
            keyPrefix: `nls4${status}`,
            retryOptions: {
              maxRetries: 0,
              retryBackoffBaseTime: 1000,
              retryBackoffMethod: "linear",
            },
          },
          async (context) => {
            const original = context.backend.publish.bind(context.backend);
            context.backend.publish = async (channel, message) => {
              if (channel.endsWith(":freezeStateChanged")) {
                announcements.push(message);
              }
              return original(channel, message);
            };
            const result = await settle(
              send(context.handler, context.client, "boom")
            );
            expect(result.ok).toBe(false);
            expect(
              await waitFor(
                async () =>
                  (await context.backend.getFreezeState(context.freezeKey)) !==
                  null
              )
            ).toBe(true);
            const state = await context.backend.getFreezeState(
              context.freezeKey
            );
            // Not a rate-limit response, so no probe budget — the freeze simply
            // lapses. Losing this distinction would silently turn every 5xx
            // into a single-flight recovery, or every 429 into a stampede.
            expect(state?.thawRequestCount).toBe(0);
            // A 5xx is the downtime signal a host opens a window on; a 429 is
            // not, and does not announce.
            expect(announcements).toHaveLength(1);
            expect(JSON.parse(announcements[0])).toMatchObject({
              clientName: "nl:_:a",
              state: "frozen",
              reason: "UPSTREAM_UNAVAILABLE",
            });
          }
        );
      } finally {
        await upstream.close();
      }
    }
  }, 60_000);

  it("freezes on a socket-level failure", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan(() => ({ destroy: true }));
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls5",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 1000,
            retryBackoffMethod: "linear",
          },
        },
        async (context) => {
          const result = await settle(
            send(context.handler, context.client, "reset")
          );
          expect(result.ok).toBe(false);
          if (result.ok) return;
          expect(result.error.code).toBe("ECONNRESET");
          expect(
            await waitFor(
              async () =>
                (await context.backend.getFreezeState(context.freezeKey)) !==
                null
            )
          ).toBe(true);
          expect(
            (await context.backend.getFreezeState(context.freezeKey))
              ?.thawRequestCount
          ).toBe(0);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  it("leaves an upstream that never answers to the caller's own timeout", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan(() => ({ hang: true }));
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls6",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 1000,
            retryBackoffMethod: "linear",
          },
          // Admission-only. It bounds the wait for a *turn*, not the request.
          requestOptions: { cleanupTimeout: 300 },
        },
        async (context) => {
          let settled = false;
          const hanging = settle(
            send(context.handler, context.client, "hang")
          ).then((result) => {
            settled = true;
            return result;
          });
          await sleep(900);
          // Three times `cleanupTimeout` later it is still open: an admission
          // timeout cannot fire for a request that was never waiting to be
          // admitted. Callers who need a ceiling on the call itself pass one to
          // axios — which the next assertion shows does work.
          expect(settled).toBe(false);
          expect(
            await context.backend.getFreezeState(context.freezeKey)
          ).toBeNull();

          const timedOut = await settle(
            send(context.handler, context.client, "hang2", { timeout: 250 })
          );
          expect(timedOut.ok).toBe(false);
          if (timedOut.ok) return;
          expect(timedOut.error.code).toBe("ECONNABORTED");
          // ECONNABORTED is an upstream failure like any other, so it freezes.
          expect(
            await waitFor(
              async () =>
                (await context.backend.getFreezeState(context.freezeKey)) !==
                null
            )
          ).toBe(true);

          await upstream.close();
          await hanging;
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  // ----------------------------------------------------------- the freeze gate

  it("sends nothing upstream while a confirmed freeze stands", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan((tag) =>
        tag === "trigger" ? { status: 429 } : { status: 200 }
      );
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls7",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 2000,
            retryBackoffMethod: "linear",
            thawRequestCount: 1,
          },
          requestOptions: { cleanupTimeout: QUEUED_ADMISSION_TIMEOUT_MS },
        },
        async (context) => {
          await freezeConfirmed(context, upstream);

          const burst = Array.from({ length: 40 }, (_, i) =>
            settle(send(context.handler, context.client, `b${i}`))
          );
          await waitFor(
            async () =>
              (await context.backend.getQueueLength(context.queueKey)) === 40
          );
          await sleep(250);
          // The whole point of the type's one round-trip: forty requests, no
          // budget of our own, and not one of them reaches the vendor.
          expect(upstream.servedCount()).toBe(0);
          expect(
            await context.backend.getQueueStats(
              context.queueKey,
              context.metaPrefix
            )
          ).toMatchObject({ pending: 40, inProgress: 0 });

          const results = await Promise.all(burst);
          expect(results.filter((r) => r.ok)).toHaveLength(40);
          expect(upstream.servedCount()).toBe(40);
          // The caller's promise resolves as soon as the response arrives; the
          // controller removes the entry off the broadcast that follows, so the
          // queue empties a moment later.
          expect(
            await waitFor(
              async () =>
                (await context.backend.getQueueLength(context.queueKey)) === 0
            )
          ).toBe(true);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 40_000);

  /**
   * The retry waits out its own back-off, even before the fleet-wide freeze has
   * landed.
   *
   * The local wait used to be skipped whenever `freezeClient` was set, on the
   * reasoning that the client-wide freeze would enforce the delay instead. It
   * cannot: the freeze is applied by the controller in response to a `requestDone`
   * broadcast, and the retry loop re-enters admission immediately, so on Redis the
   * one request the vendor had just refused went straight back out — measured at
   * 1-2ms against a configured 4000ms. Memory happened to be safe because its
   * "broadcast" is a synchronous emit.
   *
   * `armFreeze` is delayed here on purpose, and the delay is what makes this test
   * mean anything. The harness runs the controller in the SAME process as the
   * caller, which collapses the round trip to well under a millisecond, so
   * whether the freeze or the retry wins is decided by scheduler noise: written
   * without the delay, this test failed on Redis in isolation and passed inside
   * the full file, which is the signature of a test that measures the machine.
   * Holding the arm past the back-off removes the race entirely — for the length
   * of `ARM_DELAY` the freeze provably does not exist, so nothing but the local
   * wait can account for the gap.
   */
  it("waits out its own back-off before the freeze has even landed", async () => {
    const upstream = await startScriptedUpstream();
    const backoff = 1200;
    // Comfortably longer than the back-off: the freeze cannot be what delayed
    // the retry, because it is not in the backend yet when the retry goes out.
    const ARM_DELAY = 2500;
    try {
      upstream.setPlan((_tag, n) =>
        n === 1 ? { status: 429 } : { status: 200 }
      );
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nlgap",
          retryOptions: {
            maxRetries: 1,
            retryBackoffBaseTime: backoff,
            retryBackoffMethod: "linear",
            thawRequestCount: 1,
          },
        },
        async (context) => {
          const armFreeze = context.backend.armFreeze.bind(context.backend);
          context.backend.armFreeze = async (key, frozenUntil, thawCount) => {
            await sleep(ARM_DELAY);
            return armFreeze(key, frozenUntil, thawCount);
          };

          const result = await settle(
            send(context.handler, context.client, "gap")
          );
          expect(result.ok).toBe(true);
          expect(upstream.arrivals).toHaveLength(2);
          const gap = upstream.arrivals[1].at - upstream.arrivals[0].at;
          // Not ~1ms: the retry served its own sentence.
          expect(gap).toBeGreaterThanOrEqual(backoff - 50);
          // And it did not additionally wait for the freeze, which had not
          // arrived — so this is the local wait, measured alone.
          expect(gap).toBeLessThan(ARM_DELAY);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 40_000);

  /**
   * A freeze is never shortened by a failure that arms after it.
   *
   * The back-off multiplier comes from the failing request's own retry count, so
   * two requests in flight at different depths carry different freeze durations —
   * and the freeze belongs to the client, not to either of them. A plain write
   * let whichever `requestDone` landed second set the deadline, so a
   * first-failure arm (1x) overwrote a third-retry arm (3x) and every replica
   * read the shortened deadline. Measured here, before the fix: `frozenUntil`
   * moved BACKWARDS by 379ms on ordinary traffic. Constructed against a
   * third-retry arm, a 2988ms freeze was cut to 848ms.
   *
   * Two requests, one answered later than the other so their completions land in
   * a known order, against an upstream that fails everything: they climb the
   * retry ladder out of step, which is all it takes.
   */
  it("never shortens a standing freeze when a fresher failure arms a shorter one", async () => {
    const upstream = await startScriptedUpstream();
    try {
      // 503 rather than 429: no probe budget, so each freeze releases everything
      // at once and the two requests are genuinely in flight together.
      upstream.setPlan((tag) =>
        tag === "late" ? { status: 503, delayMs: 120 } : { status: 503 }
      );
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nlmono",
          retryOptions: {
            maxRetries: 3,
            retryBackoffBaseTime: 300,
            retryBackoffMethod: "linear",
          },
        },
        async (context) => {
          const written = tapFreezeArms(context.backend);
          const early = settle(send(context.handler, context.client, "early"));
          await sleep(40);
          const late = settle(send(context.handler, context.client, "late"));
          await Promise.all([early, late]);
          await waitFor(() => written.length >= 3);

          // Enough arms to have raced at all, and not one of them moved the
          // deadline backwards.
          expect(written.length).toBeGreaterThan(2);
          for (let i = 1; i < written.length; i++) {
            expect(written[i]).toBeGreaterThanOrEqual(written[i - 1]);
          }
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  it("probes recovery with exactly one request in flight", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan((tag) =>
        tag === "trigger" ? { status: 429 } : { status: 200, delayMs: 60 }
      );
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls8",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 1500,
            retryBackoffMethod: "linear",
            // More probes than there are requests, so every one of them is a
            // probe and the whole drain has to stay single-flight.
            thawRequestCount: 20,
          },
          requestOptions: { cleanupTimeout: QUEUED_ADMISSION_TIMEOUT_MS },
        },
        async (context) => {
          await freezeConfirmed(context, upstream);
          const queued = ["q0", "q1", "q2", "q3", "q4", "q5"].map((tag) =>
            settle(send(context.handler, context.client, tag))
          );
          const results = await Promise.all(queued);

          expect(results.filter((r) => r.ok)).toHaveLength(6);
          expect(upstream.servedCount()).toBe(6);
          // The delay is load-bearing: with an instant upstream nothing would
          // ever overlap and a fleet-wide free-for-all would look identical.
          expect(upstream.peakInFlight()).toBe(1);
          expect(upstream.arrivals.map((a) => a.inFlight)).toEqual([
            1, 1, 1, 1, 1, 1,
          ]);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 40_000);

  it("keeps the freeze standing when the probe fails, and promotes the next request", async () => {
    const upstream = await startScriptedUpstream();
    try {
      // The trigger and the first probe are both refused; everything after is
      // served. The second 429 has to re-arm the freeze rather than open it.
      let refusals = 0;
      upstream.setPlan(() => {
        refusals++;
        return { status: refusals <= 2 ? 429 : 200, delayMs: 30 };
      });
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls9",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 1200,
            retryBackoffMethod: "linear",
            thawRequestCount: 3,
          },
          requestOptions: { cleanupTimeout: QUEUED_ADMISSION_TIMEOUT_MS },
        },
        async (context) => {
          const first = await freezeConfirmed(context, upstream);
          const queued = ["q0", "q1", "q2"].map((tag) =>
            settle(send(context.handler, context.client, tag))
          );
          await waitFor(
            async () =>
              (await context.backend.getQueueLength(context.queueKey)) === 3
          );

          // One probe goes out, is refused, and the freeze extends.
          const reArmed = await waitFor(async () => {
            const state = await context.backend.getFreezeState(
              context.freezeKey
            );
            return !!state && state.frozenUntil > first.frozenUntil;
          });
          expect(reArmed).toBe(true);
          expect(upstream.servedCount()).toBe(1);
          // A failed probe consumes no thaw progress.
          expect(
            (await context.backend.getFreezeState(context.freezeKey))
              ?.thawRequestCount
          ).toBe(3);

          const results = await Promise.all(queued);
          // Exactly one caller sees the refusal; the other two are served after
          // the second freeze lapses, still one at a time.
          expect(results.filter((r) => r.ok)).toHaveLength(2);
          expect(upstream.servedCount()).toBe(3);
          expect(upstream.peakInFlight()).toBe(1);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 40_000);

  it("drains the backlog before a request that arrives after the freeze lapses", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan((tag) =>
        tag === "trigger" ? { status: 429 } : { status: 200, delayMs: 30 }
      );
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls10",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 1500,
            retryBackoffMethod: "linear",
            // ONE probe, and that is the point: with a probe budget larger than
            // the backlog the freeze never clears, every drained request is a
            // thaw probe, and the fast path refuses those on its own — so the
            // queue-depth check this test is named for was never exercised, and
            // the test passed with it removed. One probe clears the freeze after
            // the first request, and the remaining four are draining with the
            // fast path fully open when `late` arrives.
            thawRequestCount: 1,
          },
          requestOptions: { cleanupTimeout: QUEUED_ADMISSION_TIMEOUT_MS },
        },
        async (context) => {
          await freezeConfirmed(context, upstream);
          const backlog: Promise<Settled>[] = [];
          // Twenty, not five: the fast path can only overtake work that is still
          // being handed out, and a five-deep backlog is dispatched faster than a
          // late arrival can be admitted at all — so a shorter queue hid the
          // defect rather than testing for it.
          const tags = Array.from({ length: 20 }, (_, i) => `q${i}`);
          for (const tag of tags) {
            backlog.push(settle(send(context.handler, context.client, tag)));
            // Arrival time is the last term of the queue score and its
            // resolution is one millisecond, so a backlog issued inside a
            // single tick has no defined order to assert on.
            await sleep(2);
          }
          // Issued the moment the freeze state is GONE, which is exactly when the
          // fast path reopens and the only moment it can overtake. Waiting for
          // the first response instead fired while the freeze was still standing,
          // so the fast path refused for that reason and the queue-depth check
          // was never the thing under test.
          // Polled tightly rather than through `waitFor`: its 10ms step is longer
          // than the drain itself on the memory backend, so a coarser poll fires
          // `late` after the backlog has already been handed out and the overtake
          // has nothing left to overtake.
          const reopened = Date.now() + 20_000;
          while (
            (await context.backend.getFreezeState(context.freezeKey)) !== null
          ) {
            expect(Date.now()).toBeLessThan(reopened);
            await sleep(1);
          }
          const late = settle(send(context.handler, context.client, "late"));

          await Promise.all([...backlog, late]);
          // The backlog is dispatched concurrently once the freeze lapses, so the
          // order WITHIN it is not defined — `serves a frozen backlog in priority
          // order` is where queue order is asserted. What matters here is that
          // nothing that arrived later was served earlier.
          const order = upstream.tags();
          expect(order).toHaveLength(21);
          expect(order.indexOf("late")).toBe(20);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 40_000);

  it("serves a frozen backlog in priority order", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan((tag) =>
        tag === "trigger" ? { status: 429 } : { status: 200, delayMs: 20 }
      );
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls11",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 1500,
            retryBackoffMethod: "linear",
            thawRequestCount: 20,
          },
          requestOptions: { cleanupTimeout: QUEUED_ADMISSION_TIMEOUT_MS },
        },
        async (context) => {
          await freezeConfirmed(context, upstream);
          const pending: Promise<Settled>[] = [];
          // Both ends of the documented 0-10 range, and two ties to show the
          // tie-break is arrival rather than luck.
          for (const priority of [0, 10, 5, 1, 10, 3, 0]) {
            pending.push(
              settle(
                send(
                  context.handler,
                  context.client,
                  `p${priority}-${pending.length}`,
                  { priority }
                )
              )
            );
            await sleep(2);
          }
          await Promise.all(pending);
          expect(upstream.tags()).toEqual([
            "p10-1",
            "p10-4",
            "p5-2",
            "p3-5",
            "p1-3",
            "p0-0",
            "p0-6",
          ]);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 40_000);

  it("times a queued request out after cleanupTimeout while frozen", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan((tag) =>
        tag === "trigger" ? { status: 429 } : { status: 200 }
      );
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls12",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 4000,
            retryBackoffMethod: "linear",
          },
          requestOptions: { cleanupTimeout: 400 },
        },
        async (context) => {
          await freezeConfirmed(context, upstream);
          const announcements = tapRequestDone(context.backend);
          const started = Date.now();
          const result = await settle(
            send(context.handler, context.client, "waiting")
          );
          const elapsed = Date.now() - started;

          expect(result.ok).toBe(false);
          if (result.ok) return;
          expect(result.error).toBeInstanceOf(RequestTimeoutError);
          expect(elapsed).toBeGreaterThanOrEqual(350);
          // Well inside the four-second freeze it gave up on.
          expect(elapsed).toBeLessThan(2000);
          expect(upstream.servedCount()).toBe(0);
          // Its place is released, not held until the freeze lapses.
          expect(
            await waitFor(
              async () =>
                (await context.backend.getQueueLength(context.queueKey)) === 0
            )
          ).toBe(true);
          await sleep(150);
          expect([...announcements.values()]).toEqual([1]);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  it("fails a queued request when the caller aborts, and releases its place", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan((tag) =>
        tag === "trigger" ? { status: 429 } : { status: 200 }
      );
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls13",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 3000,
            retryBackoffMethod: "linear",
          },
        },
        async (context) => {
          await freezeConfirmed(context, upstream);
          const controller = new AbortController();
          const pending = settle(
            send(context.handler, context.client, "aborted", {
              signal: controller.signal,
            })
          );
          expect(
            await waitFor(
              async () =>
                (await context.backend.getQueueLength(context.queueKey)) === 1
            )
          ).toBe(true);

          controller.abort();
          const result = await pending;
          expect(result.ok).toBe(false);
          if (result.ok) return;
          expect(result.error).toBeInstanceOf(RequestAbortedError);
          // The place is given back rather than held until the freeze lapses,
          // and nothing was sent on its behalf.
          expect(
            await waitFor(
              async () =>
                (await context.backend.getQueueLength(context.queueKey)) === 0
            )
          ).toBe(true);
          expect(upstream.servedCount()).toBe(0);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  /**
   * `onAbort` has to mark the abandonment as announced, the way the timeout
   * path beside it does. Without the mark the rejection reaches
   * `releaseAbandonedRequest`, whose guard is that very flag, and the request's
   * abandonment goes out twice on a public channel — double-counting every
   * abort for anyone metering `requestDone`, and, for the types that release a
   * slot on it, releasing twice.
   */
  it("announces an aborted request's abandonment exactly once", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan((tag) =>
        tag === "trigger" ? { status: 429 } : { status: 200 }
      );
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls14",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 3000,
            retryBackoffMethod: "linear",
          },
        },
        async (context) => {
          await freezeConfirmed(context, upstream);
          const announcements = tapRequestDone(context.backend);
          const controller = new AbortController();
          const pending = settle(
            send(context.handler, context.client, "aborted", {
              signal: controller.signal,
            })
          );
          await waitFor(
            async () =>
              (await context.backend.getQueueLength(context.queueKey)) === 1
          );
          controller.abort();
          await pending;
          await sleep(200);
          expect([...announcements.values()]).toEqual([1]);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  it("fails a freeze backlog on shutdown instead of hanging", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan((tag) =>
        tag === "trigger" ? { status: 429 } : { status: 200 }
      );
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls15",
          // Long enough that the drain cannot possibly outlast it.
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 30_000,
            retryBackoffMethod: "linear",
          },
          drainTimeoutMs: 800,
        },
        async (context) => {
          await freezeConfirmed(context, upstream);
          const pending = ["s0", "s1", "s2"].map((tag) =>
            settle(send(context.handler, context.client, tag))
          );
          expect(
            await waitFor(
              async () =>
                (await context.backend.getQueueLength(context.queueKey)) === 3
            )
          ).toBe(true);

          const started = Date.now();
          await context.handler.stop();
          const elapsed = Date.now() - started;
          const results = await Promise.all(pending);

          // Bounded by the drain budget rather than by the freeze.
          expect(elapsed).toBeLessThan(5000);
          for (const result of results) {
            expect(result.ok).toBe(false);
            if (result.ok) continue;
            expect(result.error).toBeInstanceOf(ClientUnavailableError);
          }
          // Shutting down must not push a backlog at an upstream that just
          // rate-limited us.
          expect(upstream.servedCount()).toBe(0);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 40_000);

  // ------------------------------------------------------------ cost, grants

  it("ignores cost, but still validates it", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan((tag) =>
        tag === "trigger" ? { status: 429 } : { status: 200 }
      );
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls16",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 1200,
            retryBackoffMethod: "linear",
            thawRequestCount: 20,
          },
          requestOptions: { cleanupTimeout: QUEUED_ADMISSION_TIMEOUT_MS },
        },
        async (context) => {
          // There is no budget for cost to spend, so any positive cost is
          // admitted unchanged — including one no other type could satisfy.
          expect(
            await settle(
              send(context.handler, context.client, "huge", { cost: 1e9 })
            )
          ).toMatchObject({ ok: true });
          expect(
            await settle(
              send(context.handler, context.client, "fractional", {
                cost: 0.25,
              })
            )
          ).toMatchObject({ ok: true });

          // Validated all the same: a cost that cannot be spent anywhere is a
          // caller bug, and this type must not be the one place it is tolerated.
          for (const cost of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
            const rejected = await settle(
              send(context.handler, context.client, "bad", { cost })
            );
            expect(rejected.ok).toBe(false);
            if (rejected.ok) continue;
            expect(rejected.error).toBeInstanceOf(ConfigurationError);
          }

          // And a cost no ceiling could ever admit must not wedge the queue
          // during a freeze, the one time a noLimit request has to queue.
          await freezeConfirmed(context, upstream);
          const results = await Promise.all([
            settle(
              send(context.handler, context.client, "big", { cost: 1e12 })
            ),
            settle(send(context.handler, context.client, "small")),
          ]);
          expect(results.filter((r) => r.ok)).toHaveLength(2);
          expect(upstream.servedCount()).toBe(2);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 40_000);

  it("freezes one isolated grant without touching another", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan((tag) =>
        tag === "t1-trigger" ? { status: 429 } : { status: 200 }
      );
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls17",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 1500,
            retryBackoffMethod: "linear",
          },
          requestOptions: { cleanupTimeout: QUEUED_ADMISSION_TIMEOUT_MS },
          authentication: {
            type: "oauth2",
            clientId: "id",
            clientSecret: "secret",
            grantRateLimitBehavior: "isolated",
            refreshConfig: {
              // Seeded tokens below are valid for an hour, so this is never
              // called; it exists because the type requires it.
              url: `${upstream.baseURL}/token`,
              dataLocation: "jsonBody",
              data: { grant_type: "refresh_token" },
            },
          },
        },
        async (context) => {
          for (const grantId of ["t1", "t2"]) {
            await context.handler.setGrantTokens(context.client, grantId, {
              accessToken: `AT_${grantId}`,
              expiresAt: Date.now() + 3_600_000,
              refreshToken: `RT_${grantId}`,
              refreshTokenExpiresAt: Date.now() + 30 * 86_400_000,
            });
          }
          const base = "nls17:requestHandler:nl:_:a";

          const triggered = await settle(
            send(context.handler, context.client, "t1-trigger", {
              grantId: "t1",
            })
          );
          expect(triggered.ok).toBe(false);
          expect(
            await waitFor(
              async () =>
                (await context.backend.getFreezeState(
                  `${base}:grant:t1:freezeState`
                )) !== null
            )
          ).toBe(true);
          // The freeze belongs to the grant, not to the client: the other
          // tenant's key and the client-level key are untouched.
          expect(
            await context.backend.getFreezeState(`${base}:grant:t2:freezeState`)
          ).toBeNull();
          expect(
            await context.backend.getFreezeState(`${base}:freezeState`)
          ).toBeNull();
          expect(
            await context.backend.smembers(`${base}:frozenGrants`)
          ).toEqual(["t1"]);

          upstream.reset();
          const frozenTenant = settle(
            send(context.handler, context.client, "t1-a", { grantId: "t1" })
          );
          const healthyTenant = await settle(
            send(context.handler, context.client, "t2-a", { grantId: "t2" })
          );
          // Served while t1 is frozen and t1's request is sitting in the queue
          // in front of it — a frozen grant must not stall the ones behind it.
          expect(healthyTenant).toMatchObject({ ok: true });
          expect(upstream.tags()).toEqual(["t2-a"]);
          expect(await frozenTenant).toMatchObject({ ok: true });
          expect(upstream.tags()).toEqual(["t2-a", "t1-a"]);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 40_000);

  /**
   * A host observing `requestDone` must not cost the client its own copy.
   *
   * `getNamespace`'s doc comment invites exactly this subscription, and a
   * backend that hands the channel to the newest subscriber alone takes the
   * client out of its own loop — silently, because nothing errors. Both
   * symptoms are asserted here: the freeze a 429 arms, which is the whole of
   * this type's safety, and the queue entry a finished request leaves behind,
   * which pins the fast path shut for good once it leaks.
   */
  it("keeps serving itself when a host also observes requestDone", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan((tag) =>
        tag === "rl" ? { status: 429 } : { status: 200 }
      );
      await withNoLimit(
        harness,
        {
          baseURL: upstream.baseURL,
          keyPrefix: "nls18",
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 1200,
            retryBackoffMethod: "linear",
          },
          requestOptions: { cleanupTimeout: QUEUED_ADMISSION_TIMEOUT_MS },
          drainTimeoutMs: 800,
        },
        async (context) => {
          const observed: string[] = [];
          await context.backend.subscribe(
            ["nls18:requestHandler:requestDone"],
            (_channel, message) => observed.push(message)
          );

          // Something already waiting, so the next request has to take the
          // queued path and therefore leaves an entry to be cleaned up.
          await context.backend.addRequest(
            context.queueKey,
            context.metaPrefix,
            {
              requestId: "placeholder",
              clientName: context.client,
              requestName: "t.placeholder",
              status: "inProgress",
              priority: 1,
              cost: 1,
              retries: 0,
              timestamp: Date.now(),
              isThawRequest: false,
              ownerId: "someone-else",
            }
          );
          expect(
            await settle(send(context.handler, context.client, "queued"))
          ).toMatchObject({ ok: true });
          // Back down to the placeholder alone: the completed request's entry
          // was removed, which only happens if the client heard about it.
          expect(
            await waitFor(
              async () =>
                (await context.backend.getQueueLength(context.queueKey)) === 1
            )
          ).toBe(true);
          await context.backend.removeRequest(
            context.queueKey,
            context.metaPrefix,
            "placeholder"
          );

          const result = await settle(
            send(context.handler, context.client, "rl")
          );
          expect(result.ok).toBe(false);
          // Delivery is a round trip on its own connection under Redis.
          expect(await waitFor(() => observed.length > 0, 2000)).toBe(true);
          expect(
            await waitFor(
              async () =>
                (await context.backend.getFreezeState(context.freezeKey)) !==
                null,
              2000
            )
          ).toBe(true);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);
});

// ----------------------------------------------------------- clock skew

/**
 * A freeze holds when this process's clock disagrees with Redis's.
 *
 * `frozenUntil` is written from redis `TIME` and every Lua script compares
 * against redis `TIME`, but `getFreezeState`, `isFrozen` and `canProcessRequest`
 * once compared against `Date.now()` in the calling process, and
 * `setFreezeState` sized the key's TTL from the caller's clock too. A process
 * running ahead of Redis therefore saw a live freeze as lapsed, started probing
 * immediately, and on reaching zero thaw progress DELETED the freeze key —
 * cancelling the freeze for every other replica. Measured before the fix: with a
 * +20s skew, 10 requests were released in 22ms while Redis still considered the
 * client frozen for another 7.7 seconds.
 *
 * Nothing else in the suite can see this. Every other test runs with the two
 * clocks in agreement, so the comparison being wrong is invisible: 145 tests
 * passed with all four reads back on `Date.now()`.
 *
 * WHY REPLACING `Date.now` IS ENOUGH. It skews the whole process, so every
 * replica in a multi-replica test would skew together — there is no way here to
 * give one replica a wrong clock and its peer a right one, because the harness
 * runs them in one process. That is sufficient, and the reason is worth having
 * written down: after the fix NO freeze path reads `Date.now()` at all. The
 * deadline is written from the backend's clock, every comparison against it is
 * made in a Lua script on the same clock, and `canProcessRequest` hands the
 * deadline back so callers need not subtract clocks either. A per-replica skew
 * cannot change an answer that never consults the local clock. What this test
 * pins is that property: the freeze survives an arbitrarily wrong local clock,
 * in both directions, so a future reader who reintroduces a local comparison
 * fails here rather than in production.
 *
 * Redis only — under the memory backend `now()` IS `Date.now()`, single process
 * by definition, so skewing it moves both sides of every comparison together and
 * proves nothing.
 */
describe.skipIf(!REDIS_URL)("noLimit scenarios — process clock skew", () => {
  const realNow = Date.now.bind(Date);

  /** Applied BEFORE the handler is built, so the offset is constant rather than a jump. */
  const withSkew = async (skewMs: number, fn: () => Promise<void>) => {
    Date.now = () => realNow() + skewMs;
    try {
      await fn();
    } finally {
      Date.now = realNow;
    }
  };

  const redisHarness = () => {
    const harness = harnesses(13).find((h) => h.name === "redis");
    if (!harness) throw new Error("REDIS_URL set but no redis harness");
    return harness;
  };

  // An hour as well as 20s: a plausible NTP failure and an implausible one should
  // be equally irrelevant, and a fix that merely widened a tolerance would pass
  // the first and fail the second.
  for (const skew of [20_000, 3_600_000]) {
    it(`holds a freeze with the process clock ${skew}ms AHEAD of Redis`, async () => {
      const upstream = await startScriptedUpstream();
      try {
        // 503, so there is no probe budget: this is the shape where the old read
        // returned null outright and the freeze became invisible rather than
        // merely early.
        upstream.setPlan((tag) =>
          tag === "trigger" ? { status: 503 } : { status: 200 }
        );
        await withSkew(skew, async () => {
          await withNoLimit(
            redisHarness(),
            {
              baseURL: upstream.baseURL,
              keyPrefix: "nlskew",
              retryOptions: {
                maxRetries: 0,
                retryBackoffBaseTime: 2000,
                retryBackoffMethod: "linear",
                thawRequestCount: 1,
              },
              requestOptions: {
                cleanupTimeout: QUEUED_ADMISSION_TIMEOUT_MS,
              },
            },
            async (context) => {
              const state = await freezeConfirmed(context, upstream);

              // The skew is genuinely in force, and the old comparison would have
              // called this freeze lapsed. Without this the test could pass by
              // accident on a machine where the override did not take.
              expect(Date.now()).toBeGreaterThan(state.frozenUntil);
              expect(await context.backend.now()).toBeLessThan(
                state.frozenUntil
              );

              const burst = Array.from({ length: 10 }, (_v, i) =>
                settle(send(context.handler, context.client, `b${i}`))
              );
              // Well inside the window Redis is still holding.
              await sleep(600);
              expect(upstream.servedCount()).toBe(0);
              // A read must never delete: that is what cancelled the freeze for
              // every other replica.
              expect(
                await context.backend.getFreezeState(context.freezeKey)
              ).not.toBeNull();

              // Held, not lost — they drain once Redis says the freeze is over.
              const results = await Promise.all(burst);
              expect(results.filter((r) => r.ok)).toHaveLength(10);
              expect(await context.backend.now()).toBeGreaterThanOrEqual(
                state.frozenUntil
              );
            }
          );
        });
      } finally {
        await upstream.close();
      }
    }, 40_000);
  }

  it("releases a freeze on time with the process clock an hour BEHIND Redis", async () => {
    const upstream = await startScriptedUpstream();
    try {
      upstream.setPlan((tag) =>
        tag === "trigger" ? { status: 503 } : { status: 200 }
      );
      await withSkew(-3_600_000, async () => {
        await withNoLimit(
          redisHarness(),
          {
            baseURL: upstream.baseURL,
            keyPrefix: "nlskewback",
            retryOptions: {
              maxRetries: 0,
              retryBackoffBaseTime: 500,
              retryBackoffMethod: "linear",
              thawRequestCount: 1,
            },
            requestOptions: { cleanupTimeout: QUEUED_ADMISSION_TIMEOUT_MS },
          },
          async (context) => {
            await freezeConfirmed(context, upstream);
            // The mirror image of the case above: a caller an hour behind must not
            // hold a lapsed freeze for an hour. Comfortably inside the admission
            // budget, and nowhere near the skew.
            const started = performance.now();
            expect(
              (await settle(send(context.handler, context.client, "after"))).ok
            ).toBe(true);
            expect(performance.now() - started).toBeLessThan(4000);
          }
        );
      });
    } finally {
      await upstream.close();
    }
  }, 40_000);
});

// ------------------------------------------------------------------ two hosts

/**
 * Two handlers, one Redis, one `noLimit` client: the case the memory backend
 * cannot express and the whole library exists for. A freeze is fleet state, so
 * the replica that never saw the 429 has to honour it too, and recovery has to
 * be one probe across both processes rather than one each.
 */
describe.skipIf(!REDIS_URL)("noLimit scenarios — two processes", () => {
  it("honours a freeze one replica caused, and probes recovery once", async () => {
    const upstream = await startScriptedUpstream();
    const connections: Redis[] = [];
    const handlers: RequestHandler[] = [];
    try {
      upstream.setPlan((tag) =>
        tag === "trigger" ? { status: 429 } : { status: 200, delayMs: 40 }
      );
      const wipe = new Redis(`${REDIS_URL}/13`);
      await wipe.flushdb();
      await wipe.quit();

      for (let i = 0; i < 2; i++) {
        const redis = new Redis(`${REDIS_URL}/13`);
        connections.push(redis);
        const handler = new RequestHandler({
          key: KEY,
          backend: redisBackend(redis),
          keyPrefix: "nlfleet",
        });
        await handler.registerClientTemplate(
          "nl" as never,
          ((creds: { instanceId: string }) => [
            {
              name: `nl:_:${creds.instanceId}`,
              rateLimit: { type: "noLimit" },
              retryOptions: {
                maxRetries: 0,
                retryBackoffBaseTime: 2000,
                retryBackoffMethod: "linear",
                thawRequestCount: 20,
              },
              requestOptions: {
                cleanupTimeout: QUEUED_ADMISSION_TIMEOUT_MS,
                defaults: { baseURL: upstream.baseURL },
              },
            },
          ]) as never
        );
        await handler.start();
        handlers.push(handler);
      }
      await handlers[0].addTemplateClient(
        "nl" as never,
        { instanceId: "a" } as never
      );
      const client = "nl:_:a";
      expect(
        await waitFor(() =>
          handlers.some((h) => h.getMetadata().ownedClients.includes(client))
        )
      ).toBe(true);

      // Fired from whichever replica is *not* draining the queue, so the freeze
      // has to travel: a worker publishes the completion and the controller is
      // the one that arms it.
      const worker =
        handlers.find((h) => !h.getMetadata().ownedClients.includes(client)) ??
        handlers[1];
      const observer = redisBackend(connections[0]);
      const freezeKey = "nlfleet:requestHandler:nl:_:a:freezeState";
      const queueKey = "nlfleet:requestHandler:nl:_:a:queue";
      const metaPrefix = "nlfleet:requestHandler:nl:_:a:request";

      const triggered = await settle(send(worker, client, "trigger"));
      expect(triggered.ok).toBe(false);
      expect(
        await waitFor(
          async () => (await observer.getFreezeState(freezeKey)) !== null
        )
      ).toBe(true);
      upstream.reset();

      const pending = [
        ...["h0-a", "h0-b", "h0-c"].map((tag) =>
          settle(send(handlers[0], client, tag))
        ),
        ...["h1-a", "h1-b", "h1-c"].map((tag) =>
          settle(send(handlers[1], client, tag))
        ),
      ];
      await waitFor(
        async () => (await observer.getQueueLength(queueKey)) === 6
      );
      await sleep(200);
      // Neither process sends anything, including the one that never saw the
      // 429 and has no local reason to believe anything is wrong.
      expect(upstream.servedCount()).toBe(0);
      expect(await observer.getQueueStats(queueKey, metaPrefix)).toMatchObject({
        pending: 6,
      });

      const results = await Promise.all(pending);
      expect(results.filter((r) => r.ok)).toHaveLength(6);
      expect(upstream.servedCount()).toBe(6);
      // One probe across the fleet, not one per replica.
      expect(upstream.peakInFlight()).toBe(1);
    } finally {
      for (const handler of handlers) {
        await handler.stop().catch(() => undefined);
      }
      for (const connection of connections) {
        await connection.quit().catch(() => undefined);
      }
      await upstream.close();
    }
  }, 60_000);
});
