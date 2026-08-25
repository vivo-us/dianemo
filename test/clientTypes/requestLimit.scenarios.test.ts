import type { RequestDoneData } from "../../packages/core/src/request/types.js";
import type { DianemoBackend } from "../../packages/core/src/backend/types.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import RequestHandler from "../../packages/core/src/index.js";
import type { Harness } from "./harness.js";
import { harnesses } from "./harness.js";
import { createServer } from "node:http";
import {
  ClientUnavailableError,
  ConfigurationError,
  RequestAbortedError,
  RequestCostExceedsBudgetError,
  RequestTimeoutError,
} from "../../packages/core/src/errors.js";

/**
 * `requestLimit` end to end — every response outcome and every admission path.
 *
 * The sibling file asserts that the budget is a rate. This one follows a single
 * request all the way through: what the upstream returns, what the bucket is
 * left holding, what the caller is told, and whether anything is left waiting.
 *
 * The last section is one test per defect this file was written to find: a
 * budget leak on the most ordinary retry there is, a freeze that could take the
 * host process down with it, a queued request left to time out instead of being
 * told why, an upstream failure backed off as though it were a rate limit, a
 * foreign request at the head of the queue stalling everything behind it, and an
 * urgent arrival pushed into the refill window after the one it belonged in.
 */

const KEY = "0123456789abcdef0123456789abcdef";

/**
 * An upstream driven per URL, with the failure modes a rate limiter has to
 * survive: a status code, a hang, and a socket the server drops on the floor.
 *
 * The shared `startUpstream` only ever answers 200, and every outcome below
 * turns on the response being something else.
 */
async function startScriptedUpstream() {
  const urls: string[] = [];
  const codes = new Map<string, number>();
  const hangs = new Set<string>();
  const resets = new Set<string>();
  /** URLs whose next response only is scripted, popped as they are served. */
  const once = new Map<string, number[]>();

  const server = createServer((req, res) => {
    const url = req.url ?? "";
    urls.push(url);
    // The client-credentials endpoint. Only the tests that send CLIENT-level
    // traffic through an oauth2 client reach it — the grant-scoped ones seed
    // their tokens — and it needs a real token payload rather than the
    // `{"code":n}` body every other route answers with.
    if (url.startsWith("/token")) {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          access_token: "CLIENT-ACCESS",
          token_type: "Bearer",
          expires_in: 3600,
        })
      );
      return;
    }
    if (hangs.has(url)) return;
    const queued = once.get(url);
    if (queued?.length) {
      const code = queued.shift()!;
      res.writeHead(code, { "Content-Type": "application/json" });
      res.end(`{"code":${code}}`);
      return;
    }
    if (resets.has(url)) {
      res.socket?.destroy();
      return;
    }
    const code = codes.get(url) ?? 200;
    res.writeHead(code, { "Content-Type": "application/json" });
    res.end(`{"code":${code}}`);
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;

  return {
    baseURL: `http://127.0.0.1:${port}`,
    /** Every URL the upstream was asked for, in order. */
    urls: () => [...urls],
    calls: (url: string) => urls.filter((u) => u === url).length,
    /** Answer `url` with `code` from now on. */
    code(url: string, code: number) {
      codes.set(url, code);
      return this;
    },
    /** Answer `url` with these codes once each, then fall back. */
    onlyOnce(url: string, ...codes_: number[]) {
      once.set(url, [...codes_]);
      return this;
    },
    /** Never answer `url`. */
    hang(url: string) {
      hangs.add(url);
      return this;
    },
    /** Drop the socket for `url` — ECONNRESET at the client. */
    reset(url: string) {
      resets.add(url);
      return this;
    },
    reset_: () => urls.splice(0, urls.length),
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

interface Ctx {
  handler: RequestHandler;
  backend: DianemoBackend;
  /** Times the budget was actually claimed, by either admission path. */
  admissions: () => number;
  /**
   * Every attempt on the bucket, whether or not it succeeded, with its cost.
   *
   * `admissions` counts successes, so it cannot see a request being declined
   * over and over — and a drain loop that re-enters hundreds of times a second
   * is precisely a run of declines. The cost comes with it because "this request
   * never reached the bucket" is a claim about one cost among several.
   */
  acquireAttempts: () => Array<{ cost: number; acquired: boolean }>;
  /** Times a request was written to the shared queue. */
  enqueues: () => number;
  /** Every `requestDone` this client published, in order. */
  completions: () => RequestDoneData[];
  bucket: (grantId?: string) => Promise<{ tokens: number }>;
  freeze: (grantId?: string) => Promise<{
    frozenUntil: number;
    thawRequestCount: number;
  } | null>;
  /** Grants currently marked frozen — the set the drain loop skips. */
  frozenGrants: () => Promise<string[]>;
  queued: () => Promise<{ pending: number; inProgress: number }>;
}

interface ClientOptions {
  rateLimit: Record<string, unknown>;
  /** Merged into the generated `CreateClientData` — retryOptions, auth, etc. */
  client?: Record<string, unknown>;
  /** Shutdown drain budget. Short by default: several tests leave work queued. */
  drainMs?: number;
  /** Makes `hset` reject for matching keys, to model a backend hiccup. */
  failHsetOn?: (key: string) => boolean;
  /**
   * Runs `backend.now()` this far ahead of the process clock, which is what a
   * Redis on another host with a skewed clock looks like from here. Nothing else
   * is shifted, so a test using it isolates the places that mix the two frames.
   */
  nowSkewMs?: number;
}

/**
 * A handler with one `requestLimit` client, plus counters the shared harness
 * cannot provide.
 *
 * `withHandler` builds the backend itself, and the invariant several tests turn
 * on — "one claim on the budget per outbound request" — is only observable by
 * counting the backend calls that claim it. So the backend is wrapped here
 * before the handler ever sees it.
 */
async function withClient(
  harness: Harness,
  baseURL: string,
  options: ClientOptions,
  fn: (ctx: Ctx) => Promise<void>,
  keyPrefix = "rls"
) {
  const { backend: raw, cleanup } = await harness.create();

  let admissions = 0;
  let enqueues = 0;
  const attempts: Array<{ cost: number; acquired: boolean }> = [];
  const completions: RequestDoneData[] = [];
  const backend = new Proxy(raw, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (typeof value !== "function") return value;
      const bound = value.bind(target) as (...a: never[]) => Promise<unknown>;
      if (prop === "acquireTokens") {
        return async (...args: never[]) => {
          const result = (await bound(...args)) as { acquired: boolean };
          attempts.push({
            cost: args[1] as unknown as number,
            acquired: result.acquired,
          });
          if (result.acquired) admissions++;
          return result;
        };
      }
      if (prop === "tryAdmitImmediately") {
        return async (...args: never[]) => {
          const result = await bound(...args);
          if (result === true) admissions++;
          return result;
        };
      }
      if (prop === "addRequest") {
        return async (...args: never[]) => {
          const result = await bound(...args);
          if (result === true) enqueues++;
          return result;
        };
      }
      if (prop === "publish") {
        return async (...args: never[]) => {
          const [channel, message] = args as unknown as [string, string];
          // The back-off a failure earns is decided in `handleBackoff` and only
          // ever appears here, so this is the one place it can be read exactly
          // rather than inferred from how long something took.
          if (channel.endsWith(":requestDone")) {
            completions.push(JSON.parse(message) as RequestDoneData);
          }
          return bound(...args);
        };
      }
      if (prop === "hset" && options.failHsetOn) {
        return async (...args: never[]) => {
          if (options.failHsetOn!(args[0] as unknown as string)) {
            throw new Error("backend unavailable (injected)");
          }
          return bound(...args);
        };
      }
      if (prop === "now" && options.nowSkewMs) {
        return async () =>
          ((await bound()) as number) + (options.nowSkewMs as number);
      }
      return bound;
    },
  }) as DianemoBackend;

  const handler = new RequestHandler({ key: KEY, backend, keyPrefix });
  await handler.registerClientTemplate(
    "rl" as never,
    (() => [
      {
        name: CLIENT,
        rateLimit: [{ type: "requestLimit", ...options.rateLimit }],
        ...options.client,
        requestOptions: {
          ...(options.client?.requestOptions as object),
          defaults: { baseURL },
        },
      },
    ]) as never
  );
  await handler.start();
  await handler.addTemplateClient("rl" as never, { instanceId: "a" } as never);

  const config = {
    maxTokens: options.rateLimit.maxTokens as number,
    tokensToAdd: options.rateLimit.tokensToAdd as number,
    interval: options.rateLimit.interval as number,
  };
  const base = `${keyPrefix}:requestHandler:${CLIENT}`;
  const grantPath = (grantId: string | undefined, leaf: string) =>
    grantId ? `${base}:grant:${grantId}:${leaf}` : `${base}:${leaf}`;

  try {
    await fn({
      handler,
      backend: raw,
      admissions: () => admissions,
      acquireAttempts: () => [...attempts],
      enqueues: () => enqueues,
      completions: () => [...completions],
      bucket: (grantId) =>
        // Every limit is named internally, and a client declaring one is given
        // the default name — so its bucket lives one segment deeper than the
        // freeze state beside it.
        raw.getTokenBucketState(
          grantPath(grantId, "rateLimit:default"),
          config
        ),
      freeze: (grantId) =>
        raw.getFreezeState(grantPath(grantId, "freezeState")),
      frozenGrants: () => raw.smembers(`${base}:frozenGrants`),
      queued: async () => {
        const stats = await raw.getQueueStats(
          `${base}:queue`,
          `${base}:request`
        );
        return { pending: stats.pending, inProgress: stats.inProgress };
      },
    });
  } finally {
    handler.setDrainTimeout(options.drainMs ?? 1_000);
    await handler.stop().catch(() => undefined);
    await cleanup();
  }
}

const CLIENT = "rl:_:a";

/** An outbound call through the one registered client. */
const send = (
  handler: RequestHandler,
  url: string,
  extra: Record<string, unknown> = {}
) =>
  handler.handleRequest({
    clientName: CLIENT,
    requestName: `t${url.replaceAll("/", ".")}`,
    method: "GET",
    url,
    ...extra,
  });

/** Resolves to the error rather than rejecting, so a test can inspect it. */
const sendCatching = (
  handler: RequestHandler,
  url: string,
  extra: Record<string, unknown> = {}
): Promise<any> => send(handler, url, extra).catch((error) => error);

/** Polls until `check` holds, so a pub/sub round-trip is not a race. */
async function waitFor(
  check: () => Promise<boolean>,
  timeoutMs = 5_000
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await check()) return true;
    if (Date.now() >= deadline) return false;
    await new Promise((r) => setTimeout(r, 20));
  }
}

describe.each(harnesses(10))("requestLimit scenarios — $name", (harness) => {
  /**
   * The retry wait and the freeze are paid once between them, not stacked.
   *
   * `requestLimit` is the only type where the question is decidable: its retry
   * wait honours the configured back-off for a non-429 failure while its freeze
   * is floored at the refill interval, so the two are genuinely different
   * numbers. Everywhere else they read the same field.
   *
   * Both windows open at the moment of refusal, so the caller owes the LARGER of
   * the two. A change that made the local wait run after the freeze rather than
   * inside it would double every 5xx retry's latency and pass every other
   * assertion in this suite. Deliberately far apart — overlapping costs ~2500ms,
   * stacking ~4500ms — because an earlier draft using interval 3000 / base 200
   * left only 200ms between the two hypotheses and was measuring the machine.
   */
  it("pays the retry wait and the freeze once, not twice", async () => {
    const upstream = await startScriptedUpstream();
    const INTERVAL = 2500;
    const BACKOFF = 2000;
    try {
      // 503, not 429: for a rate-limit response the retry wait is floored at the
      // interval too, which makes the two numbers equal and the question moot.
      upstream.onlyOnce("/pay", 503);
      await withClient(
        harness,
        upstream.baseURL,
        {
          rateLimit: {
            type: "requestLimit",
            interval: INTERVAL,
            tokensToAdd: 100,
            maxTokens: 100,
          },
          client: {
            retryOptions: {
              maxRetries: 1,
              retryBackoffBaseTime: BACKOFF,
              retryBackoffMethod: "linear",
              thawRequestCount: 1,
            },
          },
        },
        async (context) => {
          const started = performance.now();
          const result = await send(context.handler, "/pay");
          const elapsed = performance.now() - started;

          expect(result.status).toBe(200);
          expect(upstream.calls("/pay")).toBe(2);
          // The freeze is the longer of the two, so it sets the floor.
          expect(elapsed).toBeGreaterThanOrEqual(INTERVAL - 150);
          // And it is not charged on top of the local wait.
          expect(elapsed).toBeLessThan(INTERVAL + BACKOFF - 500);
        },
        "rlpay"
      );
    } finally {
      await upstream.close();
    }
  }, 30_000);

  let upstream: Awaited<ReturnType<typeof startScriptedUpstream>>;

  beforeAll(async () => {
    upstream = await startScriptedUpstream();
  });
  afterAll(async () => {
    await upstream.close();
  });

  const run = (options: ClientOptions, fn: (ctx: Ctx) => Promise<void>) =>
    withClient(harness, upstream.baseURL, options, fn);

  // ==========================================================================
  // RESPONSE OUTCOMES
  // ==========================================================================

  it("spends one token on a 200 and leaves the queue empty", async () => {
    await run(
      { rateLimit: { interval: 60_000, tokensToAdd: 10, maxTokens: 10 } },
      async (ctx) => {
        const res = await send(ctx.handler, "/ok");
        expect(res.status).toBe(200);
        expect((await ctx.bucket()).tokens).toBe(9);
        expect(ctx.admissions()).toBe(1);
        expect(await ctx.queued()).toEqual({ pending: 0, inProgress: 0 });
        expect(await ctx.freeze()).toBeNull();
      }
    );
  }, 30_000);

  it.each([400, 401, 404, 409])(
    "surfaces a %i once, without retrying or freezing",
    async (status) => {
      const url = `/status${status}`;
      upstream.code(url, status);
      await run(
        { rateLimit: { interval: 60_000, tokensToAdd: 10, maxTokens: 10 } },
        async (ctx) => {
          const before = upstream.calls(url);
          const error = await sendCatching(ctx.handler, url);
          // The axios error reaches the caller with the status intact — the
          // limiter neither swallows it nor rewrites it.
          expect(error?.response?.status).toBe(status);
          // Exactly one attempt: none of these is retryable by default.
          expect(upstream.calls(url) - before).toBe(1);
          // The attempt happened, so it is paid for — but a client error says
          // nothing about the upstream, so nothing is frozen and the bucket
          // keeps the rest of its budget.
          expect((await ctx.bucket()).tokens).toBe(9);
          expect(await ctx.freeze()).toBeNull();
          expect(await ctx.queued()).toEqual({ pending: 0, inProgress: 0 });
        }
      );
    },
    30_000
  );

  it("retries a 409 when retryStatusCodes asks for it, without freezing", async () => {
    upstream.onlyOnce("/conflict", 409, 409);
    await run(
      {
        rateLimit: { interval: 150, tokensToAdd: 10, maxTokens: 10 },
        client: {
          retryOptions: {
            retryStatusCodes: [409],
            maxRetries: 3,
            retryBackoffBaseTime: 20,
          },
        },
      },
      async (ctx) => {
        const res = await send(ctx.handler, "/conflict");
        expect(res.status).toBe(200);
        expect(upstream.calls("/conflict")).toBe(3);
        // Three attempts, three tokens: a retry is a real outbound call and is
        // metered like one.
        expect(ctx.admissions()).toBe(3);
        // A retry the consumer asked for by status says nothing about the
        // upstream's health, so it must not zero the bucket for everyone else.
        expect(await ctx.freeze()).toBeNull();
        expect((await ctx.bucket()).tokens).toBeGreaterThan(0);
        // Nor is it paced by the budget: the first retry waits the configured
        // 20ms, not the 150ms refill interval. Retrying a 409 has nothing to do
        // with how fast tokens arrive.
        const first = ctx.completions().find((done) => done.httpStatus === 409);
        expect(first?.waitTime).toBe(20);
        expect(first?.freezeClient).toBe(false);
      }
    );
  }, 30_000);

  it("freezes on a 429, empties the bucket, and recovers on one probe", async () => {
    upstream.onlyOnce("/limited", 429);
    await run(
      {
        rateLimit: { interval: 400, tokensToAdd: 5, maxTokens: 5 },
        client: {
          retryOptions: {
            maxRetries: 3,
            retryBackoffBaseTime: 20,
            thawRequestCount: 2,
          },
        },
      },
      async (ctx) => {
        const inFlight = sendCatching(ctx.handler, "/limited");
        // Wait on the freeze state rather than on the emptied bucket: the reset
        // is written first, so seeing the freeze means the reset has landed too.
        const froze = await waitFor(async () => {
          const current = await ctx.freeze();
          return !!current && current.frozenUntil > Date.now();
        }, 2_000);
        expect(froze).toBe(true);
        // A 429 is a rate limit, so recovery is probed rather than resumed.
        expect((await ctx.freeze())?.thawRequestCount).toBe(2);
        // And the whole budget is gone, not just this request's token.
        expect((await ctx.bucket()).tokens).toBe(0);

        const res = await inFlight;
        expect(res.status).toBe(200);
        // One probe, not a herd: the retry is the only thing that went out.
        expect(upstream.calls("/limited")).toBe(2);
        // The removal rides the completion broadcast, so let it land.
        const drained = await waitFor(async () => {
          const stats = await ctx.queued();
          return stats.pending + stats.inProgress === 0;
        });
        expect(drained).toBe(true);
      }
    );
  }, 30_000);

  it.each([500, 502, 503])(
    "retries a %i and freezes the client while it backs off",
    async (status) => {
      const url = `/server${status}`;
      upstream.onlyOnce(url, status);
      await run(
        {
          rateLimit: { interval: 250, tokensToAdd: 5, maxTokens: 5 },
          client: { retryOptions: { maxRetries: 2, retryBackoffBaseTime: 20 } },
        },
        async (ctx) => {
          const inFlight = send(ctx.handler, url);
          // A 5xx is the downtime signal, so it freezes the whole client —
          // and, unlike a 429, announces itself as unavailability.
          const froze = await waitFor(async () => {
            const state = await ctx.freeze();
            return !!state && state.frozenUntil > Date.now();
          }, 2_000);
          expect(froze).toBe(true);
          expect((await ctx.freeze())?.thawRequestCount).toBe(0);

          const res = await inFlight;
          expect(res.status).toBe(200);
          expect(upstream.calls(url)).toBe(2);
        }
      );
    },
    30_000
  );

  it("gives the caller the last upstream error once retries run out", async () => {
    upstream.code("/always500", 500);
    await run(
      {
        rateLimit: { interval: 100, tokensToAdd: 20, maxTokens: 20 },
        client: { retryOptions: { maxRetries: 2, retryBackoffBaseTime: 10 } },
      },
      async (ctx) => {
        const error = await sendCatching(ctx.handler, "/always500");
        // Not a NoResponseError and not a timeout: the real failure survives.
        expect(error?.response?.status).toBe(500);
        // maxRetries: 2 means three attempts in total, each paying a token.
        expect(upstream.calls("/always500")).toBe(3);
        expect(ctx.admissions()).toBe(3);
        // The retries queued, so the last removal rides the completion
        // broadcast and arrives after the caller does.
        const drained = await waitFor(async () => {
          const stats = await ctx.queued();
          return stats.pending + stats.inProgress === 0;
        });
        expect(drained).toBe(true);
      }
    );
  }, 30_000);

  it("retries a dropped socket and recovers", async () => {
    upstream.reset("/econnreset");
    await run(
      {
        rateLimit: { interval: 200, tokensToAdd: 5, maxTokens: 5 },
        client: {
          retryOptions: { maxRetries: 3, retryBackoffBaseTime: 20 },
          // Below this test's own 30s timeout, so a lost admission reports as
          // the library's `RequestTimeoutError` naming the request rather than a
          // bare "Test timed out". The default is 60s, which no test-level
          // timeout can outlive, and that is what makes a stall here
          // undiagnosable. Four attempts cost ~280ms of local back-off and
          // freezes of 200/800/1800ms, so the longest single wait for admission
          // is 1.8s and 8s is headroom rather than a deadline.
          requestOptions: { cleanupTimeout: 8_000 },
        },
      },
      async (ctx) => {
        // The socket dies before any status exists, so the retry decision can
        // only come from the error code.
        const inFlight = sendCatching(ctx.handler, "/econnreset");
        const froze = await waitFor(async () => !!(await ctx.freeze()), 2_000);
        // A transport failure is upstream trouble, so it freezes like a 5xx.
        expect(froze).toBe(true);
        const error = await inFlight;
        expect(["ECONNRESET", "ECONNABORTED"]).toContain(error?.code);
        expect(upstream.calls("/econnreset")).toBe(4);
      }
    );
  }, 30_000);

  it("fails a request whose response never arrives, and releases the queue", async () => {
    upstream.hang("/hang");
    await run(
      {
        rateLimit: { interval: 200, tokensToAdd: 5, maxTokens: 5 },
        client: { retryOptions: { maxRetries: 0, retryBackoffBaseTime: 20 } },
      },
      async (ctx) => {
        const error = await sendCatching(ctx.handler, "/hang", {
          timeout: 300,
        });
        expect(error?.code).toBe("ECONNABORTED");
        // Nothing is left holding a place: the next caller must not inherit a
        // queue pinned above empty by a request that never came back.
        const drained = await waitFor(async () => {
          const stats = await ctx.queued();
          return stats.pending + stats.inProgress === 0;
        });
        expect(drained).toBe(true);
      }
    );
  }, 30_000);

  // ==========================================================================
  // ADMISSION PATHS
  // ==========================================================================

  it("skips the queue entirely when nothing is contending", async () => {
    await run(
      { rateLimit: { interval: 60_000, tokensToAdd: 10, maxTokens: 10 } },
      async (ctx) => {
        await send(ctx.handler, "/fast");
        // The point of the fast path: no enqueue, no admission pass, no
        // broadcast, no removal — and still exactly one claim on the budget.
        expect(ctx.enqueues()).toBe(0);
        expect(ctx.admissions()).toBe(1);
      }
    );
  }, 30_000);

  it("queues once the budget is gone, and admits from the queue", async () => {
    await run(
      { rateLimit: { interval: 300, tokensToAdd: 1, maxTokens: 1 } },
      async (ctx) => {
        await send(ctx.handler, "/first");
        expect(ctx.enqueues()).toBe(0);
        const res = await send(ctx.handler, "/second");
        expect(res.status).toBe(200);
        // The second could not fast-path, so it took the ordered path.
        expect(ctx.enqueues()).toBe(1);
        expect(ctx.admissions()).toBe(2);
      }
    );
  }, 30_000);

  it("keeps a queued request through a freeze that lands while it waits", async () => {
    upstream.onlyOnce("/trigger", 429);
    await run(
      {
        rateLimit: { interval: 300, tokensToAdd: 1, maxTokens: 1 },
        client: {
          retryOptions: {
            maxRetries: 2,
            retryBackoffBaseTime: 20,
            thawRequestCount: 1,
          },
        },
        drainMs: 5_000,
      },
      async (ctx) => {
        const trigger = sendCatching(ctx.handler, "/trigger");
        // Queued behind the only token, so it is still waiting when the 429
        // freezes the client and zeroes the bucket underneath it.
        const waiter = sendCatching(ctx.handler, "/waiter");
        const [triggered, waited] = await Promise.all([trigger, waiter]);
        expect(triggered.status).toBe(200);
        // The freeze must delay it, not lose it.
        expect(waited.status).toBe(200);
        expect(upstream.calls("/waiter")).toBe(1);
      }
    );
  }, 30_000);

  it("releases a queued request when the caller aborts", async () => {
    await run(
      { rateLimit: { interval: 30_000, tokensToAdd: 1, maxTokens: 1 } },
      async (ctx) => {
        await send(ctx.handler, "/held");
        const controller = new AbortController();
        const aborting = sendCatching(ctx.handler, "/aborted", {
          signal: controller.signal,
        });
        const queued = await waitFor(
          async () => (await ctx.queued()).pending === 1
        );
        expect(queued).toBe(true);

        const spentBefore = ctx.admissions();
        controller.abort();
        const error = await aborting;
        expect(error).toBeInstanceOf(RequestAbortedError);
        expect((error as RequestAbortedError).statusCode).toBe(499);

        // The place is given back rather than held until the budget arrives,
        // and nothing was spent on a request that never went out.
        const released = await waitFor(async () => {
          const stats = await ctx.queued();
          return stats.pending + stats.inProgress === 0;
        });
        expect(released).toBe(true);
        expect(ctx.admissions()).toBe(spentBefore);
        expect(upstream.calls("/aborted")).toBe(0);
      }
    );
  }, 30_000);

  it("never queues a request whose signal has already fired", async () => {
    await run(
      { rateLimit: { interval: 30_000, tokensToAdd: 1, maxTokens: 1 } },
      async (ctx) => {
        await send(ctx.handler, "/held2");
        const controller = new AbortController();
        controller.abort();
        const enqueuesBefore = ctx.enqueues();
        const error = await sendCatching(ctx.handler, "/dead", {
          signal: controller.signal,
        });
        expect(error).toBeInstanceOf(RequestAbortedError);
        // Taking a place in the queue would mean being admitted in turn,
        // spending a token, and only then failing inside axios.
        expect(ctx.enqueues()).toBe(enqueuesBefore);
        expect(await ctx.queued()).toEqual({ pending: 0, inProgress: 0 });
      }
    );
  }, 30_000);

  /**
   * The same property with NOTHING contending, which is the case that was broken.
   *
   * The test above spends the client's only token before aborting, so the fast
   * path is unavailable and the pre-enqueue check is the first thing an aborted
   * request meets — the ordering that was wrong could not be observed through it.
   * On an idle client the fast path answers first, and while the check sat below
   * it a caller who had already given up was admitted immediately: a token spent
   * on a request axios then refused to dispatch, and axios's `CanceledError`
   * surfacing to the caller in place of this library's own error.
   *
   * So the budget is the assertion, not just the queue: an already-aborted
   * request must cost nothing by EITHER admission path. `admissions` counts both,
   * which is what makes this readable as coverage rather than merely green.
   */
  it("spends nothing for a signal that has already fired on an idle client", async () => {
    await run(
      { rateLimit: { interval: 60_000, tokensToAdd: 10, maxTokens: 10 } },
      async (ctx) => {
        // No warm-up request and no backlog: the fast path is available, so it
        // is the path an aborted request would otherwise take.
        expect(await ctx.queued()).toEqual({ pending: 0, inProgress: 0 });
        const controller = new AbortController();
        controller.abort();

        const error = await sendCatching(ctx.handler, "/dead-idle", {
          signal: controller.signal,
        });

        // The library's own error, with its own status, rather than whatever
        // axios raises when it declines to dispatch.
        expect(error).toBeInstanceOf(RequestAbortedError);
        expect((error as RequestAbortedError).statusCode).toBe(499);
        // Nothing claimed the budget, by either path, and the bucket is whole.
        expect(ctx.admissions()).toBe(0);
        expect((await ctx.bucket()).tokens).toBe(10);
        expect(ctx.enqueues()).toBe(0);
        expect(await ctx.queued()).toEqual({ pending: 0, inProgress: 0 });
        expect(upstream.calls("/dead-idle")).toBe(0);
      }
    );
  }, 30_000);

  it("times a queued request out rather than waiting for a budget it will never get", async () => {
    await run(
      {
        rateLimit: { interval: 30_000, tokensToAdd: 1, maxTokens: 1 },
        client: { requestOptions: { cleanupTimeout: 400 } },
      },
      async (ctx) => {
        await send(ctx.handler, "/hold3");
        const started = Date.now();
        const error = await sendCatching(ctx.handler, "/late");
        expect(error).toBeInstanceOf(RequestTimeoutError);
        expect((error as RequestTimeoutError).statusCode).toBe(408);
        expect(Date.now() - started).toBeLessThan(3_000);
        // And the abandoned entry is released, not left pinning the queue.
        const released = await waitFor(async () => {
          const stats = await ctx.queued();
          return stats.pending + stats.inProgress === 0;
        });
        expect(released).toBe(true);
        expect(upstream.calls("/late")).toBe(0);
      }
    );
  }, 30_000);

  it("fails a queued request with a reason when the handler shuts down", async () => {
    // Deliberately outside `run`: the shutdown is the thing under test, so it
    // has to happen while the request is still parked.
    const { backend, cleanup } = await harness.create();
    const handler = new RequestHandler({
      key: KEY,
      backend,
      keyPrefix: "rlsstop",
    });
    await handler.registerClientTemplate(
      "rl" as never,
      (() => [
        {
          name: CLIENT,
          rateLimit: [
            {
              type: "requestLimit",
              interval: 30_000,
              tokensToAdd: 1,
              maxTokens: 1,
            },
          ],
          requestOptions: { defaults: { baseURL: upstream.baseURL } },
        },
      ]) as never
    );
    await handler.start();
    await handler.addTemplateClient(
      "rl" as never,
      {
        instanceId: "a",
      } as never
    );
    try {
      await send(handler, "/shutdownFirst");
      const stranded = sendCatching(handler, "/stranded");
      await new Promise((r) => setTimeout(r, 100));
      handler.setDrainTimeout(200);
      await handler.stop();
      const error = await stranded;
      // A named 503, not an opaque hang until the admission timeout.
      expect(error).toBeInstanceOf(ClientUnavailableError);
      expect((error as ClientUnavailableError).code).toBe("handler_stopped");
      expect(upstream.calls("/stranded")).toBe(0);
    } finally {
      await handler.stop().catch(() => undefined);
      await cleanup();
    }
  }, 30_000);

  it("lets a high-priority arrival overtake work already queued", async () => {
    await run(
      {
        rateLimit: { interval: 400, tokensToAdd: 1, maxTokens: 1 },
        drainMs: 5_000,
      },
      async (ctx) => {
        upstream.reset_();
        await send(ctx.handler, "/seed");
        const jobs = [0, 1, 2].map((i) =>
          send(ctx.handler, `/low${i}`, { priority: 1 })
        );
        // Long enough that all three are queued, short enough that no token has
        // refilled yet — so the arrival below really does overtake them.
        await new Promise((r) => setTimeout(r, 100));
        expect((await ctx.queued()).pending).toBe(3);
        jobs.push(send(ctx.handler, "/high", { priority: 10 }));
        jobs.push(send(ctx.handler, "/last", { priority: 0 }));
        await Promise.all(jobs);

        const order = upstream.urls();
        const at = (url: string) => order.indexOf(url);
        // Queued first, served after: priority reorders the queue rather than
        // only ordering new arrivals among themselves.
        for (const low of ["/low0", "/low1", "/low2"]) {
          expect(at("/high")).toBeLessThan(at(low));
          expect(at(low)).toBeLessThan(at("/last"));
        }
      }
    );
  }, 30_000);

  it("meters cost rather than requests, fractions included", async () => {
    await run(
      { rateLimit: { interval: 60_000, tokensToAdd: 10, maxTokens: 10 } },
      async (ctx) => {
        await send(ctx.handler, "/half", { cost: 0.5 });
        expect((await ctx.bucket()).tokens).toBe(9.5);
        await send(ctx.handler, "/three", { cost: 3 });
        expect((await ctx.bucket()).tokens).toBe(6.5);
      }
    );
  }, 30_000);

  it("admits a request that costs the entire budget", async () => {
    await run(
      { rateLimit: { interval: 60_000, tokensToAdd: 8, maxTokens: 8 } },
      async (ctx) => {
        // Exactly the ceiling is satisfiable, so it must be admitted rather
        // than rejected alongside the costs that are genuinely impossible.
        const res = await send(ctx.handler, "/whole", { cost: 8 });
        expect(res.status).toBe(200);
        expect((await ctx.bucket()).tokens).toBe(0);
      }
    );
  }, 30_000);

  it("rejects an impossible cost up front and keeps serving afterwards", async () => {
    await run(
      { rateLimit: { interval: 60_000, tokensToAdd: 8, maxTokens: 8 } },
      async (ctx) => {
        const error = await sendCatching(ctx.handler, "/toobig", { cost: 8.5 });
        expect(error).toBeInstanceOf(RequestCostExceedsBudgetError);
        expect(upstream.calls("/toobig")).toBe(0);
        // Rejected before admission, so it neither queued nor spent anything.
        expect(ctx.enqueues()).toBe(0);
        expect((await ctx.bucket()).tokens).toBe(8);
        expect((await send(ctx.handler, "/after")).status).toBe(200);
      }
    );
  }, 30_000);

  it.each([0, -1, Number.NaN])(
    "rejects a cost of %s as configuration, not as a budget problem",
    async (cost) => {
      await run(
        { rateLimit: { interval: 60_000, tokensToAdd: 8, maxTokens: 8 } },
        async (ctx) => {
          const error = await sendCatching(ctx.handler, "/badcost", { cost });
          // A negative cost would mint budget rather than spend it, so this has
          // to fail before anything reaches the bucket.
          expect(error).toBeInstanceOf(ConfigurationError);
          expect((error as ConfigurationError).code).toBe(
            "invalid_request_cost"
          );
          expect((await ctx.bucket()).tokens).toBe(8);
        }
      );
    },
    30_000
  );

  /**
   * A signal axios cannot subscribe to, rejected before it costs anything.
   *
   * Axios types `signal` as `GenericAbortSignal`, whose `addEventListener` is
   * optional, but its Node adapter calls both listener methods unguarded — so the
   * looser shape throws a `TypeError` from inside axios at dispatch, and by then
   * the request has been admitted and a token spent on a call that never goes
   * out. `handleRetry` classifies that `TypeError` as an unknown non-retryable
   * failure, so without this check the caller is told nothing that points at
   * their signal. The bucket is the second assertion for that reason: the
   * complaint is not only which error arrives but that it arrives before the
   * budget moves.
   */
  it("rejects a signal axios cannot subscribe to, before spending anything", async () => {
    await run(
      { rateLimit: { interval: 60_000, tokensToAdd: 8, maxTokens: 8 } },
      async (ctx) => {
        // The shape the axios type permits and its adapter does not: `aborted`
        // present, no listener methods.
        const error = await sendCatching(ctx.handler, "/badsignal", {
          signal: { aborted: false } as unknown as AbortSignal,
        });
        expect(error).toBeInstanceOf(ConfigurationError);
        expect((error as ConfigurationError).code).toBe(
          "invalid_request_signal"
        );
        expect((await ctx.bucket()).tokens).toBe(8);
        expect(ctx.admissions()).toBe(0);
        expect(ctx.enqueues()).toBe(0);
        expect(upstream.calls("/badsignal")).toBe(0);
        // A real signal on the same client still works, so the check has not
        // simply refused cancellation.
        const controller = new AbortController();
        controller.abort();
        const aborted = await sendCatching(ctx.handler, "/goodsignal", {
          signal: controller.signal,
        });
        expect(aborted).toBeInstanceOf(RequestAbortedError);
      }
    );
  }, 30_000);

  // ==========================================================================
  // GRANTS
  // ==========================================================================

  const oauthClient = {
    authentication: {
      type: "oauth2",
      clientId: "CID",
      clientSecret: "SECRET",
      grantRateLimitBehavior: "isolated",
      refreshConfig: {
        url: "/token",
        dataLocation: "urlEncodedForm",
        data: { grant_type: "client_credentials" },
      },
    },
  };

  /**
   * The same oauth2 client, with a token endpoint it can actually reach.
   *
   * `oauthClient` is used by tests that seed every grant they send, so its
   * refresh URL is never called. A test that also sends CLIENT-level traffic
   * needs the client's own credentials, and the refresh call does not go through
   * `requestOptions.defaults` — so that URL has to be absolute, which means
   * building it after `beforeAll` has started the upstream.
   */
  const oauthClientWithTokenEndpoint = () => ({
    authentication: {
      ...oauthClient.authentication,
      refreshConfig: {
        ...oauthClient.authentication.refreshConfig,
        url: `${upstream.baseURL}/token`,
      },
    },
  });

  /** Seeds a live access token so no refresh runs during the test. */
  const seedGrants = async (handler: RequestHandler, ...grantIds: string[]) => {
    for (const grantId of grantIds) {
      await handler.setGrantTokens(CLIENT, grantId, {
        accessToken: `TOKEN_${grantId}`,
        refreshToken: `REFRESH_${grantId}`,
        expiresAt: Date.now() + 3_600_000,
        refreshTokenExpiresAt: Date.now() + 86_400_000,
        tokenType: "Bearer",
      });
    }
  };

  it("meters each isolated grant against its own bucket", async () => {
    await run(
      {
        rateLimit: { interval: 30_000, tokensToAdd: 2, maxTokens: 2 },
        client: oauthClient,
        drainMs: 300,
      },
      async (ctx) => {
        await seedGrants(ctx.handler, "g1", "g2");
        await Promise.all([
          send(ctx.handler, "/g1a", { grantId: "g1" }),
          send(ctx.handler, "/g1b", { grantId: "g1" }),
        ]);
        expect((await ctx.bucket("g1")).tokens).toBe(0);
        // The other tenant is untouched, and so is the client-level bucket the
        // non-grant traffic draws on.
        expect((await ctx.bucket("g2")).tokens).toBe(2);
        expect((await ctx.bucket()).tokens).toBe(2);

        // g1 is exhausted for the next 30 seconds; g2 must not wait behind it.
        const starved = sendCatching(ctx.handler, "/g1c", { grantId: "g1" });
        const started = Date.now();
        const res = await send(ctx.handler, "/g2a", { grantId: "g2" });
        expect(res.status).toBe(200);
        expect(Date.now() - started).toBeLessThan(2_000);
        expect(upstream.calls("/g1c")).toBe(0);
        starved.catch(() => undefined);
      }
    );
  }, 30_000);

  it("freezes only the grant that was rate limited", async () => {
    upstream.code("/g1boom", 429);
    await run(
      {
        rateLimit: { interval: 30_000, tokensToAdd: 3, maxTokens: 3 },
        client: {
          ...oauthClient,
          retryOptions: { maxRetries: 0, retryBackoffBaseTime: 50 },
        },
        drainMs: 300,
      },
      async (ctx) => {
        await seedGrants(ctx.handler, "g1", "g2");
        const error = await sendCatching(ctx.handler, "/g1boom", {
          grantId: "g1",
        });
        expect(error?.response?.status).toBe(429);

        const frozen = await waitFor(async () => {
          const state = await ctx.freeze("g1");
          return !!state && state.frozenUntil > Date.now();
        });
        expect(frozen).toBe(true);
        // Only g1's bucket is emptied, and only g1's freeze state is set.
        expect((await ctx.bucket("g1")).tokens).toBe(0);
        expect((await ctx.bucket("g2")).tokens).toBe(3);
        expect(await ctx.freeze("g2")).toBeNull();
        expect(await ctx.freeze()).toBeNull();

        // And the other tenant keeps being served throughout.
        const res = await send(ctx.handler, "/g2ok", { grantId: "g2" });
        expect(res.status).toBe(200);
      }
    );
  }, 30_000);

  /**
   * A thaw probe that recovers through a RETRY has to release its grant.
   *
   * The probe is nominated by the controller, which marks the grant frozen to
   * hold the single-probe claim. Success or permanent failure removes the queue
   * entry, and the removal is what reports "this was the probe" and clears the
   * mark. A retry keeps its entry, so neither happens — and when the retry is one
   * the consumer asked for (`retryStatusCodes` here, or `retryHandler`) no freeze
   * is armed either, so no timer is coming. The mark then outlives the recovery
   * it was protecting: the retry cannot be re-admitted, because the drain loop
   * skips its own grant, and nothing frees it until the controller's stale-grant
   * sweep on the next health tick.
   *
   * `healthCheckIntervalMs` is far away and the admission deadline is close, so
   * this fails in a second and fails hard rather than passing slowly: with the
   * mark left set the retry is never admitted and the caller gets a
   * `RequestTimeoutError` having spent two of its four attempts, with the
   * upstream never asked a third time. Measured 616ms fixed against 10 001ms with
   * the default tick as the only backstop.
   *
   * Three upstream calls is the load-bearing count — it proves the middle attempt
   * was a retry rather than a probe that simply succeeded.
   */
  it("clears the frozen mark when a thaw probe recovers through a retry", async () => {
    upstream.onlyOnce("/thaw", 429, 409);
    await run(
      {
        rateLimit: { interval: 400, tokensToAdd: 5, maxTokens: 5 },
        client: {
          ...oauthClient,
          retryOptions: {
            maxRetries: 3,
            retryBackoffBaseTime: 50,
            retryStatusCodes: [409],
          },
          healthCheckIntervalMs: 30_000,
          requestOptions: { cleanupTimeout: 2_000 },
        },
        drainMs: 300,
      },
      async (ctx) => {
        await seedGrants(ctx.handler, "g1");
        const started = Date.now();
        const res = await send(ctx.handler, "/thaw", { grantId: "g1" });
        expect(res.status).toBe(200);
        expect(Date.now() - started).toBeLessThan(3_000);
        expect(upstream.calls("/thaw")).toBe(3);

        // Nothing is left holding the grant. Polled, because the caller is
        // answered by axios while the mark is cleared by the CONTROLLER from the
        // `requestDone` broadcast — a real round trip on Redis. The poll cannot
        // launder the defect: `healthCheckIntervalMs` above puts the stale-grant
        // sweep half a minute away, so within this window only the fix can empty
        // the set, and on a revert the request fails on its deadline before this
        // line is reached.
        const released = await waitFor(
          async () => (await ctx.frozenGrants()).length === 0
        );
        expect(released).toBe(true);
        // And the successful probe consumed exactly one unit of thaw progress,
        // which is what makes it the probe rather than an ordinary request that
        // happened to go out. Read directly: thaw progress is written before the
        // mark is cleared, so the wait above has already ordered it.
        expect((await ctx.freeze("g1"))?.thawRequestCount).toBe(2);
      }
    );
  }, 30_000);

  /**
   * A CLIENT-LEVEL request that cannot spend must not stall an isolated grant.
   *
   * The two sibling tests above cover a grant blocking a grant, which the drain
   * loop handles by skipping the grant. This is the other half, and it is the one
   * with no owner: the head of the queue draws on the CLIENT-level bucket, which
   * is empty for a full refill interval, and every isolated tenant behind it
   * draws on a bucket of its own that is completely untouched. Breaking out of
   * the loop there — which is the right answer for a client with no isolated
   * grants, because nothing else can spend either — stalls every tenant for the
   * refill, or until their own admission deadline fails them. That is precisely
   * the cross-tenant delay isolation exists to prevent, arriving one scope up
   * from where the grant skip catches it.
   *
   * Measured as the tenant's LATENCY, not its ordering: an assertion on order
   * would pass on a client that served the tenant a minute late. Verified to fail
   * on a revert — with the `usesGrantIsolation()` skip in the `tryAcquireTurn`
   * branch replaced by `break`, the tenant is never admitted, the upstream never
   * sees it, and it dies on its admission timeout (3-8ms becomes 1500ms and a
   * `RequestTimeoutError`) on both backends.
   */
  it("a client-level head with an empty bucket does not stall an isolated grant", async () => {
    await run(
      {
        rateLimit: { interval: 60_000, tokensToAdd: 2, maxTokens: 2 },
        client: {
          ...oauthClientWithTokenEndpoint(),
          // Short, so a revert fails this test in a second rather than waiting
          // out the default minute.
          requestOptions: { cleanupTimeout: 1_500 },
        },
        drainMs: 300,
      },
      async (ctx) => {
        await seedGrants(ctx.handler, "g1");
        // Spend the client-level budget down to nothing.
        expect((await send(ctx.handler, "/cl1")).status).toBe(200);
        expect((await send(ctx.handler, "/cl2")).status).toBe(200);
        expect((await ctx.bucket()).tokens).toBe(0);

        // A client-level request that cannot be admitted for a full interval.
        // Aborted rather than timed out at the end, so the observation window
        // belongs to this test rather than to an admission budget.
        const controller = new AbortController();
        const head = sendCatching(ctx.handler, "/clhead", {
          signal: controller.signal,
        });
        const queued = await waitFor(
          async () => (await ctx.queued()).pending === 1
        );
        expect(queued).toBe(true);

        const started = Date.now();
        const tenant = await send(ctx.handler, "/tenant", { grantId: "g1" });
        expect(tenant.status).toBe(200);
        expect(Date.now() - started).toBeLessThan(1_000);

        // The tenant spent its own bucket, the client-level one is still empty,
        // and the head really was still waiting — a head that had somehow been
        // served would make this pass for the wrong reason.
        expect((await ctx.bucket("g1")).tokens).toBe(1);
        expect((await ctx.bucket()).tokens).toBe(0);
        expect(upstream.calls("/clhead")).toBe(0);

        controller.abort();
        expect(await head).toBeInstanceOf(RequestAbortedError);
      }
    );
  }, 30_000);

  /**
   * The same guarantee on the other gate: a client-level FREEZE at the head.
   *
   * `canProcessNextRequest` reads the freeze state, so a client-level 429 stops
   * the head one step earlier than an empty bucket does — before any budget is
   * consulted. The skip has to exist in both places or the stall simply moves:
   * this branch is the one a 429 takes, the sibling above is the one a drained
   * bucket takes, and a client can be in either state with healthy tenants
   * queued behind it.
   *
   * Verified to fail on a revert of the freeze branch alone (the `break` in the
   * `canProcess` arm): the tenant waits out its admission timeout while its own
   * grant is neither frozen nor short of tokens.
   */
  it("a client-level freeze at the head does not stall an isolated grant", async () => {
    upstream.code("/clboom", 429);
    await run(
      {
        // The freeze floors at the refill interval, so this is a freeze that
        // outlasts the test by a wide margin — the tenant cannot be waiting it
        // out.
        rateLimit: { interval: 60_000, tokensToAdd: 5, maxTokens: 5 },
        client: {
          ...oauthClientWithTokenEndpoint(),
          retryOptions: { maxRetries: 0, retryBackoffBaseTime: 50 },
          requestOptions: { cleanupTimeout: 1_500 },
        },
        drainMs: 300,
      },
      async (ctx) => {
        await seedGrants(ctx.handler, "g1");
        const error = await sendCatching(ctx.handler, "/clboom");
        expect(error?.response?.status).toBe(429);

        // The controller arms the freeze from the `requestDone` broadcast, so the
        // caller's rejection does not mean the freeze exists yet.
        const frozen = await waitFor(async () => {
          const state = await ctx.freeze();
          return !!state && state.frozenUntil > Date.now();
        });
        expect(frozen).toBe(true);

        const controller = new AbortController();
        const head = sendCatching(ctx.handler, "/clhead2", {
          signal: controller.signal,
        });
        const queued = await waitFor(
          async () => (await ctx.queued()).pending === 1
        );
        expect(queued).toBe(true);

        const started = Date.now();
        const tenant = await send(ctx.handler, "/tenant2", { grantId: "g1" });
        expect(tenant.status).toBe(200);
        expect(Date.now() - started).toBeLessThan(1_000);

        // The tenant's own freeze state was never written, which is what makes
        // its admission legitimate rather than a freeze being ignored.
        expect(await ctx.freeze("g1")).toBeNull();
        expect(upstream.calls("/clhead2")).toBe(0);

        controller.abort();
        expect(await head).toBeInstanceOf(RequestAbortedError);
      }
    );
  }, 30_000);

  // ==========================================================================
  // DEFECTS THIS FILE WAS WRITTEN TO FIND
  // ==========================================================================

  /**
   * One claim on the budget per outbound request. It used to be two, for the
   * most ordinary retry there is.
   *
   * A first attempt with nothing contending takes the fast path, so it leaves no
   * queue entry. When it retried, `waitForRequestReady` re-added the entry as
   * `pending` — which wakes the drain loop — and then flipped it to `pending` a
   * second time. The flip landed after the loop had claimed it, un-claiming a
   * request mid-admission, and the next pass claimed and paid for the very same
   * request again. Nothing gave that token back: `requestLimit` overrides
   * neither `releaseAdmission` nor `handleOwnTypeRequestDone`.
   *
   * The vendor never saw extra traffic — the leak cost throughput, not
   * politeness — but on a `tokensToAdd: 1` client every retried request quietly
   * cost twice its budget, and it bit hardest during a 429 recovery, when the
   * bucket holds exactly `tokensToAdd`. The flip is now conditional on the add
   * not having created the entry.
   */
  it("spends exactly one token per outbound attempt when a fast-path request retries", async () => {
    // Three rounds, because this is a race and one round is thin evidence.
    // Recorded as a pair per round rather than two running totals: an admission
    // can only ever exceed an outbound call, never fall short of one, so
    // per-round equality is no stricter than equality of the totals — it just
    // names the round that diverged instead of reporting "9 !== 6" for the file.
    const rounds: Array<[admitted: number, served: number]> = [];
    for (let round = 0; round < 3; round++) {
      const url = `/leak${round}`;
      upstream.onlyOnce(url, 429);
      await run(
        {
          rateLimit: { interval: 100, tokensToAdd: 5, maxTokens: 5 },
          client: {
            retryOptions: { maxRetries: 2, retryBackoffBaseTime: 50 },
            // Well under this test's own timeout, so a lost admission surfaces
            // as `RequestTimeoutError` for the named request rather than as the
            // whole test timing out. Inherited, the default is 60s — the same as
            // the timeout below, which no failure can outlive informatively.
            requestOptions: { cleanupTimeout: 8_000 },
          },
        },
        async (ctx) => {
          const res = await send(ctx.handler, url);
          expect(res.status).toBe(200);
          // The stray claim can land just after the caller is answered, so
          // let the drain loop finish its pass before counting.
          await new Promise((r) => setTimeout(r, 150));
          rounds.push([ctx.admissions(), upstream.calls(url)]);
        }
      );
    }
    // Two outbound calls per round — the 429 and the retry that succeeds — and
    // exactly one token spent per call. Three claims in a round before the fix,
    // the third for a request that had already been admitted.
    expect(rounds).toEqual([
      [2, 2],
      [2, 2],
      [2, 2],
    ]);
  }, 60_000);

  /**
   * A queued request the budget can no longer satisfy is failed, not left to
   * time out.
   *
   * `tryAcquireTurn` throws `RequestCostExceedsBudgetError` for it, and the
   * drain loop steps over it so an impossible head cannot wedge the queue — but
   * stepping over it used to be all that happened, so the entry sat until
   * `cleanupTimeout` and the caller was told the controller had been slow (408)
   * rather than that its cost no longer fit (400). A request submitted fresh
   * with the identical cost got the correct answer instantly, which is what made
   * the difference indefensible. The owning replica now fails the waiter
   * directly; on any other replica the admission timeout is still the backstop,
   * since only the owner holds it.
   *
   * What delivers the reason is the `processRequests()` poke inside
   * `handleRateLimitUpdated`: applying the new budget runs a pass immediately,
   * that pass re-reads the ceiling, and the request is failed there. It is NOT
   * the `scheduleDrain` the earlier decline booked — this test passes with
   * `scheduleDrain` stubbed out entirely, and it passes with a refill interval
   * far longer than `cleanupTimeout`, both of which were checked. So the timings
   * below are ordinary rather than load-bearing, and the 3s bound measures the
   * poke rather than a revisit.
   */
  it("tells the caller why, when a shrinking budget makes a queued cost impossible", async () => {
    await run(
      {
        rateLimit: { interval: 500, tokensToAdd: 10, maxTokens: 10 },
        client: { requestOptions: { cleanupTimeout: 8_000 } },
      },
      async (ctx) => {
        // Drain the bucket so the costly request has to queue for a refill.
        await send(ctx.handler, "/drain", { cost: 9 });
        const queuedRequest = sendCatching(ctx.handler, "/costly", {
          cost: 8,
        });
        await waitFor(async () => (await ctx.queued()).pending === 1);
        const shrunk = Date.now();
        await ctx.backend.publish(
          `rls:requestHandler:rateLimitUpdated`,
          JSON.stringify({
            clientName: CLIENT,
            rateLimit: [
              {
                type: "requestLimit",
                interval: 500,
                tokensToAdd: 4,
                maxTokens: 4,
              },
            ],
            source: "operator",
            publisherInstanceId: "operator-console",
          })
        );
        const error = await queuedRequest;
        expect(error).toBeInstanceOf(RequestCostExceedsBudgetError);
        // Well inside the 8s `cleanupTimeout`: the caller hears the real reason
        // as soon as the loop discovers it, rather than a 408 once the wait runs
        // out.
        expect(Date.now() - shrunk).toBeLessThan(3_000);
        expect(upstream.calls("/costly")).toBe(0);
        // And the entry goes with it, rather than being skipped forever.
        const released = await waitFor(async () => {
          const stats = await ctx.queued();
          return stats.pending + stats.inProgress === 0;
        });
        expect(released).toBe(true);
      }
    );
  }, 30_000);

  /**
   * A backend hiccup during a freeze must not take the host process down.
   *
   * `handleFreezeRequests` used to call `handleFreezeOwnTypeRequests` without
   * awaiting it, and this client's implementation is async — it is the `hset`
   * that empties the bucket. A rejected write therefore had no catch anywhere
   * above it and surfaced as an unhandled rejection, which Node terminates the
   * process for by default; every other fire-and-forget path in this codebase
   * guards against exactly that. Awaiting it also orders the reset before the
   * freeze is armed, so the freeze can no longer be set while the bucket still
   * holds a balance, and a slow write can no longer land after the freeze has
   * lapsed and re-zero a bucket the thaw probe has already drawn from.
   */
  it("does not let a failed bucket reset escape as an unhandled rejection", async () => {
    const escaped: unknown[] = [];
    const record = (error: unknown) => escaped.push(error);
    process.on("unhandledRejection", record);
    try {
      upstream.code("/hiccup", 429);
      await run(
        {
          rateLimit: { interval: 100, tokensToAdd: 5, maxTokens: 5 },
          client: {
            retryOptions: { maxRetries: 0, retryBackoffBaseTime: 20 },
          },
          // Only the bucket reset fails. What happens to the rest of the
          // freeze is the implementation's business; what must not happen is
          // a rejection nobody owns.
          failHsetOn: (key) => key.endsWith(":rateLimit"),
        },
        async (ctx) => {
          await sendCatching(ctx.handler, "/hiccup");
          await new Promise((r) => setTimeout(r, 300));
        }
      );
    } finally {
      process.off("unhandledRejection", record);
    }
    // One "backend unavailable (injected)" rejection with no handler, before
    // the fix — which under Node's default policy ends the process.
    expect(escaped).toHaveLength(0);
  }, 30_000);

  /**
   * A 5xx or a dropped socket is not a budget problem, so it is backed off by
   * the time the operator configured.
   *
   * `getRetryBackoffBaseTime` used to return `max(interval, base)` for every
   * failure. That floor is honest only after a 429, which empties the bucket —
   * for an upstream failure the bucket is untouched, and flooring at the
   * interval stopped the whole fleet for a full window over one bad response.
   * On a client transcribing an hourly vendor limit literally, one 502 stopped
   * it for an hour.
   *
   * Read off the `requestDone` payload rather than timed, because the two
   * durations are now different numbers and only the payload separates them:
   * `waitTime` is this request's own back-off, `freezeTime` is how long the
   * fleet stands down. Wall-clock latency conflates them with the next refill.
   */
  it("backs a 502 off by the configured base time, not the refill interval", async () => {
    upstream.onlyOnce("/coarse", 502);
    await run(
      {
        rateLimit: { interval: 400, tokensToAdd: 20, maxTokens: 20 },
        client: { retryOptions: { maxRetries: 1, retryBackoffBaseTime: 20 } },
      },
      async (ctx) => {
        expect((await send(ctx.handler, "/coarse")).status).toBe(200);
        const failure = ctx
          .completions()
          .find((done) => done.httpStatus === 502);
        // A 5xx is still a fleet-wide statement about the upstream...
        expect(failure?.freezeClient).toBe(true);
        expect(failure?.isRateLimited).toBe(false);
        // ...so the freeze it arms still outlasts a refill, because freezing is
        // what emptied the bucket...
        expect(failure?.freezeTime).toBe(400);
        // ...but this request's own back-off is the 20ms that was configured.
        // Charging it a refill interval was the defect: the bucket had a full
        // twenty tokens and the upstream, not the budget, was the problem.
        expect(failure?.waitTime).toBe(20);
      }
    );
  }, 30_000);

  /**
   * The interval floor is deliberate for a real rate limit, and stays.
   *
   * The 429 emptied the bucket, so a retry before it refills cannot succeed —
   * the interval is the honest minimum however short the configured base time
   * is, and the docs recommend short intervals precisely because they lower
   * latency, which would otherwise produce the shortest possible 429 back-off.
   * Here the two durations coincide, which is the point: for a rate limit there
   * is nothing to separate.
   */
  it("still floors a 429 back-off at the refill interval", async () => {
    upstream.onlyOnce("/floored", 429);
    await run(
      {
        rateLimit: { interval: 400, tokensToAdd: 5, maxTokens: 5 },
        client: { retryOptions: { maxRetries: 2, retryBackoffBaseTime: 20 } },
      },
      async (ctx) => {
        expect((await send(ctx.handler, "/floored")).status).toBe(200);
        const limited = ctx
          .completions()
          .find((done) => done.httpStatus === 429);
        expect(limited?.isRateLimited).toBe(true);
        // The configured 20ms is raised to the interval, because the bucket this
        // request would draw on has just been emptied.
        expect(limited?.waitTime).toBe(400);
        expect(limited?.freezeTime).toBe(400);
      }
    );
  }, 30_000);

  /**
   * A head the budget can never satisfy must not stop the queue behind it, even
   * when this replica cannot fail it.
   *
   * The drain loop steps over an impossible cost and, when it owns the waiter,
   * fails it with the reason — which also removes the entry, so the step-past
   * itself is barely exercised. The case that does exercise it is a head owned by
   * ANOTHER replica: `failUnsatisfiable` declines, because only the owner holds
   * the waiter, so the entry stays `pending` and every later pass is handed it
   * again. Without `skipRequestIds` the loop either re-selects it forever or
   * breaks on it, and nothing behind it is examined — a whole client stalled by
   * one foreign request until its owner's admission timeout removes it.
   *
   * Deliberately only the head-of-line property. The skip set is also per pass
   * rather than a blacklist — a ceiling that grows back puts the request into
   * contention again — but asserting that here would make one test depend on the
   * skip set, `rateLimitUpdated` dispatch and ownership at once, and a failure
   * would not say which broke.
   *
   * Verified to fail on a revert: with the `break` restored and `skipRequestIds`
   * not passed, the three requests behind it time out on both backends.
   */
  it("keeps draining behind an impossible head this replica cannot fail", async () => {
    await run(
      {
        rateLimit: { interval: 700, tokensToAdd: 5, maxTokens: 5 },
        client: { requestOptions: { cleanupTimeout: 4_000 } },
        drainMs: 500,
      },
      async (ctx) => {
        // Written straight to the queue: `handleRequest` can only produce an
        // entry this instance owns, and foreign ownership is the whole point.
        const base = `rls:requestHandler:${CLIENT}`;
        const queueKey = `${base}:queue`;
        const prefix = `${base}:request`;
        await ctx.backend.addRequest(queueKey, prefix, {
          requestId: "foreign",
          clientName: CLIENT,
          requestName: "t.foreign",
          status: "pending",
          // Top of the queue and above the ceiling, so every pass selects it
          // first and every pass has to step over it.
          priority: 10,
          cost: 9,
          retries: 0,
          timestamp: Date.now(),
          isThawRequest: false,
          ownerId: "someone-else",
        } as never);

        const started = Date.now();
        const behind = await Promise.all([
          send(ctx.handler, "/behind1"),
          send(ctx.handler, "/behind2"),
          send(ctx.handler, "/behind3"),
        ]);
        expect(behind.map((res) => res.status)).toEqual([200, 200, 200]);
        // On the tokens already in the bucket, so no refill was waited for —
        // and nowhere near the 4s admission budget a stalled queue would spend.
        expect(Date.now() - started).toBeLessThan(1_500);

        // One claim each, and no run of declines: stepping over the head must
        // not turn into re-selecting it in a loop.
        const attempts = ctx.acquireAttempts();
        expect(attempts.filter((attempt) => attempt.acquired)).toHaveLength(3);
        expect(attempts.length).toBeLessThan(10);
        // The impossible cost never reached the bucket at all: the ceiling is
        // re-read before the acquire, so it is refused above it.
        expect(attempts.some((attempt) => attempt.cost === 9)).toBe(false);

        // Stepped over, not consumed and not failed: only its owner can answer
        // for it, and the admission timeout there is the backstop.
        const settled = await waitFor(async () => {
          const entry = await ctx.backend.getRequest(prefix, "foreign");
          return entry?.status === "pending";
        });
        expect(settled).toBe(true);

        // Nothing will ever complete it, and the shutdown drain waits on depth.
        await ctx.backend.removeRequest(queueKey, prefix, "foreign");
      }
    );
  }, 30_000);

  /**
   * The skip set lasts one pass, not the client's lifetime.
   *
   * `skippedRequestIds` is local to `processRequests`, so a head stepped over in
   * one pass is a candidate again in the next. Hoisting it to the client — an
   * inviting optimisation, since re-selecting a head already known to be
   * impossible looks like waste — turns it into a permanent blacklist, and a
   * ceiling that grows back would then never rescue the request: it would sit
   * queued, never priced again, until its owner's admission timeout.
   *
   * The ceiling is raised in process and the pass invoked directly, rather than
   * through a `rateLimitUpdated` broadcast and a fresh arrival, because the
   * property under test is the lifetime of one function's local state. Every
   * other route — the broadcast, a rebuild, an arriving request — would be a
   * second suspect when this fails. Reaching for the client is the point rather
   * than a shortcut: nothing else is involved.
   *
   * The head is foreign-owned so that the loop cannot fail it and remove it,
   * which is what leaves it there for the second pass to reconsider.
   */
  it("reconsiders a skipped head on the next pass", async () => {
    await run(
      {
        // No refill inside the test, so what changes between the two passes is
        // the ceiling and nothing else.
        rateLimit: { interval: 60_000, tokensToAdd: 5, maxTokens: 5 },
        drainMs: 500,
      },
      async (ctx) => {
        const base = `rls:requestHandler:${CLIENT}`;
        const queueKey = `${base}:queue`;
        const prefix = `${base}:request`;
        // An explicit balance, because an untouched bucket initialises itself to
        // `maxTokens` — which after the raise below would be enough to admit the
        // head, and this test is about pricing it, not about serving it.
        await ctx.backend.resetTokenBucket(`${base}:rateLimit:default`, 5);
        await ctx.backend.addRequest(queueKey, prefix, {
          requestId: "skipped",
          clientName: CLIENT,
          requestName: "t.skipped",
          status: "pending",
          priority: 10,
          cost: 9,
          retries: 0,
          timestamp: Date.now(),
          isThawRequest: false,
          ownerId: "someone-else",
        } as never);

        const client = (
          ctx.handler as unknown as {
            clients: Map<
              string,
              {
                processRequests: () => Promise<void>;
                rateLimit: Record<string, unknown>[];
              }
            >;
          }
        ).clients.get(CLIENT)!;

        // Pass one: cost 9 against a ceiling of 5, so it is refused above the
        // bucket and never priced.
        await client.processRequests();
        const priced = () =>
          ctx.acquireAttempts().some((attempt) => attempt.cost === 9);
        expect(priced()).toBe(false);

        // Same client, same entry, a ceiling that now fits. One more pass is all
        // that should be needed for the head to be considered again.
        client.rateLimit = [
          {
            name: "default",
            type: "requestLimit",
            interval: 60_000,
            tokensToAdd: 5,
            maxTokens: 20,
          },
        ];
        await client.processRequests();
        // Priced, not admitted: 9 against the 5 tokens in the bucket is still a
        // decline. Reaching the bucket at all is the property.
        expect(priced()).toBe(true);
        expect(ctx.admissions()).toBe(0);

        await ctx.backend.removeRequest(queueKey, prefix, "skipped");
      }
    );
  }, 30_000);

  /**
   * An early wake-up must re-read the deadline, not trust the one it booked from.
   *
   * `armFreeze` is monotone: a freeze deadline can only GROW, because the freeze
   * belongs to the client and a failure landing second must not be able to shorten
   * what the first earned. That direction is what makes booking a wake-up from a
   * deadline safe at all — a booking taken at time T is either exactly right or
   * EARLY, and early costs one extra decline. It is also the whole reason the loop
   * books on every decline instead of once.
   *
   * Book once and trust it, and this is what happens: the pass wakes at the
   * original 400ms, finds the client still frozen against the grown deadline,
   * returns without booking anything, and the request then waits for a health tick
   * that is 120s away — so it dies on its admission budget with the upstream never
   * called. The same shape as the defect this whole area was fixed for, one level
   * up. So: do not "simplify" this into a single booking.
   *
   * The property is measured rather than asserted about the timer bookkeeping,
   * because until this test existed it was a claim relayed between agents about
   * `scheduleDrain`'s earliest-timer guard. 900ms separates the two outcomes: the
   * first deadline is at 400ms and the grown one at about 1200ms.
   */
  it("follows a freeze deadline that grows while a request waits", async () => {
    await run(
      {
        // A budget that cannot be the reason for any delay here.
        rateLimit: { interval: 60_000, tokensToAdd: 10, maxTokens: 10 },
        client: {
          healthCheckIntervalMs: 120_000,
          requestOptions: { cleanupTimeout: 6_000 },
        },
        drainMs: 300,
      },
      async (ctx) => {
        const freezeKey = `rls:requestHandler:${CLIENT}:freezeState`;
        const started = Date.now();
        // `setFreezeState` rather than `armFreeze` for the fixture: it is the
        // non-monotone write, which is what lets a test place a freeze wherever it
        // needs one.
        await ctx.backend.setFreezeState(
          freezeKey,
          (await ctx.backend.now()) + 400,
          0
        );

        const queued = send(ctx.handler, "/grown");
        await new Promise((r) => setTimeout(r, 150));
        // Extended while the first wake-up is still pending, through the monotone
        // operation — so this is the deadline the fleet now honours.
        const armed = await ctx.backend.armFreeze(
          freezeKey,
          (await ctx.backend.now()) + 1_050,
          0
        );
        expect(armed.frozenUntil).toBeGreaterThan(await ctx.backend.now());

        const res = await queued;
        const elapsed = Date.now() - started;
        expect(res.status).toBe(200);
        // Too late to have been served by the first booking, and far too early to
        // have been rescued by anything periodic.
        expect(elapsed).toBeGreaterThan(900);
        expect(elapsed).toBeLessThan(3_000);
      }
    );
  }, 30_000);

  /**
   * A freeze ends on the deadline the fleet honours, not on the one this process
   * happened to time.
   *
   * The drain loop's freeze exits used to book no wake-up at all, leaving the
   * `setTimeout` armed in `handleRequestDone` as the only one — and that timer is
   * local to whichever replica was controller when the 429 landed, is cleared by
   * `destroy()`, and counts a duration in THIS process's frame while `frozenUntil`
   * is written in the backend's. A backend clock running ahead therefore fired it
   * early, the pass it woke found the client still frozen and returned without
   * booking anything, and the queue then slept past the real end of the freeze
   * until the health check: measured as a request timing out at 5001ms with the
   * upstream never called, against 504ms with the clocks agreeing.
   *
   * The skew is injected on `backend.now()` alone, which is exactly the asymmetry —
   * one clock for the deadline, another for the delay. The health check is pushed
   * far out so that nothing but the loop's own booking can recover this, and the
   * admission budget is shorter than the health interval so a lost wake-up fails
   * rather than merely being slow.
   *
   * WHAT THIS DOES NOT COVER. The loop has two freeze exits and this guards one of
   * them: the `canProcess === false` branch. The other is the `ClientFrozenError`
   * catch, taken when a freeze lands between admission and the acquire, and a
   * change that dropped only that booking would leave this test green — verified,
   * by removing each in turn. Covering it needs the freeze to land inside that
   * window, which is a race nobody has made deterministic here; the gap is real and
   * left documented rather than implied.
   */
  it("ends a freeze on the backend's deadline, not on a locally timed delay", async () => {
    upstream.onlyOnce("/skewboom", 429);
    await run(
      {
        // Short interval so the bucket is back almost at once after the freeze:
        // whatever holds the second request, it is not the budget.
        rateLimit: { interval: 500, tokensToAdd: 10, maxTokens: 10 },
        nowSkewMs: 600,
        client: {
          healthCheckIntervalMs: 120_000,
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 50,
            thawRequestCount: 1,
          },
          requestOptions: { cleanupTimeout: 5_000 },
        },
        drainMs: 300,
      },
      async (ctx) => {
        const rejected = await sendCatching(ctx.handler, "/skewboom");
        expect(rejected?.response?.status).toBe(429);
        const armed = await waitFor(async () => !!(await ctx.freeze()));
        expect(armed).toBe(true);

        // Arrives while the client is frozen, so it queues and the pass that
        // looks at it declines on the freeze — the exit that booked nothing.
        const started = Date.now();
        const res = await send(ctx.handler, "/skewbehind");
        expect(res.status).toBe(200);
        // The freeze really is in force for ~1.1s of this process's time, so the
        // lower bound proves it was honoured and the upper bound proves the loop
        // came back for it on its own rather than waiting out anything else.
        expect(Date.now() - started).toBeGreaterThan(400);
        expect(Date.now() - started).toBeLessThan(3_000);
        expect(upstream.calls("/skewbehind")).toBe(1);
      }
    );
  }, 30_000);

  /**
   * A cheap urgent arrival is served in the refill window it arrives in, not the
   * one after the costly head's.
   *
   * Admission used to SLEEP until the head's tokens arrived, holding
   * `processingLock` the whole time, so nothing else was even considered — and
   * when the sleep ended the head spent the entire refill, pushing everything
   * behind it into the next window. The sibling assertion in `auditRound2` checks
   * the ORDER, which a blocking loop eventually gets right too: with the drain
   * wake-up disabled the order still holds and only the test timeout notices. So
   * the bound here is the assertion, and the order is the corroboration.
   *
   * 1.4s sits between the two windows: 800ms is when the urgent request must go,
   * 1.6s is when it goes if it waited behind the head's spend.
   */
  it("serves a cheap urgent arrival in the window it arrives in", async () => {
    await run(
      { rateLimit: { interval: 800, tokensToAdd: 10, maxTokens: 10 } },
      async (ctx) => {
        // One request for the whole budget, so everything below has to queue.
        await send(ctx.handler, "/hol.drain", { cost: 10 });
        expect((await ctx.bucket()).tokens).toBe(0);

        const started = Date.now();
        const heavy = send(ctx.handler, "/hol.heavy", {
          cost: 10,
          priority: 1,
        });
        // Queued behind the head, and behind it in the backend too, so being
        // served first can only come from the queue being re-examined.
        await new Promise((r) => setTimeout(r, 60));
        const urgent = send(ctx.handler, "/hol.urgent", {
          cost: 1,
          priority: 10,
        });

        expect((await urgent).status).toBe(200);
        const urgentAt = Date.now() - started;
        expect((await heavy).status).toBe(200);
        const heavyAt = Date.now() - started;

        expect(urgentAt).toBeLessThan(1_400);
        expect(heavyAt).toBeGreaterThan(urgentAt);
        const order = upstream.urls();
        expect(order.indexOf("/hol.urgent")).toBeLessThan(
          order.indexOf("/hol.heavy")
        );
      }
    );
  }, 30_000);
});
