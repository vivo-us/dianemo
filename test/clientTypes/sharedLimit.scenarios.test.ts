import type { DianemoBackend } from "../../packages/core/src/backend/types.js";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import RequestHandler from "../../packages/core/src/index.js";
import { harnesses, type Harness } from "./harness.js";
import {
  ClientUnavailableError,
  ConfigurationError,
  RequestCostExceedsBudgetError,
  RequestTimeoutError,
} from "../../packages/core/src/errors.js";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";

/**
 * `sharedLimit`, end to end — one budget, several credentials.
 *
 * The existing `sharedLimit` suite addresses every request to the *parent's*
 * name, so it never exercises the child at all: each of its sharing assertions
 * would pass unchanged if the child had quietly been given a budget of its own.
 * This file drives traffic through the child, and through both at once, for
 * every response outcome the request pipeline distinguishes.
 *
 * Two things have to hold simultaneously, and they pull in opposite directions:
 *
 * - **one budget** — the queue, the token bucket / concurrency set, and the
 *   freeze state are the parent's, so parent and children are metered together;
 * - **separate credentials** — the token cache is keyed by the name each client
 *   was *registered* under, so no client ever sends another's access token.
 *
 * Anything that only checks the first is worthless: a child with its own bucket
 * serves every request while the vendor sees double the agreed rate.
 */

const KEY = "0123456789abcdef0123456789abcdef";
const PREFIX = "cts";
const NS = `${PREFIX}:requestHandler`;

const PARENT = "parent:_:a";
const CHILD = "child:_:a";
const CHILD2 = "child2:_:a";

const keys = (client: string) => ({
  bucket: `${NS}:${client}:rateLimit`,
  queue: `${NS}:${client}:queue`,
  meta: `${NS}:${client}:request`,
  freeze: `${NS}:${client}:freezeState`,
  concurrency: `${NS}:${client}:concurrency`,
  oauth2: `${NS}:${client}:oauth2`,
});

const P = keys(PARENT);
const C = keys(CHILD);

const settle = (ms = 60) => new Promise((r) => setTimeout(r, ms));

/**
 * Waits until a freeze is actually recorded, and reports when it lifts.
 *
 * The freeze is applied by the controller off the `requestDone` broadcast, so
 * on Redis it lands a round trip after the rejected request has already
 * returned to its caller. A test that fires the next request immediately is
 * measuring that window, not the propagation it means to check.
 */
async function waitForFreeze(
  backend: DianemoBackend,
  key: string,
  timeoutMs = 3_000
) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const state = await backend.getFreezeState(key);
    if (state && state.frozenUntil > Date.now()) return state;
    await settle(10);
  }
  throw new Error(`No freeze recorded at ${key} within ${timeoutMs}ms`);
}

// ---------------------------------------------------------------- upstream

interface Seen {
  url: string;
  auth?: string;
  at: number;
}

type Route = (
  req: IncomingMessage,
  res: ServerResponse,
  hit: number
) => void | Promise<void>;

const ok: Route = (_req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end('{"ok":true}');
};

/**
 * An upstream whose response the test writes.
 *
 * The per-client-type harness only serves 200s; the outcomes below need
 * statuses, socket resets, hangs and — for the credential checks — a record of
 * what actually arrived on the wire.
 */
async function startServer() {
  const seen: Seen[] = [];
  let route: Route = ok;
  let inFlight = 0;
  let peak = 0;
  /** Releases anything the "never responds" route is holding, so close() can finish. */
  const held = new Set<() => void>();

  const server = createServer((req, res) => {
    const hit = seen.length;
    seen.push({
      url: req.url ?? "",
      auth: req.headers.authorization,
      at: Date.now(),
    });
    inFlight++;
    peak = Math.max(peak, inFlight);
    void (async () => {
      try {
        await route(req, res, hit);
      } catch {
        if (!res.headersSent) res.writeHead(500);
        res.end();
      } finally {
        inFlight--;
      }
    })();
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;

  return {
    baseURL: `http://127.0.0.1:${port}`,
    seen,
    urls: () => seen.map((s) => s.url),
    /** Authorization headers seen for requests whose URL contains `fragment`. */
    authFor: (fragment: string) =>
      seen.filter((s) => s.url.includes(fragment)).map((s) => s.auth),
    setRoute: (next: Route) => {
      route = next;
    },
    /** Holds the connection open until `release()` — an upstream that hangs. */
    hang: (): Route => (_req, res) =>
      new Promise<void>((resolve) => {
        held.add(() => {
          res.destroy();
          resolve();
        });
      }),
    /** Drops every held connection, so a hung request finally fails. */
    release: () => {
      for (const stop of held) stop();
      held.clear();
    },
    peak: () => peak,
    inFlight: () => inFlight,
    reset: () => {
      seen.length = 0;
      peak = 0;
      route = ok;
    },
    close: async () => {
      for (const release of held) release();
      held.clear();
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

type Upstream = Awaited<ReturnType<typeof startServer>>;

// ------------------------------------------------------------------ rigging

interface Spec {
  /** Template name; the client is registered as `<name>:_:a`. */
  name: string;
  rateLimit: Record<string, unknown>;
  token?: string;
  /** Absolute token endpoint, for the credential-separation checks. */
  oauth2Url?: string;
  /** Sets `grantRateLimitBehavior: "isolated"` on the oauth2 auth above. */
  isolatedGrants?: boolean;
  retryOptions?: Record<string, unknown>;
  cleanupTimeout?: number;
  rateLimitChange?: unknown;
}

/**
 * Registers each spec as its own template, so the parent and its children can
 * be added — and removed — independently. `withHandler` in the shared harness
 * builds one client per template with no authentication and no retry knobs,
 * which cannot express a parent/child pair.
 *
 * Registration order is the spec order: a `sharedLimit` client resolves its
 * parent by name at construction, so the parent has to be added first.
 */
async function withPair(
  harness: Harness,
  baseURL: string,
  specs: Spec[],
  fn: (handler: RequestHandler, backend: DianemoBackend) => Promise<void>,
  drainTimeoutMs = 250
) {
  const { backend, cleanup } = await harness.create();
  const handler = new RequestHandler({ key: KEY, backend, keyPrefix: PREFIX });
  handler.setDrainTimeout(drainTimeoutMs);

  for (const spec of specs) {
    await handler.registerClientTemplate(
      spec.name as never,
      ((creds: { instanceId: string }) => [
        {
          name: `${spec.name}:_:${creds.instanceId}`,
          rateLimit: spec.rateLimit,
          requestOptions: {
            defaults: { baseURL },
            ...(spec.cleanupTimeout
              ? { cleanupTimeout: spec.cleanupTimeout }
              : {}),
          },
          ...(spec.retryOptions ? { retryOptions: spec.retryOptions } : {}),
          ...(spec.rateLimitChange
            ? { rateLimitChange: spec.rateLimitChange }
            : {}),
          ...(spec.token
            ? { authentication: { type: "token", token: spec.token } }
            : {}),
          ...(spec.oauth2Url
            ? {
                authentication: {
                  type: "oauth2",
                  clientId: `${spec.name}-id`,
                  clientSecret: `${spec.name}-secret`,
                  ...(spec.isolatedGrants
                    ? { grantRateLimitBehavior: "isolated" }
                    : {}),
                  refreshConfig: {
                    url: spec.oauth2Url,
                    dataLocation: "jsonBody",
                    data: {
                      client_id: "{{clientId}}",
                      client_secret: "{{clientSecret}}",
                    },
                  },
                },
              }
            : {}),
        },
      ]) as never
    );
  }
  await handler.start();
  for (const spec of specs) {
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

const fireAt = (
  handler: RequestHandler,
  clientName: string,
  opts: {
    url?: string;
    cost?: number;
    priority?: number;
    grantId?: string;
  } = {}
) =>
  handler.handleRequest({
    clientName,
    requestName: "t.noop",
    method: "GET",
    url: opts.url ?? "/",
    ...(opts.cost !== undefined ? { cost: opts.cost } : {}),
    ...(opts.priority !== undefined ? { priority: opts.priority } : {}),
    ...(opts.grantId !== undefined ? { grantId: opts.grantId } : {}),
  });

const manyAt = (
  handler: RequestHandler,
  clientName: string,
  count: number,
  url?: string
) =>
  Promise.all(
    Array.from({ length: count }, (_, i) =>
      fireAt(handler, clientName, { url: url ? `${url}${i}` : undefined })
    )
  );

const bucket = (interval: number, tokens: number) => ({
  maxTokens: tokens,
  tokensToAdd: tokens,
  interval,
});

/** How much of the bucket is gone, so assertions read as "spent N". */
async function spent(
  backend: DianemoBackend,
  interval: number,
  tokens: number,
  key = P.bucket
) {
  const state = await backend.getTokenBucketState(
    key,
    bucket(interval, tokens)
  );
  return tokens - state.tokens;
}

// -------------------------------------------------------------------- suite

describe.each(harnesses(12))("sharedLimit scenarios — $name", (harness) => {
  let up: Upstream;

  beforeAll(async () => {
    up = await startServer();
  });
  afterAll(async () => {
    await up.close();
  });
  afterEach(() => {
    up.reset();
  });

  /** A `requestLimit` parent plus one `sharedLimit` child, both authenticated. */
  const pair = (
    parentLimit: Record<string, unknown>,
    fn: (handler: RequestHandler, backend: DianemoBackend) => Promise<void>,
    extra: { parent?: Partial<Spec>; child?: Partial<Spec> } = {}
  ) =>
    withPair(
      harness,
      up.baseURL,
      [
        {
          name: "parent",
          rateLimit: { type: "requestLimit", ...parentLimit },
          token: "PARENT-TOKEN",
          ...extra.parent,
        },
        {
          name: "child",
          rateLimit: { type: "sharedLimit", clientName: PARENT },
          token: "CHILD-TOKEN",
          ...extra.child,
        },
      ],
      fn
    );

  // =========================================================== one budget

  it("spends the parent's bucket for a request addressed to the CHILD", async () => {
    // The existing suite only ever addresses the parent, so this — the actual
    // premise of the type — was untested. A child with its own bucket leaves
    // the parent's untouched and still answers 200.
    await pair(
      { interval: 60_000, tokensToAdd: 20, maxTokens: 20 },
      async (handler, backend) => {
        const results = await manyAt(handler, CHILD, 6);
        expect(results.every((r) => r.status === 200)).toBe(true);
        expect(await spent(backend, 60_000, 20)).toBe(6);
        // Not merely "the child's bucket is empty": a bucket key that never
        // existed and one that exists at full are indistinguishable by tokens.
        expect(await backend.hgetall(C.bucket)).toEqual({});
      }
    );
  }, 30_000);

  it("caps parent and child together, not one budget each", async () => {
    // Four tokens, no refill inside the test. Two from each side empties it;
    // the fifth cannot be admitted and dies on its admission timeout. With a
    // bucket each, that fifth request would sail through.
    await pair(
      { interval: 600_000, tokensToAdd: 4, maxTokens: 4 },
      async (handler, backend) => {
        await manyAt(handler, PARENT, 2);
        await manyAt(handler, CHILD, 2);
        expect(await spent(backend, 600_000, 4)).toBe(4);

        await expect(fireAt(handler, CHILD, { url: "/fifth" })).rejects.toThrow(
          RequestTimeoutError
        );
        expect(up.urls()).not.toContain("/fifth");
      },
      { child: { cleanupTimeout: 900 } }
    );
  }, 30_000);

  it("meters two children and the parent against the one budget", async () => {
    await withPair(
      harness,
      up.baseURL,
      [
        {
          name: "parent",
          rateLimit: {
            type: "requestLimit",
            interval: 600_000,
            tokensToAdd: 3,
            maxTokens: 3,
          },
        },
        {
          name: "child",
          rateLimit: { type: "sharedLimit", clientName: PARENT },
          cleanupTimeout: 900,
        },
        {
          name: "child2",
          rateLimit: { type: "sharedLimit", clientName: PARENT },
          cleanupTimeout: 900,
        },
      ],
      async (handler, backend) => {
        await fireAt(handler, PARENT);
        await fireAt(handler, CHILD);
        await fireAt(handler, CHILD2);
        expect(await spent(backend, 600_000, 3)).toBe(3);
        expect(await backend.hgetall(C.bucket)).toEqual({});
        expect(await backend.hgetall(keys(CHILD2).bucket)).toEqual({});

        // Three siblings, three tokens: the fourth request from any of them has
        // nothing left to spend.
        await expect(fireAt(handler, CHILD2, { url: "/x" })).rejects.toThrow(
          RequestTimeoutError
        );
      }
    );
  }, 30_000);

  it("queues the child's request in the parent's queue, not one of its own", async () => {
    await pair(
      { interval: 600_000, tokensToAdd: 1, maxTokens: 1 },
      async (handler, backend) => {
        await fireAt(handler, PARENT, { url: "/first" });

        const blocked = fireAt(handler, CHILD, { url: "/blocked" }).catch(
          (e) => e
        );
        await settle(150);
        const parentQueue = await backend.getQueueStats(P.queue, P.meta);
        const childQueue = await backend.getQueueStats(C.queue, C.meta);
        expect(parentQueue.pending).toBe(1);
        expect(childQueue.pending + childQueue.inProgress).toBe(0);

        expect(await blocked).toBeInstanceOf(RequestTimeoutError);
      },
      { child: { cleanupTimeout: 700 } }
    );
  }, 30_000);

  it("has no fast path of its own: the child always goes through the queue", async () => {
    // A `sharedLimit` client cannot decide admission atomically — the budget is
    // not its own — so it inherits the base `tryAdmitImmediately`, which always
    // declines. The parent, uncontended, skips the queue entirely; the child in
    // the same state does not. Both requests are metered the same either way,
    // so this is about the path, not the budget.
    await pair(
      { interval: 600_000, tokensToAdd: 5, maxTokens: 5 },
      async (handler, backend) => {
        up.setRoute(async (req, res) => {
          await settle(250);
          ok(req, res, 0);
        });

        const viaParent = fireAt(handler, PARENT, { url: "/p" });
        await settle(120);
        const duringParent = await backend.getQueueStats(P.queue, P.meta);
        await viaParent;

        const viaChild = fireAt(handler, CHILD, { url: "/c" });
        await settle(120);
        const duringChild = await backend.getQueueStats(P.queue, P.meta);
        await viaChild;

        expect(duringParent.pending + duringParent.inProgress).toBe(0);
        expect(duringChild.inProgress).toBe(1);
      }
    );
  }, 30_000);

  it("lets a high-priority child request overtake a low-priority parent one", async () => {
    // One token per interval. Both sides queue behind the same score, so the
    // ordering is only right if they really are in one queue.
    await pair(
      { interval: 700, tokensToAdd: 1, maxTokens: 1 },
      async (handler) => {
        await fireAt(handler, PARENT, { url: "/warm" });
        const lo = fireAt(handler, PARENT, { url: "/lo", priority: 1 });
        await settle(80);
        const hi = fireAt(handler, CHILD, { url: "/hi", priority: 5 });
        await Promise.all([lo, hi]);
        const order = up.urls().filter((u) => u !== "/warm");
        expect(order).toEqual(["/hi", "/lo"]);
      }
    );
  }, 30_000);

  // ================================================== separate credentials

  it("attaches the child's own token, never the parent's", async () => {
    await pair(
      { interval: 60_000, tokensToAdd: 20, maxTokens: 20 },
      async (handler) => {
        await fireAt(handler, PARENT, { url: "/as-parent" });
        await fireAt(handler, CHILD, { url: "/as-child" });
        expect(up.authFor("/as-parent")).toEqual(["Bearer PARENT-TOKEN"]);
        expect(up.authFor("/as-child")).toEqual(["Bearer CHILD-TOKEN"]);
      }
    );
  }, 30_000);

  it("caches each side's OAuth2 token under its own registered name", async () => {
    // The hazard this guards: one credential entry for two clients means
    // whichever refreshes first wins and the other attaches that token to its
    // own requests — which the vendor accepts, so nothing surfaces as an error.
    up.setRoute((req, res) => {
      const url = req.url ?? "";
      if (url.startsWith("/token")) {
        const who = new URL(url, "http://x").searchParams.get("who");
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            access_token: `${who}-access`,
            expires_in: 3600,
            token_type: "Bearer",
          })
        );
        return;
      }
      ok(req, res, 0);
    });

    await withPair(
      harness,
      up.baseURL,
      [
        {
          name: "parent",
          rateLimit: {
            type: "requestLimit",
            interval: 60_000,
            tokensToAdd: 20,
            maxTokens: 20,
          },
          oauth2Url: `${up.baseURL}/token?who=parent`,
        },
        {
          name: "child",
          rateLimit: { type: "sharedLimit", clientName: PARENT },
          oauth2Url: `${up.baseURL}/token?who=child`,
        },
      ],
      async (handler, backend) => {
        await fireAt(handler, PARENT, { url: "/p" });
        await fireAt(handler, CHILD, { url: "/c" });

        expect(up.authFor("/p")).toEqual(["Bearer parent-access"]);
        expect(up.authFor("/c")).toEqual(["Bearer child-access"]);

        // Two entries, not one shared entry.
        const parentCred = await backend.hgetall(P.oauth2);
        const childCred = await backend.hgetall(C.oauth2);
        expect(parentCred.accessToken).toBeTruthy();
        expect(childCred.accessToken).toBeTruthy();
        expect(childCred.accessToken).not.toBe(parentCred.accessToken);
      }
    );
  }, 30_000);

  // ============================================================== outcomes

  it("passes a 400 straight back and neither freezes nor retries the shared budget", async () => {
    await pair(
      { interval: 600_000, tokensToAdd: 10, maxTokens: 10 },
      async (handler, backend) => {
        up.setRoute((_req, res) => {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end('{"error":"bad"}');
        });
        await expect(fireAt(handler, CHILD, { url: "/bad" })).rejects.toThrow();
        await settle(120);
        // One attempt, one token, no freeze on anyone's budget.
        expect(up.urls()).toEqual(["/bad"]);
        expect(await spent(backend, 600_000, 10)).toBe(1);
        expect(await backend.getFreezeState(P.freeze)).toBeNull();
        expect(await backend.getFreezeState(C.freeze)).toBeNull();
        // The entry the child queued is the parent's to remove.
        const stats = await backend.getQueueStats(P.queue, P.meta);
        expect(stats.pending + stats.inProgress).toBe(0);
      }
    );
  }, 30_000);

  it.each([401, 404])(
    "passes a %i back without touching the shared freeze state",
    async (status) => {
      await pair(
        { interval: 600_000, tokensToAdd: 10, maxTokens: 10 },
        async (handler, backend) => {
          up.setRoute((_req, res) => {
            res.writeHead(status);
            res.end("{}");
          });
          await expect(
            fireAt(handler, CHILD, { url: `/s${status}` })
          ).rejects.toThrow();
          await settle(120);
          expect(up.urls()).toEqual([`/s${status}`]);
          expect(await backend.getFreezeState(P.freeze)).toBeNull();
          expect(await spent(backend, 600_000, 10)).toBe(1);
        }
      );
    },
    30_000
  );

  it("does not retry a 409 by default", async () => {
    await pair(
      { interval: 600_000, tokensToAdd: 10, maxTokens: 10 },
      async (handler, backend) => {
        up.setRoute((_req, res) => {
          res.writeHead(409);
          res.end("{}");
        });
        await expect(fireAt(handler, CHILD, { url: "/c" })).rejects.toThrow();
        await settle(120);
        expect(up.urls()).toEqual(["/c"]);
        expect(await spent(backend, 600_000, 10)).toBe(1);
      }
    );
  }, 30_000);

  it("spends a shared token per attempt when the child opts into retrying 409", async () => {
    // Two things at once, and the second is a guard rail.
    //
    // The accounting: a child's retry policy is the child's, but the tokens the
    // retries burn come out of the parent's budget, so a chatty child can starve
    // its siblings.
    //
    // The pacing: an opted-in retry must stay on the CHILD's configured
    // back-off, never the parent's refill interval. This is the test that caught
    // the first attempt at the freeze-duration fix — flooring the child's
    // `getRetryBackoffBaseTime` at the parent's interval made every attempt here
    // wait a full 600s window and the test timed out at 30s. The elapsed
    // assertion below turns that into a clear failure instead of a timeout, so
    // the next attempt says what went wrong. Note the parent's interval is
    // deliberately enormous relative to the child's 20ms base.
    await pair(
      { interval: 600_000, tokensToAdd: 10, maxTokens: 10 },
      async (handler, backend) => {
        up.setRoute((_req, res) => {
          res.writeHead(409);
          res.end("{}");
        });
        const started = Date.now();
        await expect(fireAt(handler, CHILD, { url: "/c" })).rejects.toThrow();
        const elapsed = Date.now() - started;
        await settle(150);
        // Two retries plus the original attempt.
        expect(up.urls()).toEqual(["/c", "/c", "/c"]);
        expect(await spent(backend, 600_000, 10)).toBe(3);
        // 20ms then 80ms of back-off, not one refill interval per attempt.
        expect(elapsed).toBeLessThan(2_000);
        // A consumer-requested retry says nothing about the upstream, so the
        // shared budget must not be frozen by it — which is also why a change to
        // the FREEZE duration cannot affect this path: with `freezeClient`
        // false, `handleRequestDone` never reads `freezeTime` at all.
        expect(await backend.getFreezeState(P.freeze)).toBeNull();
      },
      {
        child: {
          retryOptions: {
            retryStatusCodes: [409],
            maxRetries: 2,
            retryBackoffBaseTime: 20,
          },
        },
      }
    );
  }, 30_000);

  // ================================================================ freeze

  it("freezes the SHARED budget when the child is rate limited", async () => {
    await pair(
      { interval: 200, tokensToAdd: 10, maxTokens: 10 },
      async (handler, backend) => {
        up.setRoute((_req, res) => {
          res.writeHead(429);
          res.end("{}");
        });
        await expect(fireAt(handler, CHILD, { url: "/429" })).rejects.toThrow();

        const state = await waitForFreeze(backend, P.freeze);
        expect(state.frozenUntil).toBeGreaterThan(Date.now());
        // Deliberately not the child's namespace: sharing the budget means
        // sharing the freeze, and a child-local freeze would leave the parent
        // hammering an API that just said stop.
        expect(await backend.getFreezeState(C.freeze)).toBeNull();
      },
      {
        child: {
          retryOptions: { maxRetries: 0, retryBackoffBaseTime: 1_200 },
        },
      }
    );
  }, 30_000);

  it("holds the parent back while a child's 429 freeze is in force", async () => {
    await pair(
      { interval: 100, tokensToAdd: 20, maxTokens: 20 },
      async (handler, backend) => {
        let mode: "429" | "ok" = "429";
        up.setRoute((req, res) => {
          if (mode === "429" && (req.url ?? "").startsWith("/child")) {
            res.writeHead(429);
            res.end("{}");
            return;
          }
          ok(req, res, 0);
        });
        await expect(
          fireAt(handler, CHILD, { url: "/child" })
        ).rejects.toThrow();
        mode = "ok";
        const frozen = await waitForFreeze(backend, P.freeze);
        const remaining = frozen.frozenUntil - Date.now();
        expect(remaining).toBeGreaterThan(200);

        const started = Date.now();
        const res = await fireAt(handler, PARENT, { url: "/parent" });
        expect(res.status).toBe(200);
        // The parent's own credential was never rate limited, but the budget
        // was — so its request must sit out the rest of the freeze rather than
        // going straight back at a vendor that just said stop.
        expect(Date.now() - started).toBeGreaterThanOrEqual(remaining - 50);
      },
      { child: { retryOptions: { maxRetries: 0, retryBackoffBaseTime: 800 } } }
    );
  }, 30_000);

  it("holds every child back while a 429 on the PARENT is in force", async () => {
    await pair(
      { interval: 100, tokensToAdd: 20, maxTokens: 20 },
      async (handler, backend) => {
        let mode: "429" | "ok" = "429";
        up.setRoute((req, res) => {
          if (mode === "429" && (req.url ?? "").startsWith("/parent")) {
            res.writeHead(429);
            res.end("{}");
            return;
          }
          ok(req, res, 0);
        });
        await expect(
          fireAt(handler, PARENT, { url: "/parent" })
        ).rejects.toThrow();
        mode = "ok";
        const frozen = await waitForFreeze(backend, P.freeze);
        const remaining = frozen.frozenUntil - Date.now();
        expect(remaining).toBeGreaterThan(200);

        const started = Date.now();
        const res = await fireAt(handler, CHILD, { url: "/child" });
        expect(res.status).toBe(200);
        expect(Date.now() - started).toBeGreaterThanOrEqual(remaining - 50);
      },
      { parent: { retryOptions: { maxRetries: 0, retryBackoffBaseTime: 800 } } }
    );
  }, 30_000);

  it("takes the thaw count from the budget owner, not from the child that was 429'd", async () => {
    // The freeze is assembled from both sides: `waitTime` and the decision to
    // freeze come from the client that got the 429, while `thawRequestCount`
    // is read off the client that owns the budget. Worth pinning, because it
    // is the half of the freeze the child cannot influence.
    await pair(
      { interval: 100, tokensToAdd: 20, maxTokens: 20 },
      async (handler, backend) => {
        up.setRoute((_req, res) => {
          res.writeHead(429);
          res.end("{}");
        });
        await expect(fireAt(handler, CHILD, { url: "/429" })).rejects.toThrow();
        const state = await waitForFreeze(backend, P.freeze);
        expect(state.thawRequestCount).toBe(7);
      },
      {
        parent: { retryOptions: { thawRequestCount: 7 } },
        child: {
          retryOptions: {
            maxRetries: 0,
            thawRequestCount: 1,
            retryBackoffBaseTime: 1_000,
          },
        },
      }
    );
  }, 30_000);

  it("freezes the shared budget for the same length whichever credential is 429'd", async () => {
    // One upstream, one budget, one recovery window — regardless of which
    // credential the vendor happened to reject. A freeze whose length depends on
    // which of several credentials received the 429 is not one shared budget.
    //
    // The freeze written to the SHARED state is `handleBackoff`'s `freezeTime`,
    // taken from the receiving client's `getFreezeBaseTime()`. A `requestLimit`
    // budget floors that at its refill interval, because resuming before the
    // bucket refills cannot help, and a client sharing that budget has to answer
    // the same way — hence both sides here are configured with a 100ms back-off
    // and both must record the parent's 4000ms interval.
    //
    // Only the FREEZE is floored. Retry pacing stays on each client's configured
    // value, which is what lets an opted-in 409 retry go again immediately
    // instead of waiting out a refill interval it has no reason to. `spends a
    // shared token per attempt when the child opts into retrying 409` is the
    // guard for that half; the two tests constrain each other and should be read
    // together.
    const freezeAfter429At = async (clientName: string) => {
      let observed = 0;
      await pair(
        { interval: 4_000, tokensToAdd: 5, maxTokens: 5 },
        async (handler, backend) => {
          up.setRoute((_req, res) => {
            res.writeHead(429);
            res.end("{}");
          });
          // `frozenUntil` is `backend.now() + freezeFor`, so the baseline has to
          // come from the same clock. Measuring from `Date.now()` put the local
          // clock against Redis server TIME and lost a few ms to skew, which is
          // enough to make an exact floor flap.
          const before = await backend.now();
          await expect(
            fireAt(handler, clientName, { url: "/429" })
          ).rejects.toThrow();
          const state = await waitForFreeze(backend, P.freeze);
          observed = state.frozenUntil - before;
        },
        {
          parent: {
            retryOptions: { maxRetries: 0, retryBackoffBaseTime: 100 },
          },
          child: {
            retryOptions: { maxRetries: 0, retryBackoffBaseTime: 100 },
          },
        }
      );
      up.reset();
      return observed;
    };

    const viaParent = await freezeAfter429At(PARENT);
    const viaChild = await freezeAfter429At(CHILD);
    // Both floored at the parent's 4000ms interval rather than the 100ms each has
    // configured. Exact, because both readings now come off the backend's own
    // clock.
    expect(viaParent).toBeGreaterThanOrEqual(4_000);
    expect(viaChild).toBeGreaterThanOrEqual(4_000);
    // And they agree with each other, which is the invariant: one budget, one
    // recovery window. Checking only that the child is no shorter would pass if
    // both had collapsed to the raw 100ms back-off.
    expect(viaChild).toBeGreaterThanOrEqual(viaParent * 0.9);
    expect(viaChild).toBeLessThanOrEqual(viaParent * 1.1);
  }, 30_000);

  it("freezes the shared budget on a 429 even when the child retries immediately", async () => {
    // A freeze is armed whenever the failure warrants one and a duration exists
    // for it, independent of how the failing request paces its own retry. The
    // two are separate questions: "should everyone sharing this budget stop"
    // and "how soon does this one request go again".
    //
    // `retryBackoffBaseTime: 0` is legal — `?? 1000` only defaults
    // null/undefined — and answers the second question with "immediately". It
    // says nothing about the first, so it must not disarm the freeze for the
    // other credentials on the budget, and the freeze it does arm has to be the
    // budget owner's interval rather than this client's zero.
    await pair(
      { interval: 4_000, tokensToAdd: 5, maxTokens: 5 },
      async (handler, backend) => {
        up.setRoute((_req, res) => {
          res.writeHead(429);
          res.end("{}");
        });
        await expect(fireAt(handler, CHILD, { url: "/429" })).rejects.toThrow();
        await settle(250);
        // The vendor said stop, so the shared budget is paused for every
        // credential drawing on it, whatever this one configured for its own
        // retry pacing — and for the budget owner's interval, not for zero.
        const state = await backend.getFreezeState(P.freeze);
        expect(state).not.toBeNull();
        expect(state!.frozenUntil).toBeGreaterThan(Date.now() + 3_000);
      },
      { child: { retryOptions: { maxRetries: 0, retryBackoffBaseTime: 0 } } }
    );
  }, 30_000);

  it("probes recovery one request at a time across parent and child", async () => {
    await pair(
      { interval: 60, tokensToAdd: 30, maxTokens: 30 },
      async (handler, backend) => {
        up.setRoute((_req, res) => {
          res.writeHead(429);
          res.end("{}");
        });
        await expect(fireAt(handler, CHILD, { url: "/429" })).rejects.toThrow();
        // The freeze has to be in force before the probes are queued, or they
        // are ordinary traffic and this measures nothing.
        const state = await waitForFreeze(backend, P.freeze);
        expect(state.thawRequestCount).toBe(3);

        up.reset();
        up.setRoute(async (req, res) => {
          await settle(60);
          ok(req, res, 0);
        });

        // Exactly the probe budget, split across both credentials: every one of
        // them is a thaw probe, and probes must not overlap.
        const results = await Promise.all([
          fireAt(handler, CHILD, { url: "/c0" }),
          fireAt(handler, PARENT, { url: "/p0" }),
          fireAt(handler, CHILD, { url: "/c1" }),
        ]);
        expect(results.every((r) => r.status === 200)).toBe(true);
        expect(up.peak()).toBe(1);
        // Three successful probes clear the shared freeze for everyone. The
        // last decrement rides the `requestDone` round trip, which is a real
        // network hop on Redis.
        await settle(250);
        expect(await backend.getFreezeState(P.freeze)).toBeNull();
      },
      { child: { retryOptions: { maxRetries: 0, retryBackoffBaseTime: 300 } } }
    );
  }, 30_000);

  it.each([500, 502, 503])(
    "freezes the shared budget on a %i from the child",
    async (status) => {
      await pair(
        { interval: 100, tokensToAdd: 20, maxTokens: 20 },
        async (handler, backend) => {
          up.setRoute((_req, res) => {
            res.writeHead(status);
            res.end("{}");
          });
          await expect(
            fireAt(handler, CHILD, { url: `/s${status}` })
          ).rejects.toThrow();
          const state = await waitForFreeze(backend, P.freeze);
          expect(state.frozenUntil).toBeGreaterThan(Date.now());
          // A 5xx is downtime, not rate limiting, so no probe budget is armed.
          expect(state.thawRequestCount).toBe(0);
          expect(await backend.getFreezeState(C.freeze)).toBeNull();
        },
        {
          child: {
            retryOptions: { maxRetries: 0, retryBackoffBaseTime: 900 },
          },
        }
      );
    },
    30_000
  );

  it("freezes the shared budget when the child's socket is reset", async () => {
    await pair(
      { interval: 100, tokensToAdd: 20, maxTokens: 20 },
      async (handler, backend) => {
        up.setRoute((req) => {
          req.socket.destroy();
        });
        await expect(fireAt(handler, CHILD, { url: "/reset" })).rejects.toThrow(
          /socket hang up|ECONNRESET|aborted/i
        );
        const state = await waitForFreeze(backend, P.freeze);
        expect(state.frozenUntil).toBeGreaterThan(Date.now());
      },
      { child: { retryOptions: { maxRetries: 0, retryBackoffBaseTime: 900 } } }
    );
  }, 30_000);

  // ============================================================== cost

  it("rejects a cost above the PARENT's ceiling at the child", async () => {
    await pair(
      { interval: 1_000, tokensToAdd: 5, maxTokens: 5 },
      async (handler) => {
        // The child has no ceiling of its own to check against, so it has to
        // resolve the parent's — otherwise this queues forever.
        await expect(
          fireAt(handler, CHILD, { url: "/big", cost: 6 })
        ).rejects.toBeInstanceOf(RequestCostExceedsBudgetError);
        expect(up.urls()).toEqual([]);
      }
    );
  }, 30_000);

  it("admits a cost exactly equal to the parent's ceiling", async () => {
    await pair(
      { interval: 600_000, tokensToAdd: 5, maxTokens: 5 },
      async (handler, backend) => {
        const res = await fireAt(handler, CHILD, { url: "/exact", cost: 5 });
        expect(res.status).toBe(200);
        expect(await spent(backend, 600_000, 5)).toBe(5);
      }
    );
  }, 30_000);

  it("meters a fractional cost against the shared bucket", async () => {
    await pair(
      { interval: 600_000, tokensToAdd: 4, maxTokens: 4 },
      async (handler, backend) => {
        await fireAt(handler, CHILD, { url: "/half", cost: 0.5 });
        await fireAt(handler, PARENT, { url: "/quarter", cost: 0.25 });
        expect(await spent(backend, 600_000, 4)).toBeCloseTo(0.75, 5);
      }
    );
  }, 30_000);

  // ================================================== concurrencyLimit parent

  const concurrencyPair = (
    maxConcurrency: number,
    fn: (handler: RequestHandler, backend: DianemoBackend) => Promise<void>,
    childCleanupTimeout?: number
  ) =>
    withPair(
      harness,
      up.baseURL,
      [
        {
          name: "parent",
          rateLimit: { type: "concurrencyLimit", maxConcurrency },
        },
        {
          name: "child",
          rateLimit: { type: "sharedLimit", clientName: PARENT },
          ...(childCleanupTimeout
            ? { cleanupTimeout: childCleanupTimeout }
            : {}),
        },
      ],
      fn
    );

  it("never exceeds a concurrencyLimit parent's cap across parent and child", async () => {
    await concurrencyPair(2, async (handler) => {
      up.setRoute(async (req, res) => {
        await settle(120);
        ok(req, res, 0);
      });
      await Promise.all([
        manyAt(handler, PARENT, 4, "/p"),
        manyAt(handler, CHILD, 4, "/c"),
      ]);
      // Eight requests, cap of two. Its own concurrency key would show four.
      expect(up.peak()).toBeLessThanOrEqual(2);
      expect(up.seen).toHaveLength(8);
    });
  }, 30_000);

  it("returns a slot the child abandoned on its admission timeout", async () => {
    await concurrencyPair(
      1,
      async (handler, backend) => {
        up.setRoute(async (req, res) => {
          if ((req.url ?? "").startsWith("/slow")) await settle(700);
          ok(req, res, 0);
        });
        const slow = fireAt(handler, PARENT, { url: "/slow" });
        await settle(80);
        await expect(
          fireAt(handler, CHILD, { url: "/timeout" })
        ).rejects.toThrow(RequestTimeoutError);
        await slow;
        await settle(150);

        // The child announces the abandonment under the parent's name, which is
        // the only way the parent can hand the slot back. Without that the cap
        // is permanently one lower and the next request never runs.
        const state = await backend.getConcurrencyState(P.concurrency, 120_000);
        expect(state.currentConcurrency).toBe(0);
        const queue = await backend.getQueueStats(P.queue, P.meta);
        expect(queue.pending + queue.inProgress).toBe(0);
        const res = await fireAt(handler, CHILD, { url: "/after" });
        expect(res.status).toBe(200);
      },
      300
    );
  }, 30_000);

  it("keeps no concurrency set of its own", async () => {
    await concurrencyPair(2, async (handler, backend) => {
      up.setRoute(async (req, res) => {
        await settle(60);
        ok(req, res, 0);
      });
      await manyAt(handler, CHILD, 4, "/c");
      const child = await backend.getConcurrencyState(C.concurrency, 120_000);
      expect(child.activeRequests).toEqual([]);
    });
  }, 30_000);

  // ============================================================= admission

  it("fails a child's request when the upstream never answers past its admission timeout", async () => {
    // The hang is after admission, so the token is already spent; what has to
    // hold is that the shared budget is not corrupted by the abandonment and
    // the parent keeps serving once the hang clears.
    await pair(
      { interval: 600_000, tokensToAdd: 1, maxTokens: 1 },
      async (handler, backend) => {
        up.setRoute(up.hang());
        const hung = fireAt(handler, CHILD, { url: "/hang" }).catch((e) => e);
        await settle(120);
        // The one token is gone, so a second request cannot be admitted at all.
        await expect(
          fireAt(handler, PARENT, { url: "/behind" })
        ).rejects.toThrow(RequestTimeoutError);
        expect(await spent(backend, 600_000, 1)).toBe(1);
        expect(up.urls()).toEqual(["/hang"]);

        up.release();
        expect(await hung).toBeInstanceOf(Error);
      },
      {
        parent: { cleanupTimeout: 700 },
        child: {
          cleanupTimeout: 700,
          retryOptions: { maxRetries: 0, retryBackoffBaseTime: 50 },
        },
      }
    );
  }, 30_000);

  // ========================================================= role and shape

  it("is never the controller for the shared queue", async () => {
    await pair(
      { interval: 60_000, tokensToAdd: 10, maxTokens: 10 },
      async (handler) => {
        const owned = handler.getMetadata().ownedClients;
        expect(owned).toContain(PARENT);
        expect(owned).not.toContain(CHILD);
      }
    );
  }, 30_000);

  it("lists the child under its own registered name", async () => {
    await pair(
      { interval: 60_000, tokensToAdd: 10, maxTokens: 10 },
      async (handler) => {
        const names = handler.getLoadedClients().map((c) => c.name);
        expect(names).toContain(PARENT);
        expect(names).toContain(CHILD);
      }
    );
  }, 30_000);

  it("lets a child's rateLimitChange rewrite the PARENT's budget", async () => {
    // `updateRateLimit` publishes under `this.name`, which for a shared client
    // is the parent's, and the handler dispatches by that name — so a dynamic
    // adjustment parsed out of one credential's response headers becomes the
    // budget every sibling is metered against. Defensible for a shared quota,
    // but it is nowhere in the docs, it is not symmetric (the child's callback
    // silently outranks the parent's own limit), and a child returning a
    // `sharedLimit` object here is dropped without a word.
    await pair(
      { interval: 60_000, tokensToAdd: 10, maxTokens: 10 },
      async (handler) => {
        await fireAt(handler, CHILD, { url: "/hdr" });
        await settle(150);
        const parentStats = await handler.getClientStats(PARENT);
        expect(parentStats.rateLimit).toMatchObject({
          type: "requestLimit",
          maxTokens: 2,
        });
        // The child's own reported limit is untouched: it never has a budget.
        const childStats = await handler.getClientStats(CHILD);
        expect(childStats.rateLimit).toMatchObject({ type: "sharedLimit" });
      },
      {
        child: {
          rateLimitChange: () => ({
            type: "requestLimit",
            interval: 60_000,
            tokensToAdd: 2,
            maxTokens: 2,
          }),
        },
      }
    );
  }, 30_000);

  it("reports the child's stats under its own registered name", async () => {
    // `getStats().clientName` has to agree with `getName()`. Reporting
    // `this.name` filed the child's numbers under the parent, so a host
    // mirroring per-client stats saw two entries for one name and never saw the
    // child's own — the same defect `getName()` was changed to fix, one method
    // over.
    await pair(
      { interval: 60_000, tokensToAdd: 10, maxTokens: 10 },
      async (handler) => {
        const childStats = await handler.getClientStats(CHILD);
        const parentStats = await handler.getClientStats(PARENT);
        expect(childStats.clientName).toBe(CHILD);
        expect(parentStats.clientName).toBe(PARENT);
      }
    );
  }, 30_000);

  it("keeps the shared queue clean when the parent is rebuilt under an in-flight child request", async () => {
    // A rebuild of the *parent* — a credential re-broadcast, a rate-limit
    // override — destroys and recreates the client that owns the queue, while
    // the child is untouched and still sending. If the child's `requestDone`
    // lands while the parent is out of the registry it is dropped, and the
    // entry it should have removed stays `inProgress` in a queue that only its
    // 24h TTL will clear — which permanently disables the parent's fast path.
    await pair(
      { interval: 1_000, tokensToAdd: 10, maxTokens: 10 },
      async (handler, backend) => {
        up.setRoute(async (req, res) => {
          await settle(300);
          ok(req, res, 0);
        });
        const inFlight = fireAt(handler, CHILD, { url: "/slow" });
        await settle(80);

        // Same credentials, different budget: `clientDataEqual` sees a change
        // and `generateClients` resets the parent.
        await handler.addTemplateClient(
          "parent" as never,
          { instanceId: "a" } as never,
          {
            "": {
              type: "requestLimit",
              interval: 1_000,
              tokensToAdd: 7,
              maxTokens: 7,
            },
          }
        );

        expect((await inFlight).status).toBe(200);
        await settle(300);
        const queue = await backend.getQueueStats(P.queue, P.meta);
        expect(queue.pending + queue.inProgress).toBe(0);

        // The rebuild really happened, and the child resolves the parent's
        // ceiling live rather than caching the instance it was built against.
        const parentStats = await handler.getClientStats(PARENT);
        expect(parentStats.rateLimit).toMatchObject({ maxTokens: 7 });
        up.setRoute(ok);
        await expect(
          fireAt(handler, CHILD, { url: "/toobig", cost: 8 })
        ).rejects.toBeInstanceOf(RequestCostExceedsBudgetError);
        expect((await fireAt(handler, CHILD, { url: "/after" })).status).toBe(
          200
        );
      }
    );
  }, 30_000);

  // ===================================================== grant isolation

  /**
   * A grant-isolated `oauth2` parent with a `token`-authenticated child.
   *
   * Isolation splits the budget keys per grant, and which keys those are is
   * decided by `usesGrantIsolation()`. The child's `namespace` is the parent's
   * but its `authData` is its own, so a child answering that question from its
   * own credentials would resolve the un-isolated key while the parent resolved
   * the isolated one — and grant-scoped traffic would quietly stop being
   * shared. The differently-authenticated child is the whole point of the
   * fixture: `token` auth can never be "isolated", so the two sides only agree
   * if the decision is read off the budget owner.
   */
  const isolatedPair = (
    tokensPerGrant: number,
    fn: (handler: RequestHandler, backend: DianemoBackend) => Promise<void>,
    childRetryOptions?: Record<string, unknown>
  ) =>
    withPair(
      harness,
      up.baseURL,
      [
        {
          name: "parent",
          rateLimit: {
            type: "requestLimit",
            interval: 600_000,
            tokensToAdd: tokensPerGrant,
            maxTokens: tokensPerGrant,
          },
          oauth2Url: `${up.baseURL}/token?who=parent`,
          isolatedGrants: true,
          cleanupTimeout: 900,
        },
        {
          name: "child",
          rateLimit: { type: "sharedLimit", clientName: PARENT },
          token: "CHILD-TOKEN",
          cleanupTimeout: 900,
          ...(childRetryOptions ? { retryOptions: childRetryOptions } : {}),
        },
      ],
      fn
    );

  /** Seeds a grant's tokens so nothing has to hit the token endpoint mid-test. */
  const seedGrant = (handler: RequestHandler, grantId: string) =>
    handler.setGrantTokens(PARENT, grantId, {
      accessToken: `${grantId}-access`,
      refreshToken: `${grantId}-refresh`,
      tokenType: "Bearer",
      expiresAt: Date.now() + 3_600_000,
      refreshTokenExpiresAt: Date.now() + 86_400_000,
    });

  it("meters a child's grant traffic against the parent's isolated grant bucket", async () => {
    await isolatedPair(2, async (handler, backend) => {
      await seedGrant(handler, "g1");
      const grantBucket = `${NS}:${PARENT}:grant:g1:rateLimit`;

      const fromParent = await fireAt(handler, PARENT, {
        url: "/p",
        grantId: "g1",
      });
      const fromChild = await fireAt(handler, CHILD, {
        url: "/c",
        grantId: "g1",
      });
      expect(fromParent.status).toBe(200);
      expect(fromChild.status).toBe(200);

      // Each side still authenticates as itself.
      expect(up.authFor("/p")).toEqual(["Bearer g1-access"]);
      expect(up.authFor("/c")).toEqual(["Bearer CHILD-TOKEN"]);

      // Both spent the SAME grant-scoped bucket — two tokens gone from one key.
      expect(await spent(backend, 600_000, 2, grantBucket)).toBe(2);
      // And neither touched the client-level bucket, which isolation replaces.
      expect(await backend.hgetall(P.bucket)).toEqual({});
      // The child has no bucket at either scope.
      expect(await backend.hgetall(C.bucket)).toEqual({});
      expect(
        await backend.hgetall(`${NS}:${CHILD}:grant:g1:rateLimit`)
      ).toEqual({});

      // Two tokens for this grant, one spent by each side: the third request
      // cannot be admitted. A child resolving the un-isolated key would have
      // found a full bucket here and sailed through.
      await expect(
        fireAt(handler, CHILD, { url: "/third", grantId: "g1" })
      ).rejects.toThrow(RequestTimeoutError);
      expect(up.urls()).not.toContain("/third");
    });
  }, 30_000);

  it("keeps one grant's shared budget separate from another's", async () => {
    // The other half of isolation: sharing must not leak across grants. If the
    // child collapsed onto a single key, exhausting g1 would block g2.
    await isolatedPair(1, async (handler, backend) => {
      await seedGrant(handler, "g1");
      await seedGrant(handler, "g2");

      expect(
        (await fireAt(handler, CHILD, { url: "/c1", grantId: "g1" })).status
      ).toBe(200);
      // g1 is spent, but g2 is a different budget and the child must still go.
      expect(
        (await fireAt(handler, CHILD, { url: "/c2", grantId: "g2" })).status
      ).toBe(200);

      expect(
        await spent(backend, 600_000, 1, `${NS}:${PARENT}:grant:g1:rateLimit`)
      ).toBe(1);
      expect(
        await spent(backend, 600_000, 1, `${NS}:${PARENT}:grant:g2:rateLimit`)
      ).toBe(1);

      // ...and g1 really is exhausted, from the parent's side as well.
      await expect(
        fireAt(handler, PARENT, { url: "/p1", grantId: "g1" })
      ).rejects.toThrow(RequestTimeoutError);
      expect(up.urls()).not.toContain("/p1");
    });
  }, 30_000);

  it("freezes an isolated grant's shared budget without touching its siblings", async () => {
    await isolatedPair(
      10,
      async (handler, backend) => {
        await seedGrant(handler, "g1");
        await seedGrant(handler, "g2");
        up.setRoute((req, res) => {
          if ((req.url ?? "").startsWith("/child-g1")) {
            res.writeHead(429);
            res.end("{}");
            return;
          }
          ok(req, res, 0);
        });

        await expect(
          fireAt(handler, CHILD, { url: "/child-g1", grantId: "g1" })
        ).rejects.toThrow();

        // The freeze lands on the parent's g1 key — the grant the child was
        // rate limited on — and nowhere else.
        const frozen = await waitForFreeze(
          backend,
          `${NS}:${PARENT}:grant:g1:freezeState`
        );
        expect(frozen.frozenUntil).toBeGreaterThan(Date.now());
        expect(
          await backend.getFreezeState(`${NS}:${PARENT}:grant:g2:freezeState`)
        ).toBeNull();
        expect(await backend.getFreezeState(P.freeze)).toBeNull();
        expect(
          await backend.getFreezeState(`${NS}:${CHILD}:grant:g1:freezeState`)
        ).toBeNull();

        // g2 is a different budget with its own freeze state, so the child's
        // traffic on it must go straight out.
        const started = Date.now();
        expect(
          (await fireAt(handler, CHILD, { url: "/child-g2", grantId: "g2" }))
            .status
        ).toBe(200);
        expect(Date.now() - started).toBeLessThan(400);
      },
      { maxRetries: 0, retryBackoffBaseTime: 900 }
    );
  }, 30_000);

  // ======================================================== misconfiguration

  it("refuses a child whose parent is not registered", async () => {
    const { backend, cleanup } = await harness.create();
    const handler = new RequestHandler({
      key: KEY,
      backend,
      keyPrefix: PREFIX,
    });
    handler.setDrainTimeout(100);
    await handler.registerClientTemplate(
      "orphan" as never,
      ((creds: { instanceId: string }) => [
        {
          name: `orphan:_:${creds.instanceId}`,
          rateLimit: { type: "sharedLimit", clientName: "nobody:_:a" },
        },
      ]) as never
    );
    await handler.start();
    try {
      await expect(
        handler.addTemplateClient(
          "orphan" as never,
          {
            instanceId: "a",
          } as never
        )
      ).rejects.toBeInstanceOf(ConfigurationError);
      expect(handler.getLoadedClients().map((c) => c.name)).not.toContain(
        "orphan:_:a"
      );
    } finally {
      await handler.stop();
      await cleanup();
    }
  }, 30_000);

  it("refuses a child whose parent is itself a sharedLimit client", async () => {
    const { backend, cleanup } = await harness.create();
    const handler = new RequestHandler({
      key: KEY,
      backend,
      keyPrefix: PREFIX,
    });
    handler.setDrainTimeout(100);
    await handler.registerClientTemplate(
      "chain" as never,
      ((creds: { instanceId: string }) => [
        {
          name: `root:_:${creds.instanceId}`,
          rateLimit: {
            type: "requestLimit",
            interval: 1_000,
            tokensToAdd: 5,
            maxTokens: 5,
          },
        },
        {
          name: `mid:_:${creds.instanceId}`,
          rateLimit: { type: "sharedLimit", clientName: "root:_:a" },
        },
        {
          name: `leaf:_:${creds.instanceId}`,
          rateLimit: { type: "sharedLimit", clientName: "mid:_:a" },
        },
      ]) as never
    );
    await handler.start();
    try {
      await expect(
        handler.addTemplateClient(
          "chain" as never,
          {
            instanceId: "a",
          } as never
        )
      ).rejects.toBeInstanceOf(ConfigurationError);
      // The chain is refused at the leaf, so nothing points at a shared budget
      // of a shared budget.
      expect(handler.getLoadedClients().map((c) => c.name)).not.toContain(
        "leaf:_:a"
      );
    } finally {
      await handler.stop();
      await cleanup();
    }
  }, 30_000);

  it("refuses the request outright once the parent has been removed", async () => {
    // Registration-time validation cannot cover a parent that disappears later
    // — a separate template, a credential withdrawal — and the child would then
    // keep accepting work into a namespace no client is registered for, so
    // nobody drains it. What must NOT happen is the slow failure: sitting out a
    // 60s admission timeout and then reporting a timeout that never mentions
    // the parent. `assertReadyToQueue` has to reject before the request takes a
    // place in the queue at all.
    await withPair(
      harness,
      up.baseURL,
      [
        {
          name: "parent",
          rateLimit: {
            type: "requestLimit",
            interval: 1_000,
            tokensToAdd: 10,
            maxTokens: 10,
          },
        },
        {
          name: "child",
          rateLimit: { type: "sharedLimit", clientName: PARENT },
          cleanupTimeout: 800,
        },
      ],
      async (handler, backend) => {
        expect((await fireAt(handler, CHILD, { url: "/before" })).status).toBe(
          200
        );

        await handler.removeTemplateClient("parent" as never, "a");
        expect(handler.getLoadedClients().map((c) => c.name)).not.toContain(
          PARENT
        );

        const started = Date.now();
        const error = await fireAt(handler, CHILD, {
          url: "/orphaned",
        }).catch((e: unknown) => e);

        expect(error).toBeInstanceOf(ClientUnavailableError);
        const unavailable = error as ClientUnavailableError;
        expect(unavailable.code).toBe("shared_limit_parent_missing");
        expect(unavailable.statusCode).toBe(503);
        // The message has to name the missing parent, or the operator is left
        // reading a 503 with no idea which registration went away.
        expect(unavailable.message).toContain(PARENT);
        expect(unavailable.message).toContain(CHILD);

        // Immediately, not after the admission timeout — the child's is 800ms
        // and even that would be too slow to be useful.
        expect(Date.now() - started).toBeLessThan(200);
        expect(up.urls()).not.toContain("/orphaned");
        // And nothing was left behind in a queue nobody owns.
        const queue = await backend.getQueueStats(P.queue, P.meta);
        expect(queue.pending + queue.inProgress).toBe(0);
      }
    );
  }, 30_000);

  // ============================================================== shutdown

  it("fails queued work from both parent and child on shutdown", async () => {
    const { backend, cleanup } = await harness.create();
    const handler = new RequestHandler({
      key: KEY,
      backend,
      keyPrefix: PREFIX,
    });
    handler.setDrainTimeout(150);
    for (const [name, rateLimit] of [
      [
        "parent",
        {
          type: "requestLimit",
          interval: 600_000,
          tokensToAdd: 1,
          maxTokens: 1,
        },
      ],
      ["child", { type: "sharedLimit", clientName: PARENT }],
    ] as const) {
      await handler.registerClientTemplate(
        name as never,
        ((creds: { instanceId: string }) => [
          {
            name: `${name}:_:${creds.instanceId}`,
            rateLimit,
            requestOptions: { defaults: { baseURL: up.baseURL } },
          },
        ]) as never
      );
    }
    await handler.start();
    await handler.addTemplateClient(
      "parent" as never,
      {
        instanceId: "a",
      } as never
    );
    await handler.addTemplateClient(
      "child" as never,
      {
        instanceId: "a",
      } as never
    );

    try {
      // Spend the single token, then park one request from each side.
      await fireAt(handler, PARENT, { url: "/warm" });
      const parked = [
        fireAt(handler, PARENT, { url: "/p" }).catch((e: Error) => e),
        fireAt(handler, CHILD, { url: "/c" }).catch((e: Error) => e),
      ];
      await settle(150);

      await handler.stop();
      const outcomes = await Promise.all(parked);
      // Both sides get a reason, rather than hanging until their admission
      // timeout long after the process meant to exit.
      for (const outcome of outcomes) {
        expect(outcome).toBeInstanceOf(Error);
        expect((outcome as Error).message).toMatch(
          /handler stopped|Request handler stopped/i
        );
      }
    } finally {
      await handler.stop().catch(() => {});
      await cleanup();
    }
  }, 30_000);
});
