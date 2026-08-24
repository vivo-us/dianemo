import { encrypt, decrypt } from "../packages/core/src/utils/encryption.js";
import type { DianemoBackend } from "../packages/core/src/backend/types.js";
import Request from "../packages/core/src/request/index.js";
import RequestHandler from "../packages/core/src/index.js";
import { describe, expect, it } from "vitest";
import { createServer } from "node:http";
import {
  redactBody,
  redactUrl,
  sanitizeError,
} from "../packages/core/src/utils/redact.js";
import {
  fire,
  fireMany,
  harnesses,
  startUpstream,
  withHandler,
} from "./clientTypes/harness.js";

/**
 * Regressions for the second, blind audit.
 *
 * Grouped by the defect rather than by the file, because several of these were
 * one root cause surfacing in three places — and because the point of each test
 * is the failure it replaced, not the function it calls.
 */

// --------------------------------------------------------------- redaction

describe("form-encoded redaction", () => {
  /**
   * The string branch matched a fixed list of parameter names while the object
   * branch used the full sensitive-name test, so `clientSecret` was redacted in
   * a JSON body and printed in a form body — which is the shape an OAuth token
   * request actually uses.
   */
  it("redacts sensitive parameters the fixed list missed", () => {
    const cases = [
      "grant_type=refresh_token&clientSecret=SECRET_VALUE",
      "grant_type=refresh_token&secret=SECRET_VALUE",
      "grant_type=refresh_token&consumer_secret=SECRET_VALUE",
      "grant_type=refresh_token&refreshToken=SECRET_VALUE",
      "grant_type=refresh_token&subscription-key=SECRET_VALUE",
    ];
    for (const body of cases) {
      expect(redactBody(body)).not.toContain("SECRET_VALUE");
    }
  });

  it("leaves the rest of the body readable", () => {
    const out = redactBody(
      "grant_type=client_credentials&client_id=public-id&client_secret=SECRET_VALUE"
    ) as string;
    expect(out).toContain("grant_type=client_credentials");
    expect(out).toContain("client_id=public-id");
    expect(out).not.toContain("SECRET_VALUE");
  });

  it("does not mangle a body with no credentials in it", () => {
    const body = "grant_type=client_credentials&scope=read%20write";
    expect(redactBody(body)).toBe(body);
  });

  it("clears both references to the raw request object", () => {
    const request = {
      _header: "GET / HTTP/1.1\r\nAuthorization: Bearer T\r\n",
    };
    const error = { config: { url: "/x" }, request, response: { request } };
    sanitizeError(error);
    expect(error.request).toBeUndefined();
    expect(error.response.request).toBeUndefined();
  });
});

// -------------------------------------------------------------- encryption

describe("derived key caching", () => {
  /**
   * `encrypt` generated a fresh salt per call, making every write a guaranteed
   * cache miss — so each one paid a full scrypt derivation, ~18ms of blocked
   * event loop, which stalls the token bucket and the queue drain with it.
   */
  it("does not pay a fresh derivation per encrypt", () => {
    const secret = "0123456789abcdef0123456789abcdef";
    encrypt("warm the cache", secret);

    const started = performance.now();
    for (let i = 0; i < 25; i++) encrypt(`value-${i}`, secret);
    const elapsed = performance.now() - started;

    // 25 real scrypt derivations would be well over a second.
    expect(elapsed).toBeLessThan(200);
  });

  it("still round-trips, and still fails closed on the wrong key", () => {
    const secret = "0123456789abcdef0123456789abcdef";
    const other = "fedcba9876543210fedcba9876543210";
    const ciphertext = encrypt("hello", secret);
    expect(decrypt(ciphertext, secret)).toBe("hello");
    expect(() => decrypt(ciphertext, other)).toThrow();
  });

  it("produces different ciphertext for the same input", () => {
    const secret = "0123456789abcdef0123456789abcdef";
    // The IV is still per-operation even though the salt is now per-secret.
    expect(encrypt("same", secret)).not.toBe(encrypt("same", secret));
  });
});

// -------------------------------------------------- per-request validation

describe("per-request validation", () => {
  const build = (config: Record<string, unknown>) => () =>
    new Request(
      "c",
      { clientName: "c", requestName: "r", ...config } as never,
      "i"
    );

  /**
   * The ceiling guard is a `>` test, so a negative cost sailed through it. In the
   * bucket `tokens - cost` then ADDS tokens, and in the concurrency accounting
   * `used + cost <= max` stops being a cap at all.
   */
  it("refuses a cost that is not a positive number", () => {
    expect(build({ cost: -1 })).toThrow(/cost/);
    expect(build({ cost: 0 })).toThrow(/cost/);
    expect(build({ cost: Number.NaN })).toThrow(/cost/);
    expect(build({ cost: Number.POSITIVE_INFINITY })).toThrow(/cost/);
  });

  it("keeps a fractional cost, which is a supported spend", () => {
    expect(build({ cost: 1.9 })().getMetadata().cost).toBe(1.9);
  });

  /**
   * Positive, finite, and still free: a value below `Number.EPSILON` cannot be
   * subtracted from a balance of 1 or more, so `tokens - cost` returns `tokens`
   * and the request buys itself unlimited budget.
   */
  it("refuses a positive cost too small to actually spend anything", () => {
    expect(build({ cost: Number.MIN_VALUE })).toThrow(/cost/);
    expect(build({ cost: 5e-324 })).toThrow(/cost/);
    // The floor itself is spendable, so it must still be accepted.
    expect(build({ cost: Number.EPSILON })().getMetadata().cost).toBe(
      Number.EPSILON
    );
    expect(1 - Number.MIN_VALUE).toBe(1);
  });

  /** `priority || 1` promoted 0 — the documented bottom of the range — to 1. */
  it("preserves priority 0 instead of promoting it", () => {
    expect(build({ priority: 0 })().getMetadata().priority).toBe(0);
  });

  it("refuses a priority outside the documented range", () => {
    expect(build({ priority: -1 })).toThrow(/priority/);
    expect(build({ priority: 11 })).toThrow(/priority/);
    expect(build({ priority: 1.5 })).toThrow(/priority/);
  });

  /**
   * An empty string is falsy everywhere `grantId` is tested, so it silently fell
   * back to the client's own credentials — the request went out under the wrong
   * identity with no error at all.
   */
  it("refuses an empty grantId rather than falling back to client credentials", () => {
    expect(build({ grantId: "" })).toThrow(/grantId/);
    expect(build({ grantId: "   " })).toThrow(/grantId/);
    expect(build({ grantId: "tenant-a" })().getMetadata().grantId).toBe(
      "tenant-a"
    );
  });
});

// ------------------------------------------------------ end-to-end per type

/** An upstream that always rate-limits, for exercising the freeze path. */
async function start429Upstream() {
  const server = createServer((_req, res) => {
    res.writeHead(429, { "Content-Type": "application/json" });
    res.end('{"error":"slow down"}');
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  return {
    baseURL: `http://127.0.0.1:${port}`,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

/**
 * `withHandler` fixes `retryOptions` and `requestOptions`, and two cases here
 * turn on exactly those — the retry budget, and how long a request waits for
 * admission before giving up.
 */
async function withCustomHandler(
  harness: {
    create: () => Promise<{
      backend: DianemoBackend;
      cleanup: () => Promise<void>;
    }>;
  },
  options: {
    templateName: string;
    keyPrefix: string;
    baseURL: string;
    rateLimit: Record<string, unknown>;
    retryOptions?: Record<string, unknown>;
    requestOptions?: Record<string, unknown>;
  },
  fn: (handler: RequestHandler, backend: DianemoBackend) => Promise<void>
) {
  const { backend, cleanup } = await harness.create();
  const handler = new RequestHandler({
    key: "0123456789abcdef0123456789abcdef",
    backend,
    keyPrefix: options.keyPrefix,
  });
  await handler.registerClientTemplate(
    options.templateName as never,
    ((creds: { instanceId: string }) => [
      {
        name: `${options.templateName}:_:${creds.instanceId}`,
        rateLimit: options.rateLimit,
        ...(options.retryOptions ? { retryOptions: options.retryOptions } : {}),
        requestOptions: {
          ...options.requestOptions,
          defaults: { baseURL: options.baseURL },
        },
      },
    ]) as never
  );
  await handler.start();
  await handler.addTemplateClient(
    options.templateName as never,
    {
      instanceId: "a",
    } as never
  );
  try {
    await fn(handler, backend);
  } finally {
    await handler.stop();
    await cleanup();
  }
}

for (const harness of harnesses(9)) {
  describe(`audit round 2 — '${harness.name}'`, () => {
    /**
     * `waitForTurn` used to sleep for the refill inside `processingLock`. The
     * costly head was selected, then held the lock until its tokens arrived and
     * spent them all — so a higher-priority arrival could not be considered
     * until the window after the one it should have run in.
     */
    it("admits a higher-priority arrival ahead of a costly queued head", async () => {
      const upstream = await startUpstream(0);
      try {
        await withHandler(
          harness,
          upstream.baseURL,
          [
            {
              name: "hol",
              rateLimit: {
                type: "requestLimit",
                maxTokens: 10,
                tokensToAdd: 10,
                interval: 800,
              },
            },
          ],
          async (handler) => {
            const client = "hol:_:a";
            // Drain the bucket so everything below has to queue.
            await fireMany(handler, client, 10);

            const order: string[] = [];
            const heavy = handler
              .handleRequest({
                clientName: client,
                requestName: "t.heavy",
                method: "GET",
                url: "/",
                cost: 10,
                priority: 1,
              })
              .then(() => order.push("heavy"));

            await new Promise((r) => setTimeout(r, 60));

            const urgent = handler
              .handleRequest({
                clientName: client,
                requestName: "t.urgent",
                method: "GET",
                url: "/",
                cost: 1,
                priority: 10,
              })
              .then(() => order.push("urgent"));

            await Promise.all([heavy, urgent]);
            expect(order[0]).toBe("urgent");
          },
          "r2hol"
        );
      } finally {
        await upstream.close();
      }
    });

    /**
     * A budget cut below the cost of an already-queued request made
     * `acquireTokens` permanently unsatisfiable while it held the drain lock, so
     * the client never admitted queued work again. The fast path kept working,
     * which is what hid it.
     */
    it("keeps draining after a budget cut strands a queued request", async () => {
      const upstream = await startUpstream(0);
      try {
        await withCustomHandler(
          harness,
          {
            templateName: "wedge",
            keyPrefix: "r2wedge",
            baseURL: upstream.baseURL,
            rateLimit: {
              type: "requestLimit",
              maxTokens: 10,
              tokensToAdd: 10,
              interval: 400,
            },
            // The stranded request can never be admitted, so it has to be able
            // to give up inside the test rather than after the 60s default.
            requestOptions: { cleanupTimeout: 1500 },
          },
          async (handler, backend) => {
            const client = "wedge:_:a";
            await fireMany(handler, client, 10);

            const stranded = handler
              .handleRequest({
                clientName: client,
                requestName: "t.big",
                method: "GET",
                url: "/",
                cost: 9,
              })
              .then(() => "sent")
              .catch(() => "rejected");

            await new Promise((r) => setTimeout(r, 50));
            // The public trigger for a live change is the `rateLimitUpdated`
            // broadcast, which is what `rateLimitChange` publishes.
            await backend.publish(
              "r2wedge:requestHandler:rateLimitUpdated",
              JSON.stringify({
                clientName: client,
                rateLimit: {
                  type: "requestLimit",
                  maxTokens: 3,
                  tokensToAdd: 3,
                  interval: 400,
                },
                source: "operator",
                publisherInstanceId: "someone-else",
              })
            );
            await new Promise((r) => setTimeout(r, 60));

            // Work behind the stranded request still has to complete — before
            // the fix the drain loop spun on it forever and admitted nothing.
            const results = await Promise.all([
              fire(handler, client),
              fire(handler, client),
              fire(handler, client),
            ]);
            expect(results).toHaveLength(3);

            expect(await stranded).toBe("rejected");
          }
        );
      } finally {
        await upstream.close();
      }
    });

    it("makes a noLimit request queue when anything is already waiting", async () => {
      const upstream = await startUpstream(300);
      try {
        await withHandler(
          harness,
          upstream.baseURL,
          [{ name: "nl", rateLimit: { type: "noLimit" } }],
          async (handler, backend) => {
            const client = "nl:_:a";
            const queueKey = `r2nl:requestHandler:${client}:queue`;
            const prefix = `r2nl:requestHandler:${client}:request`;

            // `inProgress`, so it occupies the queue without being selectable —
            // it stands in for a backlog still draining, which is the state a
            // noLimit client reaches after a freeze.
            await backend.addRequest(queueKey, prefix, {
              requestId: "placeholder",
              clientName: client,
              requestName: "t.placeholder",
              status: "inProgress",
              priority: 1,
              cost: 1,
              retries: 0,
              timestamp: Date.now(),
              isThawRequest: false,
              ownerId: "someone-else",
            } as never);
            expect(await backend.getQueueLength(queueKey)).toBe(1);

            const inFlight = fire(handler, client);
            await new Promise((r) => setTimeout(r, 120));
            // Taking the fast path would have left the length at 1.
            expect(await backend.getQueueLength(queueKey)).toBe(2);
            await inFlight;
            // The placeholder has no owner to complete it, and the shutdown
            // drain waits on queue depth.
            await backend.removeRequest(queueKey, prefix, "placeholder");
          },
          "r2nl"
        );
      } finally {
        await upstream.close();
      }
    });

    /**
     * The retry classifier checked the retry budget before classifying the
     * response, so a 429 arriving on the last permitted attempt — which under
     * `maxRetries: 0` is every 429 — never armed the fleet-wide freeze.
     */
    it("freezes on a 429 that lands on the final permitted attempt", async () => {
      const upstream = await start429Upstream();
      try {
        await withCustomHandler(
          harness,
          {
            templateName: "fz",
            keyPrefix: "r2fz",
            baseURL: upstream.baseURL,
            rateLimit: { type: "noLimit" },
            retryOptions: { maxRetries: 0 },
          },
          async (handler, backend) => {
            await fire(handler, "fz:_:a").catch(() => undefined);
            // Published to the controller and applied asynchronously.
            await new Promise((r) => setTimeout(r, 150));
            const state = await backend.getFreezeState(
              "r2fz:requestHandler:fz:_:a:freezeState"
            );
            expect(state).not.toBeNull();
          }
        );
      } finally {
        await upstream.close();
      }
    });
  });
}

// ------------------------------------------------- JSON bodies and userinfo

describe("redaction gaps found by verification", () => {
  /**
   * Axios replaces an object `config.data` with its serialized form before
   * dispatch, and the error carries that same config — so a JSON token request
   * reached redaction as a string with no `=` in it, which neither the
   * value-shape test nor the form-encoded parser could act on. It printed in
   * full in the thrown error, the caller's `retryHandler`, and the library log.
   */
  it("redacts a JSON body that axios has already serialized", () => {
    const body = JSON.stringify({
      grant_type: "client_credentials",
      client_id: "public-id",
      client_secret: "JSON_CLIENT_SECRET_VALUE",
    });
    const out = redactBody(body) as string;
    expect(out).not.toContain("JSON_CLIENT_SECRET_VALUE");
    // Still readable as the request that was sent.
    expect(out).toContain("client_credentials");
    expect(out).toContain("public-id");
  });

  it("redacts a credential nested inside a serialized JSON body", () => {
    const body = JSON.stringify({
      outer: { inner: [{ refreshToken: "NESTED_RT_VALUE" }] },
    });
    expect(redactBody(body) as string).not.toContain("NESTED_RT_VALUE");
  });

  it("leaves a non-JSON string to the form-encoded parser", () => {
    expect(redactBody("grant_type=x&client_secret=S")).not.toContain("=S");
    expect(redactBody("not json, no equals")).toBe("not json, no equals");
    // Looks like JSON but is not — must not be swallowed.
    expect(redactBody("{oops")).toBe("{oops");
  });

  /**
   * A PAT used AS the username carries the whole credential and has no password
   * half to strip, so the userinfo is replaced wholesale.
   */
  it("redacts URL userinfo that has no password component", () => {
    expect(
      redactUrl("https://ghp_ABCDEFGHIJKLMNOPQRSTUVWX@host/v1/x")
    ).not.toContain("ghp_ABCDEFGHIJKLMNOPQRSTUVWX");
    expect(redactUrl("//token_value_here@host/v1/x")).not.toContain(
      "token_value_here"
    );
    // The password form still works, and the username is still shown.
    expect(redactUrl("https://user:pw@host/x")).toBe(
      "https://user:[REDACTED]@host/x"
    );
  });

  it("redacts a credential echoed back in statusText", () => {
    const err = {
      config: { url: "/x" },
      response: {
        statusText: "Token STATUSTEXT_TOKEN_VALUE rejected",
        headers: {},
      },
    };
    sanitizeError(err);
    expect(JSON.stringify(err)).not.toContain("STATUSTEXT_TOKEN_VALUE");
  });

  it("treats `credentials` and `passphrase` as sensitive names", () => {
    const out = redactBody({
      auth_block: { credentials: "PLAIN_NESTED_VALUE" },
      passphrase: "PASSPHRASE_VALUE",
    }) as Record<string, unknown>;
    expect(JSON.stringify(out)).not.toContain("PLAIN_NESTED_VALUE");
    expect(JSON.stringify(out)).not.toContain("PASSPHRASE_VALUE");
  });
});

// ------------------------------------------------------------ retryHandler

/**
 * `retryHandler` is consumer code, and it is handed the error BEFORE the request
 * is finished with — so it is the earliest place a credential can escape. It had
 * no test anywhere in the repo, which means the ordering it depends on was
 * unguarded: moving `sanitizeError` back to the throw site left every existing
 * test passing while the raw error, with the live `ClientRequest` and its
 * `_header` bytes, reached the callback.
 */
for (const harness of harnesses(9)) {
  describe(`retryHandler — '${harness.name}'`, () => {
    it("receives an error that has already been sanitized", async () => {
      const upstream = await start401Upstream();
      try {
        const seen: Array<{
          hasRequest: boolean;
          hasResponseRequest: boolean;
        }> = [];
        await withCustomHandler(
          harness,
          {
            templateName: "rh",
            keyPrefix: "r2rh",
            baseURL: upstream.baseURL,
            rateLimit: { type: "noLimit" },
            retryOptions: {
              maxRetries: 1,
              retryStatusCodes: [],
              // A 401 matches no status branch, so the chain reaches here.
              retryHandler: (error: unknown) => {
                const e = error as {
                  request?: unknown;
                  response?: { request?: unknown };
                };
                seen.push({
                  hasRequest: e.request !== undefined,
                  hasResponseRequest: e.response?.request !== undefined,
                });
                return false;
              },
            },
          },
          async (handler) => {
            await fire(handler, "rh:_:a").catch(() => undefined);
          }
        );

        expect(seen.length).toBeGreaterThan(0);
        for (const snapshot of seen) {
          // Both references to the live ClientRequest are already gone, which
          // only `sanitizeError` does — so it ran before the callback.
          expect(snapshot.hasRequest).toBe(false);
          expect(snapshot.hasResponseRequest).toBe(false);
        }
      } finally {
        await upstream.close();
      }
    }, 20_000);
  });
}

/** An upstream that always rejects with 401, which matches no retry branch. */
async function start401Upstream() {
  const server = createServer((_req, res) => {
    res.writeHead(401, { "Content-Type": "application/json" });
    res.end('{"error":"unauthorized"}');
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  return {
    baseURL: `http://127.0.0.1:${port}`,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

// -------------------------------------------- refresh-token expiry handling

/**
 * These pin `saveOAuthData`'s CHOICE of TTL arguments, which no existing test
 * observes — the credential-TTL tests call `credentialTtlSeconds` directly with
 * hand-written arguments, so a regression in how those arguments are derived is
 * invisible to the whole suite.
 */
for (const harness of harnesses(10)) {
  describe(`refresh token expiry — '${harness.name}'`, () => {
    const KEY = "0123456789abcdef0123456789abcdef";
    const NINETY_DAYS = 60 * 60 * 24 * 90;

    /** Drives the real refresh path against a token endpoint we count. */
    async function rotate(
      backend: DianemoBackend,
      seed: Record<string, string | number>,
      response: Record<string, unknown>
    ) {
      const calls: string[] = [];
      const server = createServer((req, res) => {
        let body = "";
        req.on("data", (c) => (body += c));
        req.on("end", () => {
          calls.push(body);
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify(response));
        });
      });
      await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
      const port = (server.address() as { port: number }).port;

      const handler = new RequestHandler({
        key: KEY,
        backend,
        keyPrefix: "rte",
      });
      await handler.registerClientTemplate(
        "rt" as never,
        ((creds: { instanceId: string }) => [
          {
            name: `rt:_:${creds.instanceId}`,
            rateLimit: { type: "noLimit" },
            requestOptions: {
              defaults: { baseURL: `http://127.0.0.1:${port}` },
            },
            authentication: {
              type: "oauth2",
              clientId: "cid",
              clientSecret: "csecret",
              grantType: "refresh_token",
              refreshConfig: {
                url: `http://127.0.0.1:${port}/token`,
                method: "POST",
                dataLocation: "urlEncodedForm",
                data: {
                  grant_type: "refresh_token",
                  refresh_token: "{{refreshToken}}",
                },
              },
            },
          },
        ]) as never
      );
      await handler.start();
      await handler.addTemplateClient(
        "rt" as never,
        {
          instanceId: "a",
        } as never
      );

      const credKey = "rte:requestHandler:rt:_:a:oauth2";
      await backend.hset(credKey, seed, NINETY_DAYS);
      // An access token already past its renewal margin forces the refresh.
      await handler
        .handleRequest({
          clientName: "rt:_:a",
          requestName: "t.go",
          method: "GET",
          url: "/",
        })
        .catch(() => undefined);

      const stored = await backend.hgetall(credKey);
      await handler.stop();
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
      return { stored, calls };
    }

    /**
     * A rotation with no stated expiry must record the expiry as UNKNOWN, so the
     * long default applies. Inheriting the predecessor's made the TTL shrink on
     * every rotation until a still-valid refresh token was deleted.
     */
    it("does not carry a rotated refresh token's expiry over from its predecessor", async () => {
      const { backend, cleanup } = await harness.create();
      try {
        const { stored } = await rotate(
          backend,
          {
            accessToken: encrypt("old-at", KEY),
            expiresAt: Date.now() - 1000,
            issuedAt: Date.now() - 3_600_000,
            tokenType: "Bearer",
            refreshToken: encrypt("rt-gen0", KEY),
            // 120s from now: the value the old code would have inherited.
            refreshTokenExpiresAt: Date.now() + 120_000,
          },
          {
            access_token: "at-1",
            token_type: "Bearer",
            refresh_token: "rt-gen1",
          }
        );
        expect(stored.refreshTokenExpiresAt).toBe("");
      } finally {
        await cleanup();
      }
    }, 20_000);

    /**
     * `0` is a real provider convention for "does not expire". Three separate
     * readings of the field disagreed about it, so the expiry was recorded as
     * unknown while the TTL was sized from the access token alone — deleting the
     * grant an hour later with a live refresh token in it.
     */
    it("treats a zero or negative stated refresh expiry as unstated", async () => {
      const { backend, cleanup } = await harness.create();
      try {
        for (const stated of [0, -100]) {
          const { stored } = await rotate(
            backend,
            {
              accessToken: encrypt("old-at", KEY),
              expiresAt: Date.now() - 1000,
              issuedAt: Date.now() - 3_600_000,
              tokenType: "Bearer",
              refreshToken: encrypt("rt-gen0", KEY),
            },
            {
              access_token: "at-1",
              token_type: "Bearer",
              refresh_token: "rt-gen1",
              refresh_token_expires_in: stated,
            }
          );
          // Unknown, not a past timestamp — so the long default governs the TTL.
          expect(stored.refreshTokenExpiresAt).toBe("");
        }
      } finally {
        await cleanup();
      }
    }, 25_000);

    /** A JSON-string expiry is what several providers actually send. */
    it("honours a stated refresh expiry given as a string", async () => {
      const { backend, cleanup } = await harness.create();
      try {
        const before = Date.now();
        const { stored } = await rotate(
          backend,
          {
            accessToken: encrypt("old-at", KEY),
            expiresAt: Date.now() - 1000,
            issuedAt: Date.now() - 3_600_000,
            tokenType: "Bearer",
            refreshToken: encrypt("rt-gen0", KEY),
          },
          {
            access_token: "at-1",
            token_type: "Bearer",
            refresh_token: "rt-gen1",
            refresh_token_expires_in: "604800",
          }
        );
        const recorded = Number(stored.refreshTokenExpiresAt);
        expect(recorded).toBeGreaterThan(before + 604_800_000 - 60_000);
      } finally {
        await cleanup();
      }
    }, 20_000);
  });
}

// ------------------------------------- a ceiling that dips and then recovers

/**
 * Failing an impossible queued cost must not turn a TRANSIENT dip into a
 * permanent rejection.
 *
 * Reaching the failure costs two awaited backend calls, which is an ordinary
 * window for a `rateLimitChange` to arrive — and a ceiling that dropped and
 * recovered inside it produced a hard 400 quoting a ceiling the request would
 * fit ("cost (9) exceeds maximum tokens (20)"), where the previous behaviour was
 * to serve it on the next pass.
 *
 * This reaches into the client instance to land the recovery inside that window,
 * because the window cannot be hit reliably from outside. That couples the test
 * to an internal method name, which is a real cost — accepted because the
 * alternative is no guard at all for a defect introduced by a fix.
 */
for (const harness of harnesses(11)) {
  describe(`ceiling dip and recover — '${harness.name}'`, () => {
    it("serves a request whose ceiling recovers before it is failed", async () => {
      const upstream = await startUpstream(0);
      const INTERVAL = 300;
      try {
        await withCustomHandler(
          harness,
          {
            templateName: "dip",
            keyPrefix: "r2dip",
            baseURL: upstream.baseURL,
            rateLimit: {
              type: "requestLimit",
              maxTokens: 10,
              tokensToAdd: 10,
              interval: INTERVAL,
            },
            requestOptions: { cleanupTimeout: 5000 },
          },
          async (handler, backend) => {
            const clientName = "dip:_:a";
            await fireMany(handler, clientName, 10);

            const client = (
              handler as unknown as {
                clients: Map<
                  string,
                  {
                    rateLimit: { maxTokens: number };
                    updateRequestInQueue: (
                      id: string,
                      u: Record<string, unknown>
                    ) => Promise<unknown>;
                  }
                >;
              }
            ).clients.get(clientName)!;

            const stranded = handler
              .handleRequest({
                clientName,
                requestName: "t.big",
                method: "GET",
                url: "/",
                cost: 9,
              })
              .then(() => "served")
              .catch((e: Error) => e.constructor.name);

            await new Promise((r) => setTimeout(r, 60));

            // Raise the ceiling back the moment the loop starts unwinding the
            // refusal — i.e. inside the window between the throw and the reason.
            const original = client.updateRequestInQueue.bind(client);
            let raised = false;
            client.updateRequestInQueue = async (id, updates) => {
              if (!raised && client.rateLimit.maxTokens === 3) {
                raised = true;
                client.rateLimit = {
                  type: "requestLimit",
                  maxTokens: 20,
                  tokensToAdd: 20,
                  interval: INTERVAL,
                } as never;
              }
              return original(id, updates);
            };

            await backend.publish(
              "r2dip:requestHandler:rateLimitUpdated",
              JSON.stringify({
                clientName,
                rateLimit: {
                  type: "requestLimit",
                  maxTokens: 3,
                  tokensToAdd: 3,
                  interval: INTERVAL,
                },
                source: "dynamic",
                publisherInstanceId: "someone-else",
              })
            );

            // The dip must have actually been entered, or this proves nothing.
            expect(await stranded).toBe("served");
            expect(raised).toBe(true);
          }
        );
      } finally {
        await upstream.close();
      }
    }, 25_000);
  });
}
