import { credentialTtlSeconds } from "../packages/core/src/utils/credentialTtl.js";
import { harnesses, withHandler, fire } from "./clientTypes/harness.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import RequestHandler from "../packages/core/src/index.js";
import type { Harness } from "./clientTypes/harness.js";
import { createServer } from "node:http";

/**
 * Regressions for bugs that were silent in production and invisible in tests.
 *
 * Each case here reproduces a specific failure the suite used to allow. They
 * are grouped by the promise they defend rather than by the module they touch,
 * because every one of them was a case of two components each behaving
 * reasonably on its own.
 */

const KEY = "0123456789abcdef0123456789abcdef";

/** Polls `check` until it holds or the deadline passes. */
async function waitFor(
  check: () => Promise<boolean>,
  timeoutMs = 5_000,
  pollMs = 25
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await check()) return true;
    if (Date.now() >= deadline) return false;
    await new Promise((r) => setTimeout(r, pollMs));
  }
}

/** An upstream whose status code the test drives, request by request. */
async function startScriptedUpstream() {
  const seen: Array<{ url: string; auth: Record<string, string> }> = [];
  let statuses: number[] = [];

  const server = createServer((req, res) => {
    seen.push({
      url: req.url ?? "",
      auth: {
        authorization: String(req.headers.authorization ?? ""),
        "x-vendor-token": String(req.headers["x-vendor-token"] ?? ""),
      },
    });
    const status = statuses.length ? statuses.shift()! : 200;
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end(status === 200 ? '{"ok":true}' : '{"error":"slow down"}');
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;

  return {
    baseURL: `http://127.0.0.1:${port}`,
    script: (...codes: number[]) => {
      statuses = codes;
    },
    seen: () => seen,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

describe.each(harnesses(8))("regressions — $name", (harness) => {
  let upstream: Awaited<ReturnType<typeof startScriptedUpstream>>;

  beforeAll(async () => {
    upstream = await startScriptedUpstream();
  });
  afterAll(async () => {
    await upstream.close();
  });

  /**
   * A fast-path request that retries used to leave a permanent phantom.
   *
   * `fastPath` was set on admission and never cleared, so a request admitted
   * without queueing that then hit a 429 and retried through the queue still
   * claimed to be a fast-path request when it finished. The completion handler
   * skips the queue removal for those, so the entry stayed — `inProgress`, and
   * marked as the thaw probe. From then on the queue was never empty (killing
   * the fast path) and `tryStartThawRequest` always answered "exists", so every
   * later request to that client timed out. Permanently, on the single most
   * ordinary sequence there is.
   */
  it("leaves no phantom queue entry when a fast-path request retries", async () => {
    upstream.script(429, 200);
    await withHandler(
      harness,
      upstream.baseURL,
      [
        {
          name: "phantom",
          // A single token, so the first attempt fast-paths and spends it and
          // the retry necessarily takes the queue path. Sizing the bucket
          // generously instead made the reproduction depend on whether the
          // freeze landed before the retry re-checked admission, which is a
          // pub/sub round-trip on Redis and a microtask in memory — so the bug
          // reproduced on one backend and not the other.
          rateLimit: {
            type: "requestLimit",
            maxTokens: 1,
            tokensToAdd: 1,
            interval: 150,
          },
        },
      ],
      async (handler) => {
        const res = await fire(handler, "phantom:_:a");
        expect(res.status).toBe(200);

        // The queue entry is removed by the controller when it receives the
        // completion broadcast, which is a real round-trip on Redis — so poll
        // rather than assert instantly. The bug this guards leaves the entry
        // there forever, so a deadline still catches it.
        const drained = await waitFor(async () => {
          const stats = await handler.getClientStats("phantom:_:a");
          return (
            stats.requestsInQueue.count === 0 &&
            stats.requestsInProgress.count === 0
          );
        });
        expect(drained).toBe(true);

        // The real symptom: everything afterwards used to time out.
        const after = await Promise.all([
          fire(handler, "phantom:_:a"),
          fire(handler, "phantom:_:a"),
        ]);
        expect(after.map((r) => r.status)).toEqual([200, 200]);
      },
      "rg"
    );
  }, 30_000);

  /**
   * `noLimit` means "no budget of our own", not "ignore a 429".
   *
   * Admission returned true unconditionally, so the request never entered the
   * queue, freeze state was never consulted, and a hard rate-limit response
   * slowed the client by nothing at all.
   */
  it("honours a freeze on a noLimit client", async () => {
    await withHandler(
      harness,
      upstream.baseURL,
      [{ name: "nolimit", rateLimit: { type: "noLimit" } }],
      async (_handler, backend) => {
        const freezeKey = "rg:requestHandler:nolimit:_:a:freezeState";
        await backend.setFreezeState(freezeKey, Date.now() + 60_000, 3);
        const state = await backend.getFreezeState(freezeKey);
        expect(state?.frozenUntil).toBeGreaterThan(Date.now());

        // canProcessRequest is what admission now consults.
        const decision = await backend.canProcessRequest(freezeKey);
        expect(decision.canProcess).toBe(false);
      },
      "rg"
    );
  }, 30_000);
});

/**
 * `sharedLimit` shares a budget. It must not share credentials.
 *
 * The client is constructed with its parent's name so the queue, bucket and
 * freeze state resolve to the parent's keys — but the OAuth token cache was
 * derived from that same name, so two clients with deliberately different
 * credentials filed their tokens under one key. Whichever refreshed first won
 * and the other silently sent the first one's access token. Both requests
 * succeed against the vendor, so this surfaced as cross-account access rather
 * than as an error.
 */
describe("sharedLimit credential isolation", () => {
  it("mints and sends each client's own token", async () => {
    const tokenRequests: string[] = [];
    const bearers: string[] = [];

    const server = createServer((req, res) => {
      if ((req.url ?? "").startsWith("/token")) {
        let body = "";
        req.on("data", (c) => (body += c));
        req.on("end", () => {
          const clientId = new URLSearchParams(body).get("client_id") ?? "?";
          tokenRequests.push(clientId);
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(
            JSON.stringify({
              access_token: `TOKEN_FOR_${clientId}`,
              expires_in: 3600,
              token_type: "Bearer",
            })
          );
        });
        return;
      }
      bearers.push(String(req.headers.authorization ?? ""));
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end('{"ok":true}');
    });
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
    const addr = server.address();
    const port = typeof addr === "object" && addr ? addr.port : 0;
    const baseURL = `http://127.0.0.1:${port}`;

    const { memoryBackend } =
      await import("../packages/core/src/backend/memory.js");
    const backend = memoryBackend();
    const handler = new RequestHandler({
      key: KEY,
      backend,
      keyPrefix: "iso",
    });

    const oauth = (clientId: string) => ({
      type: "oauth2" as const,
      clientId,
      clientSecret: `${clientId}_SECRET`,
      refreshConfig: {
        url: `${baseURL}/token`,
        dataLocation: "urlEncodedForm" as const,
        data: {
          grant_type: "client_credentials",
          client_id: "{{clientId}}",
          client_secret: "{{clientSecret}}",
        },
      },
    });

    await handler.registerClientTemplate(
      "iso" as never,
      (() => [
        {
          name: "isoparent:_:a",
          rateLimit: {
            type: "requestLimit",
            maxTokens: 50,
            tokensToAdd: 50,
            interval: 1000,
          },
          authentication: oauth("PARENT_ID"),
          requestOptions: { defaults: { baseURL } },
        },
        {
          name: "isochild:_:a",
          rateLimit: { type: "sharedLimit", clientName: "isoparent:_:a" },
          authentication: oauth("CHILD_ID"),
          requestOptions: { defaults: { baseURL } },
        },
      ]) as never
    );

    await handler.start();
    await handler.addTemplateClient(
      "iso" as never,
      {
        instanceId: "a",
      } as never
    );

    try {
      await handler.handleRequest({
        clientName: "isochild:_:a",
        requestName: "t.child",
        method: "GET",
        url: "/api",
      });
      await handler.handleRequest({
        clientName: "isoparent:_:a",
        requestName: "t.parent",
        method: "GET",
        url: "/api",
      });

      // Each client authenticates as itself.
      expect(bearers).toEqual([
        "Bearer TOKEN_FOR_CHILD_ID",
        "Bearer TOKEN_FOR_PARENT_ID",
      ]);
      // And each one actually minted a token, rather than borrowing.
      expect([...tokenRequests].sort()).toEqual(["CHILD_ID", "PARENT_ID"]);
    } finally {
      await handler.stop();
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    }
  }, 30_000);
});

/**
 * The credential hash holds the refresh token too, so its TTL has to outlive
 * the refresh token — not the access token. Sizing it from `expires_in` dropped
 * both about an hour after issue and left the grant unrecoverable.
 */
describe("credential TTL", () => {
  it("outlives the refresh token, not the access token", () => {
    const oneHour = 3600;
    const ninetyDays = 60 * 60 * 24 * 90;
    expect(credentialTtlSeconds(oneHour, true, ninetyDays)).toBeGreaterThan(
      ninetyDays
    );
  });

  it("assumes a long-lived refresh token when the provider is silent", () => {
    // Providers routinely omit refresh_token_expires_in. Falling back to the
    // access token's hour is what killed grants.
    expect(credentialTtlSeconds(3600, true)).toBeGreaterThan(3600 * 24);
  });

  it("still tracks the access token when nothing else is stored", () => {
    expect(credentialTtlSeconds(3600, false)).toBe(3660);
  });

  it("never returns a TTL below the floor", () => {
    expect(credentialTtlSeconds(-500, false)).toBe(60);
  });
});

/**
 * A token bucket that cannot refill reports "not acquired, wait 0ms" forever,
 * and the acquire loop ignored the error alongside it. That loop runs while
 * `processRequests` holds its lock, so the client stopped admitting anything
 * and hammered the backend until the process died. Nothing validated the
 * config at any layer.
 */
describe("unusable rate limits", () => {
  const build = async (rateLimit: Record<string, unknown>) => {
    const { memoryBackend } =
      await import("../packages/core/src/backend/memory.js");
    const backend = memoryBackend();
    const handler = new RequestHandler({
      key: KEY,
      backend,
      keyPrefix: "bad",
      defaultClientOptions: { name: "default", rateLimit } as never,
    });
    try {
      await handler.start();
    } finally {
      await handler.stop().catch(() => {});
      await backend.close().catch(() => {});
    }
  };

  it("rejects tokensToAdd of 0 at construction", async () => {
    await expect(
      build({
        type: "requestLimit",
        maxTokens: 10,
        tokensToAdd: 0,
        interval: 1000,
      })
    ).rejects.toThrow(/tokensToAdd must be greater than 0/);
  });

  it("rejects a non-positive interval", async () => {
    await expect(
      build({
        type: "requestLimit",
        maxTokens: 10,
        tokensToAdd: 1,
        interval: 0,
      })
    ).rejects.toThrow(/interval must be greater than 0/);
  });

  it("rejects a NaN budget rather than wedging later", async () => {
    await expect(
      build({
        type: "requestLimit",
        maxTokens: Number.NaN,
        tokensToAdd: 1,
        interval: 1000,
      })
    ).rejects.toThrow(/maxTokens must be greater than 0/);
  });
});

/**
 * A handler with one `requestLimit` client and a record of every attempt on the
 * bucket, admitted or declined.
 *
 * `withHandler` cannot serve the test below: a spin is a run of DECLINES, so
 * anything that counts successful claims sees a flat line whether the drain
 * re-enters once or a thousand times.
 */
async function withAttemptCounting(
  harness: Harness,
  baseURL: string,
  rateLimit: Record<string, unknown>,
  fn: (
    handler: RequestHandler,
    attempts: () => Array<{ cost: number; acquired: boolean }>
  ) => Promise<void>
) {
  const { backend: raw, cleanup } = await harness.create();
  const attempts: Array<{ cost: number; acquired: boolean }> = [];
  const backend = new Proxy(raw, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (typeof value !== "function") return value;
      const bound = value.bind(target) as (...a: never[]) => Promise<unknown>;
      if (prop !== "acquireTokens") return bound;
      return async (...args: never[]) => {
        const result = (await bound(...args)) as { acquired: boolean };
        attempts.push({
          cost: args[1] as unknown as number,
          acquired: result.acquired,
        });
        return result;
      };
    },
  });

  const handler = new RequestHandler({
    key: KEY,
    backend,
    keyPrefix: "rgspin",
  });
  await handler.registerClientTemplate(
    "spin" as never,
    (() => [
      {
        name: "spin:_:a",
        rateLimit: { type: "requestLimit", ...rateLimit },
        requestOptions: { defaults: { baseURL } },
      },
    ]) as never
  );
  await handler.start();
  await handler.addTemplateClient(
    "spin" as never,
    { instanceId: "a" } as never
  );

  try {
    await fn(handler, () => [...attempts]);
  } finally {
    // The parked request below is deliberately unsatisfiable, so give the
    // shutdown drain something short rather than the default.
    handler.setDrainTimeout(500);
    await handler.stop().catch(() => undefined);
    await cleanup();
  }
}

/**
 * A wake-up too far in the future for `setTimeout` used to become a hot spin.
 *
 * `setTimeout` silently collapses any delay above 2^31-1 ms (~24.8 days) to 1ms.
 * A daily- or monthly-quota client whose cost needs several refills computes
 * exactly such a wait — five refills of ~5.8 days here — so the drain loop woke
 * a millisecond later, recomputed the same enormous wait, re-booked it, and went
 * round again: ~900 attempts a second for one stuck request, each dragging a
 * queue read and a status write to the backend with it. Nothing failed, nothing
 * logged, and the only visible symptom was backend load. The delay is now capped
 * and the remainder re-booked, which is safe because waking early costs one
 * extra decline.
 *
 * Sibling of the unusable-budget cases above: there the bucket could never
 * refill, here it can, and both ended in the same tight loop against the
 * backend.
 */
describe.each(harnesses(8))("drain wake-up ceiling — $name", (harness) => {
  let upstream: Awaited<ReturnType<typeof startScriptedUpstream>>;

  beforeAll(async () => {
    upstream = await startScriptedUpstream();
  });
  afterAll(async () => {
    await upstream.close();
  });

  it("books one wake-up for a wait longer than a timer can hold", async () => {
    await withAttemptCounting(
      harness,
      upstream.baseURL,
      { interval: 500_000_000, tokensToAdd: 1, maxTokens: 5 },
      async (handler, attempts) => {
        // Spends the whole bucket, so the next request has to wait five refills.
        await handler.handleRequest({
          clientName: "spin:_:a",
          requestName: "t.drain",
          method: "GET",
          url: "/",
          cost: 5,
        });

        // Aborted rather than timed out, so the observation window belongs to
        // the test instead of to an admission budget.
        const controller = new AbortController();
        const parked = handler.handleRequest({
          clientName: "spin:_:a",
          requestName: "t.parked",
          method: "GET",
          url: "/",
          cost: 5,
          signal: controller.signal,
        });
        const queued = await waitFor(async () => {
          const stats = await handler.getClientStats("spin:_:a");
          return (
            stats.requestsInQueue.count + stats.requestsInProgress.count > 0
          );
        });
        expect(queued).toBe(true);

        const before = attempts().length;
        await new Promise((r) => setTimeout(r, 800));
        const during = attempts().length - before;
        // One in practice; the margin covers a wake-up coalescing with the
        // enqueue. The defect produced hundreds inside this window.
        expect(during).toBeLessThan(5);
        // And it WAS examined: a request nobody ever looked at also scores zero
        // attempts, which would pass for the wrong reason.
        expect(
          attempts().some((attempt) => attempt.cost === 5 && !attempt.acquired)
        ).toBe(true);

        controller.abort();
        await expect(parked).rejects.toThrow(/aborted/);
      }
    );
  }, 30_000);
});

/**
 * The encryption key protects every stored credential, and an undefined one
 * used to surface far downstream as a TypeError from inside node:crypto about
 * an argument called "password". Every documented example reads it from an
 * environment variable, so that was the failure a missing variable produced.
 */
describe("handler key validation", () => {
  it("refuses an empty key", async () => {
    const { memoryBackend } =
      await import("../packages/core/src/backend/memory.js");
    expect(
      () => new RequestHandler({ key: "", backend: memoryBackend() })
    ).toThrow(/non-empty `key`/);
  });

  it("refuses a missing key", async () => {
    const { memoryBackend } =
      await import("../packages/core/src/backend/memory.js");
    expect(
      () =>
        new RequestHandler({
          backend: memoryBackend(),
        } as unknown as ConstructorParameters<typeof RequestHandler>[0])
    ).toThrow(/non-empty `key`/);
  });
});

/**
 * `rateLimit` is optional and documented as defaulting to `noLimit`, so only an
 * ABSENT property may take that default. A property that is present and null or
 * undefined has to throw: `rateLimit: null` is what `JSON.parse` of a backend
 * override or a nullable database column produces, and `rateLimit: row.limit ??
 * undefined` on a sub-client is worse — the merge spread overwrites the parent's
 * limit with it. Normalising either with `??` silently removes a cap.
 */
describe("an absent rate limit is the only one that defaults", () => {
  const build = async (rateLimit: unknown, extra?: object) => {
    const { memoryBackend } =
      await import("../packages/core/src/backend/memory.js");
    const handler = new RequestHandler({ key: KEY, backend: memoryBackend() });
    handler.setDrainTimeout(100);
    await handler.registerClientTemplate(
      "caps" as never,
      ((creds: { instanceId: string }) => [
        {
          name: `caps:_:${creds.instanceId}`,
          ...extra,
          ...(rateLimit as object),
        },
      ]) as never
    );
    await handler.start();
    return {
      handler,
      add: () =>
        handler.addLocalTemplateClient(
          "caps" as never,
          {
            instanceId: "a",
          } as never
        ),
    };
  };

  it("omitting rateLimit entirely yields the documented noLimit default", async () => {
    const { handler, add } = await build({});
    try {
      await add();
      const loaded = handler
        .getLoadedClients()
        .find((c) => c.name === "caps:_:a");
      expect(loaded?.rateLimit).toEqual({ type: "noLimit" });
    } finally {
      await handler.stop();
    }
  });

  it("refuses an explicitly null rateLimit rather than treating it as no limit", async () => {
    const { handler, add } = await build({ rateLimit: null });
    try {
      await expect(add()).rejects.toThrow(/rateLimit as null/);
      expect(handler.getLoadedClients().map((c) => c.name)).not.toContain(
        "caps:_:a"
      );
    } finally {
      await handler.stop();
    }
  });

  it("refuses an explicitly undefined rateLimit, which a `?? undefined` produces", async () => {
    const { handler, add } = await build({ rateLimit: undefined });
    try {
      await expect(add()).rejects.toThrow(/rateLimit as null/);
    } finally {
      await handler.stop();
    }
  });

  /**
   * The dangerous one: the merge spread copies an explicit `undefined` over the
   * parent's limit, so a child written as `rateLimit: row.limit ?? undefined`
   * would inherit no cap rather than its parent's.
   */
  it("refuses an explicitly undefined rateLimit on a sub-client", async () => {
    const { handler, add } = await build(
      {
        rateLimit: {
          type: "requestLimit",
          interval: 1_000,
          tokensToAdd: 5,
          maxTokens: 5,
        },
      },
      { subClients: [{ name: "child", rateLimit: undefined }] }
    );
    try {
      await expect(add()).rejects.toThrow(/rateLimit as null/);
    } finally {
      await handler.stop();
    }
  });

  /** A misspelled type must not fall back to "no limit" either. */
  it("refuses a named rate-limit type it does not recognise", async () => {
    const { handler, add } = await build({
      rateLimit: { type: "requestLimitt", interval: 1, tokensToAdd: 1 },
    });
    try {
      await expect(add()).rejects.toThrow(/requestLimit/);
    } finally {
      await handler.stop();
    }
  });
});
