import { credentialTtlSeconds } from "../packages/core/src/utils/credentialTtl.js";
import { buildClientName } from "../packages/core/src/utils/clientName.js";
import type RequestHandler from "../packages/core/src/index.js";
import { RequestError } from "../packages/core/src/errors.js";
import axios, { type AxiosError } from "axios";
import { describe, expect, it } from "vitest";
import type { AddressInfo } from "node:net";
import http from "node:http";
import util from "node:util";
import {
  redactBody,
  redactHeaders,
  redactUrl,
  sanitizeError,
} from "../packages/core/src/utils/redact.js";
import {
  harnesses,
  startUpstream,
  withHandler,
} from "./clientTypes/harness.js";

/**
 * Regressions for the findings raised in the credential / rate-limit / queueing
 * audit. Each block names the defect it pins down, because the fix is only
 * obviously correct next to the failure it replaced.
 */

// ---------------------------------------------------------------- redaction

/**
 * Redaction used to guard the library's own log line and nothing else. The
 * error handed to the caller carried the live token in `cause.config.headers`,
 * and `cause` was an own enumerable property, so `JSON.stringify(err)` — an
 * ordinary thing for a host application to do — printed it.
 */
describe("redaction", () => {
  const SECRET = "SUPER_SECRET_ACCESS_TOKEN";
  const CLIENT_SECRET = "MY_CLIENT_SECRET";

  /**
   * A REAL axios error, from a real socket.
   *
   * The previous version of this fixture was hand-built, and its `response` had
   * no `request` key — so `delete err.request` was enough to make the assertions
   * pass while production still leaked through `err.response.request`, which
   * axios sets to the same `ClientRequest`. A fixture that cannot reproduce the
   * shape being defended against is worse than no test, because it is why nobody
   * looked again.
   */
  async function realAxiosError() {
    const server = http.createServer((_req, res) => {
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "boom" }));
    });
    await new Promise<void>((resolve) => server.listen(0, () => resolve()));
    const { port } = server.address() as AddressInfo;
    try {
      await axios.post(
        `http://127.0.0.1:${port}/v1/ship`,
        `client_secret=${CLIENT_SECRET}&grant_type=client_credentials`,
        {
          headers: {
            Authorization: `Bearer ${SECRET}`,
            "X-Custom-Key": "CUSTOM_SECRET_VALUE",
          },
        }
      );
      throw new Error("expected the request to fail");
    } catch (error) {
      return error as AxiosError;
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  }

  it("keeps credentials out of a thrown error, however it is serialized", async () => {
    const raw = await realAxiosError();
    // The fixture is only meaningful if it carries the leak in the first place.
    expect(String((raw.request as { _header?: string })?._header)).toContain(
      SECRET
    );
    expect((raw.response as { request?: unknown })?.request).toBe(raw.request);

    const sanitized = sanitizeError(raw, new Set(["x-custom-key"]));
    const err = new RequestError("ship_failed", "Could not ship", {
      cause: sanitized as Error,
    });

    const json = JSON.stringify(err);
    expect(json).not.toContain(SECRET);
    expect(json).not.toContain(CLIENT_SECRET);
    expect(util.inspect(err, { depth: null })).not.toContain(SECRET);
    expect(util.inspect(err, { depth: null })).not.toContain(CLIENT_SECRET);
    // Both references to the live ClientRequest, whose raw header block cannot
    // be made safe by redaction.
    expect((sanitized as { request?: unknown }).request).toBeUndefined();
    expect(
      (sanitized as { response?: { request?: unknown } }).response?.request
    ).toBeUndefined();
  });

  it("does not serialize `cause` by default, matching Error's own behaviour", () => {
    const err = new RequestError("x", "y", { cause: new Error("inner") });
    expect(Object.getOwnPropertyDescriptor(err, "cause")?.enumerable).toBe(
      false
    );
    expect(err.cause).toBeInstanceOf(Error);
  });

  it("redacts credentials by value, not just by key name", () => {
    // The leak that survived the old redactor: an upstream echoing a header
    // back under a name no denylist would think to include.
    const body = redactBody({
      error: "boom",
      echoed_authorization: `Bearer ${SECRET}`,
      jwt: "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig",
      harmless: "ok",
    }) as Record<string, string>;
    expect(body.echoed_authorization).toBe("[REDACTED]");
    expect(body.jwt).toBe("[REDACTED]");
    expect(body.harmless).toBe("ok");
  });

  it("survives a cyclic body instead of throwing from inside the catch", () => {
    const cyclic: Record<string, unknown> = { a: 1 };
    cyclic.self = cyclic;
    expect(() => redactBody(cyclic)).not.toThrow();
  });

  it("covers the query-string cases the old redactUrl missed", () => {
    expect(redactUrl("/x?subscription-key=SECRET123")).not.toContain(
      "SECRET123"
    );
    expect(redactUrl("/x?auth=S1&key=S2&session=S3")).not.toContain("S1");
    // Repeated parameters used to be silently collapsed, dropping data.
    const repeated = redactUrl("/x?token=A&token=B&id=1") ?? "";
    expect(repeated.match(/token=/g)).toHaveLength(2);
    expect(repeated).toContain("id=1");
    // Re-encoding used to turn an encoded value into what looked like a real
    // parameter, fabricating a plausible client_secret in the logs.
    expect(
      redactUrl("/x?token=A&note=a%26client_secret%3DSTILL_HERE")
    ).not.toContain("client_secret=STILL_HERE");
    expect(redactUrl("https://u:PASSWORD@api.v.com/x")).not.toContain(
      "PASSWORD"
    );
  });

  it("redacts a consumer's own credential header when told about it", () => {
    const headers = redactHeaders(
      { "X-Subscription-Key": "VENDOR_KEY", Accept: "application/json" },
      new Set(["x-subscription-key"])
    ) as Record<string, string>;
    expect(headers["X-Subscription-Key"]).toBe("[REDACTED]");
    expect(headers.Accept).toBe("application/json");
  });
});

// --------------------------------------------------------------- credentials

/**
 * The TTL covers the whole credential hash, and `hset` leaves fields it is not
 * given. Sizing it from a refresh response that omitted `refresh_token` — which
 * RFC 6749 §6 permits, and several major providers do — dropped a refresh token
 * that was still stored and still valid, about an hour after issue.
 */
describe("credential TTL", () => {
  it("sizes from a stored refresh token when the response omits one", () => {
    const ninetyDays = 60 * 60 * 24 * 90;
    const withRefresh = credentialTtlSeconds(3600, true, ninetyDays);
    // The caller now passes hasRefreshToken=true based on stored state, so the
    // hour-long access token cannot shrink the key below the refresh token.
    expect(withRefresh).toBeGreaterThan(ninetyDays);
    expect(credentialTtlSeconds(3600, false)).toBe(3660);
  });
});

/**
 * Client names are colon-delimited and positional, and they key the OAuth token
 * cache. An unvalidated colon let two tenants build one name — and one then
 * authenticated as the other.
 */
describe("client name segments", () => {
  it("rejects a colon that would collapse two tenants onto one client", () => {
    expect(() =>
      buildClientName("vend", { organizationId: "a", instanceId: "b:c" })
    ).toThrow(/must not contain ":"/);
    expect(() =>
      buildClientName("vend", { organizationId: "a:b", instanceId: "c" })
    ).toThrow(/must not contain ":"/);
  });

  it("rejects the reserved global sentinel as a real organization id", () => {
    expect(() =>
      buildClientName("vend", { organizationId: "_", instanceId: "x" })
    ).toThrow(/reserved/);
  });

  it("rejects whitespace, which namespace derivation would normalize away", () => {
    expect(() => buildClientName("vend", { instanceId: "tenant 1" })).toThrow(
      /whitespace/
    );
  });

  it("still builds ordinary names", () => {
    expect(buildClientName("vend", { instanceId: "prod" })).toBe("vend:_:prod");
    expect(
      buildClientName("vend", { organizationId: "acme", instanceId: "prod" })
    ).toBe("vend:acme:prod");
  });
});

// ------------------------------------------------------- admission behaviour

const fire = (
  handler: RequestHandler,
  clientName: string,
  extra: Record<string, unknown> = {}
) =>
  handler.handleRequest({
    clientName,
    requestName: "t.noop",
    method: "GET",
    url: "/",
    ...extra,
  } as never);

for (const harness of harnesses(7)) {
  describe(`audit fixes — '${harness.name}'`, () => {
    /**
     * `handlePreRequest` runs the interceptor and the auth flow *after*
     * admission has claimed a slot. A throw there used to publish no
     * `requestDone`, so the slot was never released — one credential outage took
     * the client out for a full `requestTtl`.
     */
    it("releases the slot when a pre-request step throws", async () => {
      const upstream = await startUpstream(0);
      let explode = true;
      try {
        await withHandler(
          harness,
          upstream.baseURL,
          [
            {
              name: "wedge",
              rateLimit: { type: "concurrencyLimit", maxConcurrency: 1 },
            },
          ],
          async (handler) => {
            const client = (
              handler as unknown as { clients: Map<string, any> }
            ).clients.get("wedge:_:a");
            client.requestOptions = {
              ...client.requestOptions,
              cleanupTimeout: 3000,
              requestInterceptor: (cfg: unknown) => {
                if (explode) throw new Error("credential store unavailable");
                return cfg;
              },
            };

            await expect(fire(handler, "wedge:_:a")).rejects.toThrow(
              /credential store unavailable/
            );

            explode = false;
            const started = Date.now();
            await fire(handler, "wedge:_:a");
            // Previously this timed out at cleanupTimeout with the upstream
            // never contacted.
            expect(Date.now() - started).toBeLessThan(1500);
            expect(upstream.servedCount()).toBe(1);
          }
        );
      } finally {
        await upstream.close();
      }
    }, 30000);

    /**
     * The over-budget guard covered `requestLimit` only. On a concurrency
     * client an oversized cost could never be admitted, and because the drain
     * loop stopped on it, it blocked everything behind it for its whole
     * `cleanupTimeout`.
     */
    it("rejects an impossible cost up front instead of stalling the client", async () => {
      const upstream = await startUpstream(0);
      try {
        await withHandler(
          harness,
          upstream.baseURL,
          [
            {
              name: "cost",
              rateLimit: { type: "concurrencyLimit", maxConcurrency: 2 },
            },
          ],
          async (handler) => {
            const started = Date.now();
            await expect(
              fire(handler, "cost:_:a", { cost: 5 })
            ).rejects.toThrow(/exceeds the maximum/i);
            expect(Date.now() - started).toBeLessThan(1000);

            // And traffic that fits is unaffected.
            const fits = Date.now();
            await Promise.all([
              fire(handler, "cost:_:a"),
              fire(handler, "cost:_:a"),
              fire(handler, "cost:_:a"),
            ]);
            expect(Date.now() - fits).toBeLessThan(1500);
          }
        );
      } finally {
        await upstream.close();
      }
    }, 30000);

    /**
     * `maxConcurrency: NaN` used to remove the cap entirely on the memory
     * backend and stop the client serving on Redis — the same config, opposite
     * failures, neither reported.
     */
    it("refuses an unusable concurrency ceiling at construction", async () => {
      const upstream = await startUpstream(0);
      try {
        await expect(
          withHandler(
            harness,
            upstream.baseURL,
            [
              {
                name: "nan",
                rateLimit: {
                  type: "concurrencyLimit",
                  maxConcurrency: Number("not-a-number"),
                },
              },
            ],
            async () => {}
          )
        ).rejects.toThrow(/unusable concurrencyLimit/);
      } finally {
        await upstream.close();
      }
    }, 30000);

    /**
     * `stop()` cleared its intervals and returned, leaving queued requests to
     * discover the handler was gone when their own timeout fired — up to a
     * minute after SIGTERM, despite the README promising a drain.
     */
    it("drains the queue on shutdown rather than abandoning it", async () => {
      const upstream = await startUpstream(120);
      try {
        const { backend, cleanup } = await harness.create();
        const { default: RequestHandler } =
          await import("../packages/core/src/index.js");
        const handler = new RequestHandler({
          key: "0123456789abcdef0123456789abcdef",
          backend,
          keyPrefix: "drain",
        });
        await handler.registerClientTemplate(
          "d" as never,
          ((creds: { instanceId: string }) => [
            {
              name: `d:_:${creds.instanceId}`,
              rateLimit: {
                type: "requestLimit",
                maxTokens: 2,
                tokensToAdd: 2,
                interval: 300,
              },
              requestOptions: { defaults: { baseURL: upstream.baseURL } },
            },
          ]) as never
        );
        await handler.start();
        await handler.addTemplateClient(
          "d" as never,
          {
            instanceId: "a",
          } as never
        );

        const inFlight = Array.from({ length: 6 }, () =>
          fire(handler, "d:_:a").then(
            () => "ok",
            (e) => `failed: ${(e as Error).message}`
          )
        );
        await new Promise((r) => setTimeout(r, 150));
        await handler.stop();
        const outcomes = await Promise.all(inFlight);

        // Everything either completed or was told why it could not — nothing
        // was left to time out silently.
        expect(outcomes.every((o) => o === "ok")).toBe(true);
        await cleanup();
      } finally {
        await upstream.close();
      }
    }, 30000);

    /**
     * `stop()` returned while a request was still upstream, so the release that
     * returns its concurrency slot ran after the host had closed its own connection
     * — or never, because the process had already exited. Nothing else reclaims that
     * slot: orphan cleanup only prunes queue entries, and the slot TTL is measured
     * from the claim, so a replacement replica came up against a ceiling occupied by
     * a request that had finished.
     *
     * The guarantee is an ordering one, which is why the assertion is that `stop()`
     * has not resolved while work is still upstream. Reading the ledger afterwards
     * cannot show it: `close()` only closes the pub/sub duplicate, so the caller's
     * connection is still open inside the test and the release succeeds either way.
     */
    it("does not finish stopping while a request is still upstream", async () => {
      const upstream = await startUpstream(1200);
      try {
        const { backend, cleanup } = await harness.create();
        const { default: RequestHandler } =
          await import("../packages/core/src/index.js");
        const handler = new RequestHandler({
          key: "0123456789abcdef0123456789abcdef",
          backend,
          keyPrefix: "residue",
        });
        await handler.registerClientTemplate(
          "r" as never,
          ((creds: { instanceId: string }) => [
            {
              name: `r:_:${creds.instanceId}`,
              rateLimit: { type: "concurrencyLimit", maxConcurrency: 1 },
              requestOptions: { defaults: { baseURL: upstream.baseURL } },
            },
          ]) as never
        );
        await handler.start();
        await handler.addTemplateClient(
          "r" as never,
          { instanceId: "a" } as never
        );

        // Shorter than the upstream takes, so the drain gives up with the request
        // still open — the window the leak lived in.
        handler.setDrainTimeout(600);
        let settled = false;
        const pending = fire(handler, "r:_:a").then(
          () => {
            settled = true;
            return "ok";
          },
          (e) => {
            settled = true;
            return `failed: ${(e as Error).message}`;
          }
        );
        await new Promise((r) => setTimeout(r, 150));
        await handler.stop();

        // The slot's release runs when the response lands, so stopping before that
        // leaves it to the slot TTL.
        expect(settled).toBe(true);
        expect(await pending).toBe("ok");

        const slots = await backend.getConcurrencyState(
          `${handler.getNamespace()}:r:_:a:concurrency:default`,
          120_000
        );
        expect(slots.activeRequests).toEqual([]);
        await cleanup();
      } finally {
        await upstream.close();
      }
    }, 30000);
  });
}
