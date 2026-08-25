import { RequestCostExceedsBudgetError } from "../../packages/core/src/errors.js";
import { fireMany, harnesses, startUpstream, withHandler } from "./harness.js";
import type { MultiLimitSpec } from "../../packages/core/src/backend/types.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createServer } from "node:http";

/**
 * Several rate limits on one client.
 *
 * The promise is a conjunction: a request goes out only when *every* declared
 * budget can take it. The failure that matters is not refusing too much — it is
 * spending a budget on a request that another budget then declines, because
 * nothing on the decline path hands that spend back. So the first block below
 * asserts the all-or-nothing property directly against the backends, where it
 * can be read off the balances, and the rest asserts what a client does with it.
 */

const MINUTE = 60_000;
const HOUR = 3_600_000;

// -------------------------------------------------------- backend primitives

describe.each(harnesses(4))("multiLimit backend — $name", (harness) => {
  const P = "ml:budget";
  const ample = { maxTokens: 10, tokensToAdd: 10, interval: MINUTE };
  const scarce = { maxTokens: 1, tokensToAdd: 1, interval: HOUR };

  const pair = (): MultiLimitSpec[] => [
    { kind: "tokenBucket", key: `${P}:ample`, config: ample },
    { kind: "tokenBucket", key: `${P}:scarce`, config: scarce },
  ];

  const withBackend = async (
    fn: (
      backend: Awaited<ReturnType<typeof harness.create>>["backend"]
    ) => Promise<void>
  ) => {
    const { backend, cleanup } = await harness.create();
    try {
      await fn(backend);
    } finally {
      await cleanup();
    }
  };

  it("spends nothing when one budget declines", async () => {
    await withBackend(async (backend) => {
      // The first request empties the scarce budget; the second must be refused
      // by it, and must leave the ample budget exactly where the first left it.
      // A backend claiming in turn instead would show 8 here, and those two
      // tokens would never come back.
      expect((await backend.acquireMultiLimit!(pair(), 1)).acquired).toBe(true);

      const declined = await backend.acquireMultiLimit!(pair(), 1);
      expect(declined.acquired).toBe(false);

      const state = await backend.getTokenBucketState(`${P}:ample`, ample);
      expect(state.tokens).toBe(9);
    });
  });

  it("reports the longest wait any budget asks for", async () => {
    await withBackend(async (backend) => {
      await backend.acquireMultiLimit!(pair(), 1);
      const declined = await backend.acquireMultiLimit!(pair(), 1);

      // Waking on the ample budget's much sooner refill would only be refused by
      // the scarce one again, so the wait has to be the scarce one's.
      expect(declined.waitTime).toBeGreaterThan(HOUR / 2);
      expect(declined.blockedBy).toBe(`${P}:scarce`);
    });
  });

  it("claims a slot and a bucket together, and hands both back together", async () => {
    await withBackend(async (backend) => {
      const specs: MultiLimitSpec[] = [
        { kind: "tokenBucket", key: `${P}:ample`, config: ample },
        {
          kind: "concurrency",
          key: `${P}:slots`,
          config: { maxConcurrency: 2, requestTtl: 120_000 },
          slotId: "req-1",
        },
      ];

      expect((await backend.acquireMultiLimit!(specs, 1)).acquired).toBe(true);
      expect(
        (await backend.getConcurrencyState(`${P}:slots`, 120_000))
          .currentConcurrency
      ).toBe(1);

      await backend.releaseMultiLimit!(specs, 1);

      expect(
        (await backend.getTokenBucketState(`${P}:ample`, ample)).tokens
      ).toBe(10);
      expect(
        (await backend.getConcurrencyState(`${P}:slots`, 120_000))
          .currentConcurrency
      ).toBe(0);
    });
  });

  it("refuses a full concurrency ledger without spending the bucket beside it", async () => {
    await withBackend(async (backend) => {
      const slot = (slotId: string): MultiLimitSpec[] => [
        { kind: "tokenBucket", key: `${P}:ample`, config: ample },
        {
          kind: "concurrency",
          key: `${P}:slots`,
          config: { maxConcurrency: 1, requestTtl: 120_000 },
          slotId,
        },
      ];

      expect((await backend.acquireMultiLimit!(slot("a"), 1)).acquired).toBe(
        true
      );
      expect((await backend.acquireMultiLimit!(slot("b"), 1)).acquired).toBe(
        false
      );

      expect(
        (await backend.getTokenBucketState(`${P}:ample`, ample)).tokens
      ).toBe(9);
    });
  });

  it("reports an unusable budget rather than metering against it", async () => {
    await withBackend(async (backend) => {
      const result = await backend.acquireMultiLimit!(
        [
          { kind: "tokenBucket", key: `${P}:ample`, config: ample },
          {
            kind: "tokenBucket",
            key: `${P}:broken`,
            config: { maxTokens: 5, tokensToAdd: 0, interval: 1000 },
          },
        ],
        1
      );

      expect(result.acquired).toBe(false);
      expect(result.error).toContain("tokensToAdd");
      // Refused before anything was committed, so the healthy budget is whole.
      expect(
        (await backend.getTokenBucketState(`${P}:ample`, ample)).tokens
      ).toBe(10);
    });
  });

  it("refuses the fast path while anything is queued or a freeze stands", async () => {
    await withBackend(async (backend) => {
      const queueKey = `${P}:queue`;
      const freezeKey = `${P}:freeze`;

      expect(
        await backend.tryAdmitMultiLimit!(queueKey, [freezeKey], pair(), 1)
      ).toBe(true);

      await backend.setFreezeState(freezeKey, Date.now() + 60_000, 0);
      expect(
        await backend.tryAdmitMultiLimit!(queueKey, [freezeKey], pair(), 1)
      ).toBe(false);
    });
  });
});

// ------------------------------------------------------------ client behaviour

describe.each(harnesses(4))("multiLimit — $name", (harness) => {
  let upstream: Awaited<ReturnType<typeof startUpstream>>;

  beforeAll(async () => {
    upstream = await startUpstream(0);
  });
  afterAll(async () => {
    await upstream.close();
  });

  const CLIENT = "ml:_:a";
  const NS = "ct:requestHandler:ml:_:a";

  const perSecond = {
    name: "per_second",
    type: "requestLimit",
    interval: 1000,
    tokensToAdd: 20,
    maxTokens: 20,
  };
  const perWindow = {
    name: "per_window",
    type: "requestLimit",
    interval: HOUR,
    tokensToAdd: 6,
    maxTokens: 6,
  };

  const run = (
    rateLimit: Record<string, unknown>[],
    fn: Parameters<typeof withHandler>[3]
  ) => withHandler(harness, upstream.baseURL, [{ name: "ml", rateLimit }], fn);

  it("spends every declared budget for one request", async () => {
    await run([perSecond, perWindow], async (handler, backend) => {
      await handler.handleRequest({
        clientName: CLIENT,
        requestName: "t.one",
        method: "GET",
        url: "/",
      });

      // Each limit keys its own bucket by name, under the client's namespace.
      const second = await backend.getTokenBucketState(
        `${NS}:rateLimit:per_second`,
        { maxTokens: 20, tokensToAdd: 20, interval: 1000 }
      );
      const window = await backend.getTokenBucketState(
        `${NS}:rateLimit:per_window`,
        { maxTokens: 6, tokensToAdd: 6, interval: HOUR }
      );
      expect(window.tokens).toBe(5);
      // Refills on a 1s interval, so this is only ever read immediately after.
      expect(second.tokens).toBeLessThanOrEqual(19);
    });
  }, 30_000);

  it("is held by the tighter budget, not the looser one", async () => {
    // Twenty per second would serve all six instantly; six per hour cannot serve
    // a seventh at all. Six complete, and the balances say why.
    await run([perSecond, perWindow], async (handler, backend) => {
      const results = await fireMany(handler, CLIENT, 6);
      expect(results.every((r) => r.status === 200)).toBe(true);

      const window = await backend.getTokenBucketState(
        `${NS}:rateLimit:per_window`,
        { maxTokens: 6, tokensToAdd: 6, interval: HOUR }
      );
      expect(window.tokens).toBe(0);
    });
  }, 30_000);

  it("refuses a cost the tightest budget can never admit", async () => {
    await run([perSecond, perWindow], async (handler) => {
      // Ten is inside `per_second`'s ceiling of twenty and beyond
      // `per_window`'s of six, so the tightest limit is what decides.
      await expect(
        handler.handleRequest({
          clientName: CLIENT,
          requestName: "t.costly",
          method: "GET",
          url: "/",
          cost: 10,
        })
      ).rejects.toBeInstanceOf(RequestCostExceedsBudgetError);
    });
  }, 30_000);

  it("caps what is in flight while still metering the rate", async () => {
    upstream.setDelay(60);
    upstream.resetCounters();
    try {
      await run(
        [
          {
            name: "per_second",
            type: "requestLimit",
            interval: 250,
            tokensToAdd: 4,
            maxTokens: 4,
          },
          { name: "in_flight", type: "concurrencyLimit", maxConcurrency: 2 },
        ],
        async (handler) => {
          const results = await fireMany(handler, CLIENT, 12);
          expect(results.every((r) => r.status === 200)).toBe(true);
          expect(upstream.peakInFlight()).toBeLessThanOrEqual(2);
        }
      );
    } finally {
      upstream.setDelay(0);
    }
  }, 30_000);

  it("reports one stats entry per declared limit, with live balances", async () => {
    await run([perSecond, perWindow], async (handler) => {
      await handler.handleRequest({
        clientName: CLIENT,
        requestName: "t.one",
        method: "GET",
        url: "/",
      });

      const { rateLimit } = await handler.getClientStats(CLIENT);
      expect(rateLimit.map((l) => l.name)).toEqual([
        "per_second",
        "per_window",
      ]);
      const window = rateLimit.find((l) => l.name === "per_window");
      expect(window && "tokens" in window ? window.tokens : undefined).toBe(5);
    });
  }, 30_000);

  it("lets a plain sharedLimit client draw on a multi-limit owner", async () => {
    // The child takes the owner's queue as its own — it is constructed with the
    // owner's name — so the owner's controller is what claims the budgets, and it
    // claims all of them however many the owner declares. Nothing about the child
    // has to know there is more than one.
    await withHandler(
      harness,
      upstream.baseURL,
      [
        { name: "own", rateLimit: [perSecond, perWindow] },
        {
          name: "bor",
          rateLimit: [{ type: "sharedLimit", clientName: "own:_:a" }],
        },
      ],
      async (handler, backend) => {
        const results = await fireMany(handler, "bor:_:a", 3);
        expect(results.every((r) => r.status === 200)).toBe(true);

        // Both of the owner's named budgets paid for the borrower's requests.
        const ns = "ct:requestHandler:own:_:a";
        const window = await backend.getTokenBucketState(
          `${ns}:rateLimit:per_window`,
          { maxTokens: 6, tokensToAdd: 6, interval: HOUR }
        );
        expect(window.tokens).toBe(3);
        const second = await backend.getTokenBucketState(
          `${ns}:rateLimit:per_second`,
          { maxTokens: 20, tokensToAdd: 20, interval: 1000 }
        );
        expect(second.tokens).toBeLessThanOrEqual(17);
      }
    );
  }, 30_000);

  it("does not empty a long-horizon budget over one 429", async () => {
    // A single `requestLimit` client zeroes its bucket on a 429, because the
    // response proves the bucket's picture of the vendor was wrong. With several
    // budgets the response does not say which was breached, and zeroing the daily
    // one over a per-second breach would stand the client down until tomorrow.
    // The freeze window is the stand-down instead.
    const rejecting = createServer((_req, res) => {
      res.writeHead(429);
      res.end("{}");
    });
    await new Promise<void>((r) => rejecting.listen(0, "127.0.0.1", () => r()));
    const address = rejecting.address();
    const port = typeof address === "object" && address ? address.port : 0;

    try {
      await withHandler(
        harness,
        `http://127.0.0.1:${port}`,
        [
          {
            name: "ml",
            rateLimit: [perSecond, perWindow],
            // One attempt, so the balance below counts the request rather than
            // the retries.
            retryOptions: { maxRetries: 0 },
          },
        ],
        async (handler, backend) => {
          await handler
            .handleRequest({
              clientName: CLIENT,
              requestName: "t.rejected",
              method: "GET",
              url: "/",
            })
            .catch(() => undefined);

          const window = await backend.getTokenBucketState(
            `${NS}:rateLimit:per_window`,
            { maxTokens: 6, tokensToAdd: 6, interval: HOUR }
          );
          // Down by the one request that was actually sent, and no further.
          expect(window.tokens).toBe(5);

          const freeze = await backend.getFreezeState(`${NS}:freezeState`);
          expect(freeze?.frozenUntil ?? 0).toBeGreaterThan(Date.now());
        }
      );
    } finally {
      await new Promise<void>((r) => rejecting.close(() => r()));
    }
  }, 30_000);
});

// --------------------------------------------------------------- configuration

/**
 * Rejections at registration. Every one of these is a configuration a backend
 * would happily meter against and get wrong — an unnamed entry has no stable
 * key, two entries sharing a name are one bucket metering two limits, and a
 * `sharedLimit` entry with nothing behind it is a budget nobody drains.
 *
 * The memory harness alone: these never reach a backend, and running them twice
 * would say nothing the first run did not.
 */
describe("multiLimit configuration", () => {
  const memory = harnesses(4)[0];
  let upstream: Awaited<ReturnType<typeof startUpstream>>;

  beforeAll(async () => {
    upstream = await startUpstream(0);
  });
  afterAll(async () => {
    await upstream.close();
  });

  const build = (clients: { name: string; rateLimit: unknown }[]) =>
    withHandler(
      memory,
      upstream.baseURL,
      clients as never,
      async () => {},
      "mlc"
    );

  const rejects = (rateLimit: unknown, message: RegExp) =>
    expect(build([{ name: "c", rateLimit }])).rejects.toThrow(message);

  it("names an unnamed entry `default` rather than refusing it", async () => {
    // A client with one limit never has to invent a name for it.
    await build([
      {
        name: "c",
        rateLimit: [
          {
            type: "requestLimit",
            interval: 1000,
            tokensToAdd: 1,
            maxTokens: 1,
          },
        ],
      },
    ]);
  }, 30_000);

  it("refuses a second entry with no name", async () => {
    // Both would take `default`, so both would meter against one bucket.
    await rejects(
      [
        { type: "requestLimit", interval: 1000, tokensToAdd: 1, maxTokens: 1 },
        {
          type: "requestLimit",
          interval: 60_000,
          tokensToAdd: 5,
          maxTokens: 5,
        },
      ],
      /two rate limits without a name/
    );
  }, 30_000);

  it("refuses a name that would not survive a backend key", async () => {
    await rejects([{ name: "Per Second", type: "noLimit" }], /needs a name/);
    await rejects([{ name: "per:second", type: "noLimit" }], /needs a name/);
  }, 30_000);

  it("refuses two entries sharing one name", async () => {
    await rejects(
      [
        { name: "same", type: "noLimit" },
        { name: "same", type: "noLimit" },
      ],
      /two rate limits named "same"/
    );
  }, 30_000);

  it("refuses an empty array", async () => {
    await rejects([], /empty rate-limit array/);
  }, 30_000);

  it("refuses an entry whose budget can never hand out a token", async () => {
    await rejects(
      [
        {
          name: "broken",
          type: "requestLimit",
          interval: 1000,
          tokensToAdd: 0,
          maxTokens: 5,
        },
      ],
      /rate limit "broken" has an unusable requestLimit/
    );
  }, 30_000);

  it("refuses a sharedLimit alongside a limit of its own", async () => {
    // A sharedLimit client has no queue of its own — it takes the owner's — so a
    // client that also had budgets of its own would need a second queue, and
    // nothing would arbitrate ordering between the two.
    await rejects(
      [
        {
          name: "per_second",
          type: "requestLimit",
          interval: 1000,
          tokensToAdd: 1,
          maxTokens: 1,
        },
        { name: "contract", type: "sharedLimit", clientName: "owner:_:a" },
      ],
      /sharedLimit must be the only limit/
    );
  }, 30_000);

  it("refuses two sharedLimit entries", async () => {
    await rejects(
      [
        { name: "a", type: "sharedLimit", clientName: "x:_:a" },
        { name: "b", type: "sharedLimit", clientName: "y:_:a" },
      ],
      /one shared budget at most/
    );
  }, 30_000);

  it("accepts a lone sharedLimit written as an array", async () => {
    // The same client either way it is written, so the array form is unwrapped
    // rather than given a second implementation.
    await build([
      {
        name: "owner",
        rateLimit: [
          {
            type: "requestLimit",
            interval: 1000,
            tokensToAdd: 1,
            maxTokens: 1,
          },
        ],
      },
      {
        name: "c",
        rateLimit: [
          { name: "contract", type: "sharedLimit", clientName: "owner:_:a" },
        ],
      },
    ]);
  }, 30_000);
});
