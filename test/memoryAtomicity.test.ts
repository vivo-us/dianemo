import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { memoryBackend } from "../packages/core/src/backend/memory.js";
import { harnesses } from "./clientTypes/harness.js";
import type {
  DianemoBackend,
  QueuedRequest,
} from "../packages/core/src/backend/types.js";

/**
 * The memory backend's atomicity claim, and the inputs that used to slip past
 * its guards.
 *
 * The claim is the one at the top of `memory.ts`: every operation is
 * synchronous, which is what makes it atomic. Three methods broke it by
 * awaiting a sibling mid-decision, and an `await` yields to the microtask queue
 * even when the promise is already settled — so a peer runs between the read
 * and the answer. Those cases need the interleaving rig below; a sequential
 * call can never show them.
 *
 * Redis DB 0 for the parity half — every other index is claimed by another
 * suite, `replicas.failover` having taken 15.
 */

/** Blocks the thread, so a microtask gap spans real time rather than none. */
function blockFor(ms: number) {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    /* spin */
  }
}

const queued = (
  requestId: string,
  overrides: Partial<QueuedRequest> = {}
): QueuedRequest => ({
  requestId,
  clientName: "c",
  requestName: "r",
  status: "pending",
  priority: 1,
  cost: 1,
  retries: 0,
  timestamp: Date.now(),
  isThawRequest: false,
  ownerId: "o",
  ...overrides,
});

describe("memory backend: a decision does not straddle a suspension", () => {
  let backend: DianemoBackend;

  beforeEach(() => {
    backend = memoryBackend();
  });
  afterEach(async () => {
    await backend.close();
  });

  it("never lists a queue with an atomic batch half-applied", async () => {
    const queueKey = "ma:q";
    const prefix = "ma:m";
    for (const id of ["a", "b", "c", "d"]) {
      expect(await backend.addRequest(queueKey, prefix, queued(id))).toBe(true);
    }

    const reading = backend.getAllRequests(queueKey, prefix);
    await Promise.resolve();
    await backend.batch(
      [
        { op: "hset", key: `${prefix}:a`, fields: { status: "inProgress" } },
        { op: "hset", key: `${prefix}:d`, fields: { status: "inProgress" } },
      ],
      { atomic: true }
    );

    const seen = await reading;
    const status = Object.fromEntries(seen.map((r) => [r.requestId, r.status]));
    expect(status.a).toBe(status.d);
    expect(seen.map((r) => r.status)).toEqual([
      "pending",
      "pending",
      "pending",
      "pending",
    ]);
  });

  it("answers canProcessRequest on the state and clock of one instant", async () => {
    const key = "ma:fz:cpr";
    const now = Date.now();
    await backend.hset(key, {
      frozenUntil: String(now + 200),
      thawRequestCount: "0",
    });

    // Queued BEFORE the call, so it runs ahead of the continuation of any await
    // the call makes — which is the only way to put real time in that gap.
    queueMicrotask(() => blockFor(400));
    expect(await backend.canProcessRequest(key)).toEqual({
      canProcess: false,
      isThawRequest: false,
      frozenUntil: now + 200,
    });
  });

  it("answers isFrozen on the state and clock of one instant", async () => {
    const key = "ma:fz:frozen";
    const now = Date.now();
    await backend.hset(key, {
      frozenUntil: String(now + 200),
      thawRequestCount: "0",
    });

    queueMicrotask(() => blockFor(400));
    expect(await backend.isFrozen(key)).toBe(true);
  });
});

describe("memory backend: an unusable concurrency cost", () => {
  let backend: DianemoBackend;
  const config = { maxConcurrency: 2, requestTtl: 60_000 };

  beforeEach(() => {
    backend = memoryBackend();
  });
  afterEach(async () => {
    await backend.close();
  });

  const unusable: Array<[string, number]> = [
    ["NaN", Number.NaN],
    ["Infinity", Number.POSITIVE_INFINITY],
    ["-Infinity", Number.NEGATIVE_INFINITY],
    ["a negative number", -5],
  ];

  it.each(unusable)("is refused by acquireConcurrency: %s", async (_, cost) => {
    expect(
      await backend.acquireConcurrency("mc:k", cost, "r1", config)
    ).toEqual({
      acquired: false,
      currentConcurrency: 0,
      error: "cost must be a finite number that is not negative",
    });
  });

  it.each(unusable)(
    "is refused by acquireQueuedConcurrency ahead of the status read: %s",
    async (_, cost) => {
      const metadataKey = "mc:meta";
      await backend.hset(metadataKey, { status: "inProgress" });
      expect(
        await backend.acquireQueuedConcurrency!(
          "mc:k",
          metadataKey,
          cost,
          "r1",
          config
        )
      ).toEqual({
        acquired: false,
        currentConcurrency: 0,
        error: "cost must be a finite number that is not negative",
      });
    }
  );

  it.each(unusable)(
    "is refused by tryAdmitConcurrency: %s",
    async (_, cost) => {
      expect(
        await backend.tryAdmitConcurrency(
          "mc:q",
          "mc:k",
          "mc:fz",
          cost,
          "r1",
          config
        )
      ).toBe(false);
    }
  );

  it("keeps the ceiling enforced against a queue of NaN costs", async () => {
    let admitted = 0;
    for (let i = 0; i < 11; i++) {
      const result = await backend.acquireConcurrency(
        "mc:nan",
        Number.NaN,
        `r${i}`,
        config
      );
      if (result.acquired) admitted++;
    }
    expect(admitted).toBe(0);
    expect(
      await backend.getConcurrencyState("mc:nan", config.requestTtl)
    ).toEqual({ currentConcurrency: 0, activeRequests: [] });
  });

  it("cannot be parked to inflate the capacity every later request sees", async () => {
    const key = "mc:inflate";
    expect(
      (await backend.acquireConcurrency(key, 1, "a", config)).acquired
    ).toBe(true);
    expect(
      (await backend.acquireConcurrency(key, 1, "b", config)).acquired
    ).toBe(true);

    expect(
      (await backend.acquireConcurrency(key, -5, "park", config)).acquired
    ).toBe(false);
    // The ceiling still holds afterwards: an admitted -5 read the occupancy back
    // as -3 and let every honest request through for as long as it was parked.
    expect(
      (await backend.acquireConcurrency(key, 1, "honest", config)).acquired
    ).toBe(false);
    expect(
      await backend.getConcurrencyState(key, config.requestTtl)
    ).toMatchObject({ currentConcurrency: 2 });
  });

  it.each([[0], [0.5]])("still admits a usable cost of %s", async (cost) => {
    expect(
      (await backend.acquireConcurrency("mc:ok", cost, "r1", config)).acquired
    ).toBe(true);
  });
});

describe("memory backend: an unusable numeric argument", () => {
  let backend: DianemoBackend;

  beforeEach(() => {
    backend = memoryBackend();
  });
  afterEach(async () => {
    await backend.close();
  });

  it.each([
    ["NaN", Number.NaN],
    ["Infinity", Number.POSITIVE_INFINITY],
    ["-Infinity", Number.NEGATIVE_INFINITY],
    ["zero", 0],
    ["a negative number", -1],
  ])("refuses a lock ttl of %s rather than taking it", async (_, ttlMs) => {
    await expect(
      backend.acquireLock("ml:lock", "token", ttlMs)
    ).rejects.toThrow(RangeError);
    expect(await backend.get("ml:lock")).toBeNull();
    // The refusal must leave the lock takeable, not held by the caller that
    // could never have released it.
    expect(await backend.acquireLock("ml:lock", "token", 5_000)).toBe(true);
  });

  it("refuses a NaN requestTtl rather than reporting a full client as idle", async () => {
    const key = "ml:conc";
    await backend.acquireConcurrency(key, 2, "r1", {
      maxConcurrency: 4,
      requestTtl: 60_000,
    });
    await expect(backend.getConcurrencyState(key, Number.NaN)).rejects.toThrow(
      RangeError
    );
    expect(await backend.getConcurrencyState(key, 60_000)).toMatchObject({
      currentConcurrency: 2,
    });
  });

  it.each([
    ["priority", { priority: Number.NaN }],
    ["priority", { priority: Number.POSITIVE_INFINITY }],
    ["retries", { retries: Number.NaN }],
    ["retries", { retries: Number.POSITIVE_INFINITY }],
  ])(
    "leaves %s alone when the update is not a finite number",
    async (_, update) => {
      const queueKey = "mu:q";
      const prefix = "mu:m";
      await backend.addRequest(
        queueKey,
        prefix,
        queued("r1", { priority: 5, retries: 3 })
      );
      await backend.updateRequest(queueKey, prefix, "r1", update);
      expect(await backend.getRequest(prefix, "r1")).toMatchObject({
        priority: 5,
        retries: 3,
      });
    }
  );

  it("keeps a request ahead of a lower-priority one after a NaN priority update", async () => {
    const queueKey = "mu:order";
    const prefix = "mu:orderm";
    const timestamp = Date.now();
    await backend.addRequest(
      queueKey,
      prefix,
      queued("urgent", { priority: 5, timestamp })
    );
    await backend.addRequest(
      queueKey,
      prefix,
      queued("ordinary", { priority: 1, timestamp })
    );
    await backend.updateRequest(queueKey, prefix, "urgent", {
      priority: Number.NaN,
    });
    expect(
      (await backend.getAllRequests(queueKey, prefix)).map((r) => r.requestId)
    ).toEqual(["urgent", "ordinary"]);
  });
});

describe("memory backend: a backend written to after close()", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  /**
   * The sweep has no public observation: every read expires the key it passes,
   * which is the lazy fallback the sweep exists to back up. So this reaches for
   * the store rather than inferring a periodic pass that never ran.
   */
  const storeOf = (backend: DianemoBackend) =>
    (backend as unknown as { store: Map<string, unknown> }).store;

  it("reclaims expired keys instead of holding them until a read", async () => {
    const backend = memoryBackend();
    await backend.close();

    await backend.set("mk:late", "v", 1);
    expect(storeOf(backend).size).toBe(1);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(storeOf(backend).size).toBe(0);

    await backend.close();
  });
});

// ------------------------------------------------------------ backend parity

describe.each(harnesses(0))(
  "freeze fields agree across backends — $name",
  (harness) => {
    let backend: DianemoBackend;
    let cleanup: () => Promise<void>;

    beforeEach(async () => {
      ({ backend, cleanup } = await harness.create());
    });
    afterEach(async () => {
      await cleanup();
    });

    it.each([["abc"], [""], ["NaN"], ["Infinity"]])(
      "reads a stored frozenUntil of %j as no freeze at all, probes owed or not",
      async (stored) => {
        const key = `mp:fz:${stored || "blank"}`;
        await backend.hset(key, {
          frozenUntil: stored,
          thawRequestCount: "2",
        });
        expect(await backend.getFreezeState(key)).toBeNull();
        expect(await backend.isFrozen(key)).toBe(false);
        expect(await backend.tryAdmitNoLimit("mp:fz:q", key)).toBe(true);
      }
    );

    it("truncates a fractional deadline and probe budget toward zero", async () => {
      const key = "mp:trunc";
      const now = await backend.now();
      const armed = await backend.armFreeze(key, now + 1000.7, 2.9);
      expect(armed).toEqual({
        frozenUntil: Math.trunc(now + 1000.7),
        thawRequestCount: 2,
      });
      expect(await backend.getFreezeState(key)).toEqual(armed);

      await backend.setFreezeState(key, now + 5000.9, 4.6);
      expect(await backend.getFreezeState(key)).toEqual({
        frozenUntil: Math.trunc(now + 5000.9),
        thawRequestCount: 4,
      });
    });

    it("counts a client-level probe whose metadata carries no grantId", async () => {
      const queueKey = "mp:thaw:q";
      const prefix = "mp:thaw:m";
      await backend.addRequest(queueKey, prefix, queued("r1"));
      // Rewritten without the field, which is the shape a metadata hash takes
      // when nothing ever set a grant on it. `del` leaves the queue entry.
      await backend.del(`${prefix}:r1`);
      await backend.hset(`${prefix}:r1`, {
        status: "inProgress",
        isThawRequest: "true",
      });

      expect(await backend.hasThawRequestInProgress(queueKey, prefix, "")).toBe(
        true
      );
    });

    it("clamps a deadline past 2^53-1 instead of half-writing the state", async () => {
      const key = "mp:huge";
      expect(await backend.armFreeze(key, 1e20, 1)).toEqual({
        frozenUntil: Number.MAX_SAFE_INTEGER,
        thawRequestCount: 1,
      });
      expect(await backend.getFreezeState(key)).toEqual({
        frozenUntil: Number.MAX_SAFE_INTEGER,
        thawRequestCount: 1,
      });
    });

    it("brings a stored deadline past the ceiling back down rather than keeping it", async () => {
      const key = "mp:poisoned";
      const now = await backend.now();
      await backend.hset(key, {
        frozenUntil: "1e300",
        thawRequestCount: "1",
      });
      // "Keep the larger" would carry the stored value forward for ever, so the
      // clamp has to run after the merge for the key to be recoverable at all.
      expect(await backend.armFreeze(key, now + 1000, 1)).toEqual({
        frozenUntil: Number.MAX_SAFE_INTEGER,
        thawRequestCount: 1,
      });
    });

    it("refuses a non-finite interval instead of spending a token against it", async () => {
      const key = "mp:interval";
      const config = { maxTokens: 10, tokensToAdd: 1, interval: Infinity };
      expect(await backend.acquireTokens(key, 1, config)).toEqual({
        acquired: false,
        waitTime: 0,
        remainingTokens: 0,
        error: "interval must be a finite number",
      });
      expect(
        await backend.tryAdmitImmediately(
          "mp:interval:q",
          key,
          "mp:interval:fz",
          1,
          config
        )
      ).toBe(false);
      // Refused before any write, so the bucket is untouched and a usable
      // config still spends from a full balance.
      expect(
        await backend.acquireTokens(key, 1, { ...config, interval: 1000 })
      ).toMatchObject({ acquired: true, remainingTokens: 9 });
    });
  }
);
