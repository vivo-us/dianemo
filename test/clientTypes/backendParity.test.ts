import { describe, expect, it } from "vitest";
import { harnesses } from "./harness.js";
import type {
  DianemoBackend,
  QueuedRequest,
} from "../../packages/core/src/backend/types.js";

/**
 * Selection parity between the two backends.
 *
 * Every other suite asserts BEHAVIOUR through a client, which means each
 * backend is only ever exercised by the paths a client happens to take. The
 * memory implementation and the Lua are two independent pieces of selection
 * logic, and nothing else here holds them to the same answer — so a divergence
 * shows up as one backend's half of a `describe.each` failing on a timing
 * assertion, at a call site far from the disagreement.
 *
 * `skipRequestIds` is the argument most worth pinning: the admission loop passes
 * it to step past a head it has already judged unsatisfiable, so a backend that
 * ignores it hands back the same entry every iteration and the drain makes no
 * progress. Its edges — an empty set, an unknown id, a duplicated id, every id
 * skipped — are exactly the shapes a Lua table lookup and a JavaScript filter
 * are most likely to disagree on.
 *
 * Deliberately a single table compared in one assertion rather than a case per
 * `it`: the useful failure output is the whole matrix with the offending row
 * visible, not the first row that stopped matching.
 *
 * The types above are imported RELATIVELY, like every other suite here, and not
 * as `@dianemo/core`. `node_modules/@dianemo/core` is a workspace symlink to
 * `packages/core`, so a verification mirror that symlinks `node_modules`
 * wholesale resolves the package name to the ORIGINAL tree rather than to the
 * mirror's own sources. This was the only test importing by package name, and it
 * therefore typechecked against the wrong copy — which hides precisely the
 * interface mismatch a mirror exists to catch.
 */

const QUEUE = "parity:queue";
const PREFIX = "parity:request";

function entry(
  requestId: string,
  priority: number,
  grantId?: string
): QueuedRequest {
  return {
    requestId,
    clientName: "c",
    requestName: `t.${requestId}`,
    status: "pending",
    priority,
    cost: 1,
    retries: 0,
    // Ordered by priority alone: the score packs priority above timestamp, so
    // holding the timestamps in the same order keeps the two from being
    // confusable when a row does disagree.
    timestamp: 1_000 + priority,
    isThawRequest: false,
    ownerId: "owner",
    ...(grantId ? { grantId } : {}),
  };
}

// db2 rather than a higher index: 4-15 are all taken, and db14 in particular is
// `replicas.smoke`'s. `fileParallelism: false` makes a shared index survivable —
// four suites already double up — but a suite that needs nothing more than a
// scratch keyspace has no reason to add a fifth collision.
describe.each(harnesses(2))("backend selection parity — $name", (harness) => {
  const withBackend = async (
    fn: (backend: DianemoBackend) => Promise<void>
  ): Promise<void> => {
    const { backend, cleanup } = await harness.create();
    try {
      await fn(backend);
    } finally {
      await cleanup();
    }
  };

  /** Priority descending: `hi` is the head, `mid` carries the only grant. */
  const seed = async (backend: DianemoBackend) => {
    await backend.addRequest(QUEUE, PREFIX, entry("hi", 10));
    await backend.addRequest(QUEUE, PREFIX, entry("mid", 5, "g1"));
    await backend.addRequest(QUEUE, PREFIX, entry("lo", 1));
  };

  /** Selection claims the entry it returns, so each probe starts from pending. */
  const reset = async (backend: DianemoBackend) => {
    for (const id of ["hi", "mid", "lo"]) {
      await backend.updateRequest(QUEUE, PREFIX, id, { status: "pending" });
    }
  };

  it("selects the same request for every skip-set shape", async () => {
    await withBackend(async (backend) => {
      await seed(backend);
      const observed: Record<string, string | null> = {};

      const pick = async (
        label: string,
        skipGrantIds?: string[],
        skipRequestIds?: string[]
      ) => {
        await reset(backend);
        const next = await backend.getNextRequest(
          QUEUE,
          PREFIX,
          skipGrantIds,
          skipRequestIds
        );
        observed[label] = next?.requestId ?? null;
      };

      await pick("noSkips");
      await pick("undefinedSkips", undefined, undefined);
      await pick("emptySkips", [], []);
      await pick("skipHead", [], ["hi"]);
      await pick("skipTwo", [], ["hi", "mid"]);
      await pick("skipAll", [], ["hi", "mid", "lo"]);
      await pick("skipUnknown", [], ["nope"]);
      await pick("skipDuplicated", [], ["hi", "hi", "hi"]);
      await pick("skipHeadAndGrant", ["g1"], ["hi"]);
      await pick("grantOnly", ["g1"], []);

      expect(observed).toEqual({
        noSkips: "hi",
        undefinedSkips: "hi",
        emptySkips: "hi",
        skipHead: "mid",
        skipTwo: "lo",
        // Nothing selectable is null, not the head anyway: a backend that fell
        // back to the head here would defeat the skip entirely.
        skipAll: null,
        skipUnknown: "hi",
        skipDuplicated: "mid",
        skipHeadAndGrant: "lo",
        grantOnly: "hi",
      });
    });
  });

  /**
   * The score that decides queue order exists three times: `queueScore` in the
   * memory backend, `calculateQueueScore` on the Redis add path, and the Lua that
   * re-scores in place when `updateRequest` changes priority or retries. Its
   * properties — priority above retries above arrival, out-of-range values
   * clamped rather than allowed to invert the ordering, and no band bleeding into
   * the one above it — are asserted in `queueOrdering.test.ts` against a fourth
   * copy of the formula declared in that file, so all three real implementations
   * could drop their clamps with that suite green. Nothing else feeds an
   * out-of-range value to a real backend.
   *
   * Defence in depth against drift in a triplicated formula, not a live bug: all
   * three agree today.
   */
  const graded = (
    requestId: string,
    priority: number,
    retries: number,
    timestamp: number
  ): QueuedRequest => ({
    requestId,
    clientName: "c",
    requestName: `t.${requestId}`,
    status: "pending",
    priority,
    cost: 1,
    retries,
    timestamp,
    isThawRequest: false,
    ownerId: "owner",
  });

  // Well clear of the score epoch, and far enough apart to prove the arrival
  // term orders without reaching the retry band above it: ten years of
  // milliseconds is 3.15e11 against a 1e12 band.
  const T = 2_000_000_000_000;
  const FAR = T + 315_360_000_000;

  /** Everything the queue holds, in the order the backend hands it out. */
  const drain = async (backend: DianemoBackend): Promise<string[]> => {
    const order: string[] = [];
    for (;;) {
      const next = await backend.getNextRequest(QUEUE, PREFIX);
      if (!next) return order;
      order.push(next.requestId);
    }
  };

  const gridSeed = async (backend: DianemoBackend) => {
    // Ids are chosen so that entries which tie on score are also predictable
    // lexically, which is how both backends break a tie.
    await backend.addRequest(QUEUE, PREFIX, graded("p11", 11, 0, T));
    await backend.addRequest(QUEUE, PREFIX, graded("p10", 10, 0, T));
    await backend.addRequest(QUEUE, PREFIX, graded("p10r2", 10, 2, T));
    await backend.addRequest(QUEUE, PREFIX, graded("p5", 5, 0, T));
    await backend.addRequest(QUEUE, PREFIX, graded("p5r150", 5, 150, T));
    await backend.addRequest(QUEUE, PREFIX, graded("tieA", 5, 0, T));
    await backend.addRequest(QUEUE, PREFIX, graded("far", 5, 0, FAR));
    await backend.addRequest(QUEUE, PREFIX, graded("p1", 1, 0, T));
    await backend.addRequest(QUEUE, PREFIX, graded("p0", 0, 0, T));
    await backend.addRequest(QUEUE, PREFIX, graded("pNeg", -1, 0, T));
  };

  it("packs the score the same way in both backends", async () => {
    await withBackend(async (backend) => {
      await gridSeed(backend);

      expect(await drain(backend)).toEqual([
        // Priority band 10. `p11` clamps INTO it rather than above it, and more
        // retries come first inside a band, so the retried entry leads.
        "p10r2",
        "p10",
        "p11",
        // Priority 5. 150 retries clamps to the band maximum, and the two
        // entries sharing a millisecond fall back to their ids.
        "p5r150",
        "p5",
        "tieA",
        // Ten years later, still inside its own band: arrival orders last of the
        // three, and has not leaked into the retry band above.
        "far",
        "p1",
        // Priority 0 is the documented bottom of the range and must not be
        // promoted; -1 clamps onto it rather than below it.
        "p0",
        "pNeg",
      ]);
    });
  });

  it("re-scores in place onto the same scale", async () => {
    await withBackend(async (backend) => {
      await gridSeed(backend);
      // The entry furthest back in its band, given the most retries. It has to
      // land beside the other retried entry rather than on a different scale —
      // the Lua used its own constants once, which put every re-scored request
      // behind the whole queue, the exact opposite of retrying sooner.
      await backend.updateRequest(QUEUE, PREFIX, "far", { retries: 100 });

      expect(await drain(backend)).toEqual([
        "p10r2",
        "p10",
        "p11",
        // Both now at the retry maximum, so arrival decides between them.
        "p5r150",
        "far",
        "p5",
        "tieA",
        "p1",
        "p0",
        "pNeg",
      ]);
    });
  });

  it("clamps an out-of-range re-score instead of inverting the order", async () => {
    await withBackend(async (backend) => {
      await gridSeed(backend);
      // The clamps on the ADD path are JavaScript in both backends; the ones that
      // matter here are on the re-score path, where the Redis half is Lua. An
      // unclamped 11 would score BELOW the top band and take the head of the
      // queue outright, and an unclamped -5 would score above the bottom one.
      await backend.updateRequest(QUEUE, PREFIX, "p1", { priority: 11 });
      await backend.updateRequest(QUEUE, PREFIX, "p10", { priority: -5 });

      expect(await drain(backend)).toEqual([
        // Band 10, retried first, then the two ties by id.
        "p10r2",
        "p1",
        "p11",
        "p5r150",
        "p5",
        "tieA",
        "far",
        // Band 0 now holds three: the two that were always there and the one
        // re-scored onto it.
        "p0",
        "p10",
        "pNeg",
      ]);
    });
  });

  /**
   * A corrupt stored field means the same thing to both backends.
   *
   * The field is read at four sites — each backend's `toQueued` and each
   * backend's re-score — and before the parse was shared they disagreed three
   * ways. `Number("")` is 0, so an empty priority landed in the BOTTOM band in
   * TypeScript and read as missing in the Lua; `Number("abc")` is NaN, which the
   * memory re-score handed straight to a score, where the comparator falls
   * through to ordering by id and the priority is silently ignored.
   *
   * Absent or unparseable — empty and whitespace included — now means the field
   * default on every path: priority 1, never NaN and never 0, because 0 is a real
   * band and mapping corruption onto it is a silent demotion.
   *
   * Reachable the same way the controller-side cost check is: the queue is shared
   * state, so a peer on older code or any stray write is the source.
   */
  it("reads a corrupt stored field as missing rather than as zero or NaN", async () => {
    await withBackend(async (backend) => {
      await gridSeed(backend);
      await backend.hset(`${PREFIX}:p5`, { priority: "abc" });
      await backend.hset(`${PREFIX}:tieA`, { priority: "" });
      // Re-scoring is what re-reads the field, and setting retries to what it
      // already is forces one without changing anything else.
      await backend.updateRequest(QUEUE, PREFIX, "p5", { retries: 0 });
      await backend.updateRequest(QUEUE, PREFIX, "tieA", { retries: 0 });

      // Both fall to the default priority, so both join `p1` in band 1.
      expect(await drain(backend)).toEqual([
        "p10r2",
        "p10",
        "p11",
        "p5r150",
        "far",
        "p1",
        "p5",
        "tieA",
        "p0",
        "pNeg",
      ]);

      // And the read path agrees with the score path about what it meant.
      expect((await backend.getRequest(PREFIX, "p5"))?.priority).toBe(1);
      expect((await backend.getRequest(PREFIX, "tieA"))?.priority).toBe(1);
    });
  });

  /**
   * `"Infinity"` is the one stored value where the score parse and the read parse
   * must disagree, and it is the row that distinguishes them.
   *
   * The SCORE parse has to accept whatever Lua's `tonumber` accepts, and
   * `tonumber("Infinity")` is `inf` — which both backends then clamp into the top
   * band, `Math.min` on one side and an `> MAX_PRIORITY` test on the other. So
   * rejecting it as unparseable in TypeScript puts the entry in band 1 on memory
   * and band 10 on Redis: the same stored field, two orderings.
   *
   * The READ parse has to reject it, because a priority read back out is published
   * as JSON and `JSON.stringify(Infinity)` is `null` — so an accepted infinity
   * reaches the owning replica as a null in a field typed `number`.
   *
   * The cases above cannot tell the two rules apart: `""` and `"abc"` yield the
   * default through either. That is why a fix which corrected the helpers but left
   * one call site reading through the wrong one passed the whole suite — this row
   * is the guard for that.
   */
  it("scores a stored infinity like the Lua does, and never reads one back", async () => {
    await withBackend(async (backend) => {
      await gridSeed(backend);
      await backend.hset(`${PREFIX}:p5`, { priority: "Infinity" });
      await backend.updateRequest(QUEUE, PREFIX, "p5", { retries: 0 });

      // Clamped into the top band, joining the legitimate priority-10 entries and
      // sorting last among them: same retry count and same arrival, so the tie
      // breaks lexically and "p5" follows "p10" and "p11".
      expect(await drain(backend)).toEqual([
        "p10r2",
        "p10",
        "p11",
        "p5",
        "p5r150",
        "tieA",
        "far",
        "p1",
        "p0",
        "pNeg",
      ]);

      // Never handed back as `Infinity`: the value is about to be serialised.
      expect((await backend.getRequest(PREFIX, "p5"))?.priority).toBe(1);
    });
  });

  it("leaves a skipped entry unclaimed", async () => {
    await withBackend(async (backend) => {
      await seed(backend);
      await reset(backend);

      await backend.getNextRequest(QUEUE, PREFIX, [], ["hi"]);

      // The skipped head must be passed over, not consumed: the loop skips it
      // for this pass only, and a backend that marked it `inProgress` on the way
      // past would strand it behind a claim nothing will ever release.
      const skipped = await backend.getRequest(PREFIX, "hi");
      const claimed = await backend.getRequest(PREFIX, "mid");
      expect(skipped?.status).toBe("pending");
      expect(claimed?.status).toBe("inProgress");
    });
  });
});
