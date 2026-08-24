import { describe, expect, it } from "vitest";
import {
  calculateQueueScore as score,
  MAX_QUEUE_PRIORITY as MAX_PRIORITY,
  MAX_QUEUE_RETRIES as MAX_RETRIES,
  QUEUE_SCORE_EPOCH_MS as SCORE_EPOCH_MS,
} from "../packages/core/src/index.js";

// A plausible "now" well after the score epoch.
const NOW = SCORE_EPOCH_MS + 200 * 24 * 60 * 60 * 1000;

describe("queue score packing", () => {
  it("stays inside the range where doubles represent every integer", () => {
    // Past 2^53 consecutive integers stop being representable and
    // millisecond ordering silently collapses into ties.
    const worst = score(0, 0, NOW);
    expect(worst).toBeLessThan(Number.MAX_SAFE_INTEGER);
    expect(Number.isSafeInteger(worst)).toBe(true);
  });

  it("orders by priority before anything else", () => {
    // Higher priority wins even against an older, more-retried request.
    expect(score(5, 0, NOW)).toBeLessThan(score(1, 50, NOW - 60_000));
  });

  it("orders by retries within one priority", () => {
    expect(score(1, 5, NOW)).toBeLessThan(score(1, 0, NOW));
  });

  it("orders by arrival within one priority and retry count", () => {
    expect(score(1, 0, NOW - 1)).toBeLessThan(score(1, 0, NOW));
  });

  it("keeps millisecond arrival ordering distinct at every priority", () => {
    // The original packing pushed priority 1 past 2^53, where adjacent
    // milliseconds rounded to the same score and FIFO degraded to ties.
    for (const priority of [0, 1, 5, 10]) {
      const a = score(priority, 0, NOW);
      const b = score(priority, 0, NOW + 1);
      expect(b - a).toBe(1);
    }
  });

  it("never lets the arrival term bleed into the retry band", () => {
    // One extra retry must outrank any arrival-time difference.
    const oldest = score(1, 0, SCORE_EPOCH_MS);
    const newestWithOneRetry = score(1, 1, NOW);
    expect(newestWithOneRetry).toBeLessThan(oldest);
  });

  it("fails loudly at the version-1 horizon instead of corrupting ordering", () => {
    const afterOldBoundary = SCORE_EPOCH_MS + 1.1e12;
    expect(() => score(1, 1, afterOldBoundary)).toThrow(/version-1/);
  });

  it("never lets the retry band bleed into the priority band", () => {
    const maxRetriesLowPriority = score(1, MAX_RETRIES, NOW);
    const noRetriesHigherPriority = score(2, 0, NOW);
    expect(noRetriesHigherPriority).toBeLessThan(maxRetriesLowPriority);
  });

  it("clamps out-of-range priority instead of inverting the ordering", () => {
    // An unclamped priority above the maximum flips the sign of its term,
    // which would sort the most urgent work to the back of the queue.
    expect(score(99, 0, NOW)).toBe(score(MAX_PRIORITY, 0, NOW));
    expect(score(-5, 0, NOW)).toBe(score(0, 0, NOW));
    expect(score(MAX_PRIORITY, 0, NOW)).toBeLessThan(score(0, 0, NOW));
  });

  it("clamps retries beyond the band", () => {
    expect(score(1, 999, NOW)).toBe(score(1, MAX_RETRIES, NOW));
  });
});
