/** Shared queue-score layout used by every backend. */
export const MAX_QUEUE_PRIORITY = 10;
export const MAX_QUEUE_RETRIES = 100;
/**
 * Version-1 on-disk layout. Changing this value reinterprets every persisted
 * Redis score, so it must remain stable until a versioned queue migration
 * exists. The range guard below fails loudly before arrival can bleed into the
 * retry band.
 */
export const QUEUE_RETRY_BAND = 1e12;
export const QUEUE_PRIORITY_BAND = QUEUE_RETRY_BAND * (MAX_QUEUE_RETRIES + 1);
export const QUEUE_SCORE_EPOCH_MS = 1767225600000;

export function calculateQueueScore(
  priority: number,
  retries: number,
  timestamp: number
): number {
  const arrival = timestamp - QUEUE_SCORE_EPOCH_MS;
  if (arrival >= QUEUE_RETRY_BAND) {
    throw new RangeError(
      `Queue timestamp ${timestamp} falls outside the version-1 score epoch`
    );
  }
  const p = Math.min(Math.max(priority, 0), MAX_QUEUE_PRIORITY);
  const r = Math.min(Math.max(retries, 0), MAX_QUEUE_RETRIES);
  const score =
    (MAX_QUEUE_PRIORITY - p) * QUEUE_PRIORITY_BAND +
    (MAX_QUEUE_RETRIES - r) * QUEUE_RETRY_BAND +
    arrival;
  if (!Number.isSafeInteger(score)) {
    throw new RangeError(
      `Queue timestamp ${timestamp} cannot be represented without losing ordering precision`
    );
  }
  return score;
}
