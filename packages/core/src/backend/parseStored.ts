/**
 * Shared reads of numeric fields stored as strings, used by every backend.
 *
 * The Lua in the Redis backend is the reference implementation and this matches
 * it: docs/design-notes.md#numeric-field-parsing enumerates every place the rule
 * is written down and must agree.
 */

/**
 * Matches Lua's `tonumber` — absent, empty and unparseable all mean the default —
 * because `Number("")` is 0 and the two backends must read one stored field the same
 * way. Mirrored in three other places: docs/design-notes.md#numeric-field-parsing.
 */
export function parseStoredNumber(
  raw: string | undefined,
  fallback: number
): number {
  if (raw === undefined || raw.trim() === "") return fallback;
  const parsed = Number(raw);
  // NaN, not "not finite". `tonumber("Infinity")` is `inf` — a number Lua then
  // clamps into the top band exactly as `Math.min` does here — so rejecting it as
  // unparseable would make the two backends order the same stored field
  // differently, which is the disagreement this helper exists to remove.
  return Number.isNaN(parsed) ? fallback : parsed;
}

/**
 * Finite on top of the shared parse — the one place a read and a score differ. A
 * score keeps `inf` so both backends agree; a read is serialised, and
 * `JSON.stringify(Infinity)` is `null`, which would arrive as a null priority.
 */
export function parsePriority(raw: string | undefined): number {
  const parsed = parseStoredNumber(raw, 1);
  return Number.isFinite(parsed) ? parsed : 1;
}

/**
 * The shared parse, with a non-finite value taken as the default too.
 *
 * For a field that is compared and then written back — a freeze deadline, a probe
 * budget — where an `inf` survives every comparison and only fails at the point of
 * serialisation. Twin of `finiteNumber` in the Redis backend's Lua fragments.
 */
export function parseFiniteStored(
  raw: string | undefined,
  fallback: number
): number {
  const parsed = parseStoredNumber(raw, fallback);
  return Number.isFinite(parsed) ? parsed : fallback;
}
