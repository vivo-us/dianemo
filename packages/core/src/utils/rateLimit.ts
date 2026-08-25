import { ConfigurationError } from "../errors.js";
import type {
  ConcurrencyLimitClientOptions,
  NamedRateLimitData,
  RateLimitConfig,
  RateLimitData,
  RequestLimitClientOptions,
} from "../client/types.js";

/**
 * Shape a multi-limit entry's `name` must take.
 *
 * The name becomes a backend key segment — `<namespace>:rateLimit:<name>` — so it
 * is held to the narrowest thing that never needs quoting or normalising: a colon
 * splits the segment the way it splits a client name, whitespace is normalised to
 * `_` when namespaces are derived, and two entries differing only by case would be
 * two names for what an operator reads as one bucket. `assertNameSegment` is the
 * client-name twin of this rule.
 */
const RATE_LIMIT_NAME = /^[a-z0-9_]{1,64}$/;

/**
 * The name a client's limit takes when it declares one without naming it.
 *
 * Every limit is named internally, so there is one key shape and one config
 * shape rather than two of each; a caller writing the single form is simply
 * given this. It reads as a name because it becomes one, in the key and in
 * `getClientStats`.
 */
export const DEFAULT_RATE_LIMIT_NAME = "default";

/**
 * A declared config as the array everything downstream works in.
 *
 * Called once, where the client is built, so nothing past that point has to ask
 * which of the two shapes it was handed. Validate before normalising: the single
 * form is exempt from the name rule precisely because it has no name to check.
 */
export function normalizeRateLimit(
  config: RateLimitConfig
): NamedRateLimitData[] {
  if (isMultiRateLimit(config)) return config;
  return [{ ...config, name: DEFAULT_RATE_LIMIT_NAME }];
}

/**
 * Whether a config is nothing but a `sharedLimit` — one entry, or the bare
 * single form.
 *
 * Such a client owns no budget and no queue: it is constructed with the owner's
 * name, so the owner's controller drains one queue and arbitrates between every
 * client on that budget. That is why a `sharedLimit` may not be combined with
 * other limits — see `assertUsableRateLimits` for what combining would cost.
 */
export function isSharedLimitOnly(config: RateLimitConfig): boolean {
  const entries = rateLimitEntries(config);
  return entries.length === 1 && entries[0]?.type === "sharedLimit";
}

/** The lone limit of a config that declares exactly one, else `undefined`. */
export function soleRateLimit(
  config: readonly NamedRateLimitData[]
): NamedRateLimitData | undefined {
  return config.length === 1 ? config[0] : undefined;
}

/** Whether anything here actually meters a request. */
export function hasMeteredBudget(
  config: readonly NamedRateLimitData[]
): boolean {
  return config.some(
    (limit) =>
      limit.type === "requestLimit" || limit.type === "concurrencyLimit"
  );
}

/** Whether a client declared several limits rather than one. */
export function isMultiRateLimit(
  config: RateLimitConfig
): config is NamedRateLimitData[] {
  return Array.isArray(config);
}

/** Every limit a config declares, as a list. A single limit has no `name`. */
export function rateLimitEntries(
  config: RateLimitConfig
): readonly (RateLimitData & { name?: string })[] {
  return isMultiRateLimit(config) ? config : [config];
}

export function assertRateLimitName(name: unknown, clientName: string): void {
  if (typeof name === "string" && RATE_LIMIT_NAME.test(name)) return;
  throw new ConfigurationError(
    "invalid_rate_limit_name",
    `Client "${clientName}" declares a rate limit whose name is ${JSON.stringify(
      name ?? null
    )}. Every entry of a multi-limit array needs a name matching ${String(
      RATE_LIMIT_NAME
    )} — it is the key its bucket is stored under, so it must be stable and unambiguous. Try "per_second" or "per_day".`
  );
}

/**
 * Rejects a rate-limit config that cannot admit anything, before a client is
 * built from it.
 *
 * Single limits are validated exactly as their own client class validates them,
 * so routing an array through here cannot be more permissive than declaring the
 * same limits one at a time.
 */
export function assertUsableRateLimits(
  config: RateLimitConfig,
  clientName: string
): void {
  if (!isMultiRateLimit(config)) {
    assertUsableRateLimit(config, clientName);
    return;
  }
  if (config.length === 0) {
    throw new ConfigurationError(
      "invalid_rate_limit",
      `Client "${clientName}" declares an empty rate-limit array. Omit the property for the noLimit default, or name at least one limit.`
    );
  }
  const shared = config.filter((limit) => limit?.type === "sharedLimit");
  // The two-shared case is tested first: it also satisfies the condition below,
  // whose message would then report "0 other rate limits" and read as nonsense.
  if (shared.length > 1) {
    throw new ConfigurationError(
      "shared_limit_not_alone",
      `Client "${clientName}" declares ${shared.length} sharedLimit entries. A client can draw on one shared budget at most, since it takes that owner's queue as its own.`
    );
  }
  if (shared.length > 0 && config.length > 1) {
    throw new ConfigurationError(
      "shared_limit_not_alone",
      `Client "${clientName}" declares a sharedLimit alongside ${
        config.length - shared.length
      } other rate limit(s). A sharedLimit must be the only limit a client declares: it has no queue of its own — the owner's controller drains one queue for everything on that budget — so a client that also had budgets of its own would need a second queue, and nothing would arbitrate ordering between the two. Give this client its own limits, or share the owner's; not both.`
    );
  }

  const seen = new Set<string>();
  for (const limit of config) {
    assertRateLimitName(limit?.name, clientName);
    if (seen.has(limit.name)) {
      throw new ConfigurationError(
        "invalid_rate_limit",
        `Client "${clientName}" declares two rate limits named "${limit.name}". Names key the buckets, so duplicates would meter both limits against one balance.`
      );
    }
    seen.add(limit.name);
    assertUsableRateLimit(limit, clientName, limit.name);
  }
}

function assertUsableRateLimit(
  rateLimit: RateLimitData,
  clientName: string,
  entryName?: string
): void {
  switch (rateLimit?.type) {
    case "requestLimit":
      assertUsableBudget(rateLimit, clientName, entryName);
      return;
    case "concurrencyLimit":
      assertUsableConcurrency(rateLimit, clientName, entryName);
      return;
    case "sharedLimit":
      if (typeof rateLimit.clientName === "string" && rateLimit.clientName) {
        return;
      }
      throw new ConfigurationError(
        "invalid_rate_limit",
        `Client "${clientName}"${describeEntry(entryName)} declares a sharedLimit with no clientName. Name the client that owns the budget.`
      );
    case "noLimit":
      return;
    default:
      throw new ConfigurationError(
        "unknown_rate_limit_type",
        `Client "${clientName}"${describeEntry(entryName)} has an unrecognised rateLimit.type ${JSON.stringify(
          (rateLimit as { type?: unknown } | undefined)?.type ?? null
        )}. Expected one of: requestLimit, concurrencyLimit, sharedLimit, noLimit.`
      );
  }
}

/**
 * Rejects a token-bucket budget that can never hand out a token.
 *
 * `tokensToAdd <= 0` is the dangerous one: the bucket drains, nothing refills
 * it, and every backend reports "not acquired, wait 0ms" forever. A config typo
 * or a `rateLimitChange` that parses a header into 0 or NaN both land here, so
 * this is the only thing standing between either and the acquire loop.
 */
export function assertUsableBudget(
  rateLimit: RequestLimitClientOptions,
  clientName: string,
  entryName?: string
): void {
  const problems: string[] = [];
  if (!Number.isFinite(rateLimit.tokensToAdd) || rateLimit.tokensToAdd <= 0) {
    problems.push(`tokensToAdd must be greater than 0`);
  }
  if (!Number.isFinite(rateLimit.interval) || rateLimit.interval <= 0) {
    problems.push(`interval must be greater than 0`);
  }
  if (!Number.isFinite(rateLimit.maxTokens) || rateLimit.maxTokens <= 0) {
    problems.push(`maxTokens must be greater than 0`);
  }
  if (!problems.length) return;
  throw new ConfigurationError(
    "invalid_rate_limit",
    `Client "${clientName}"${describeEntry(entryName)} has an unusable requestLimit: ${problems.join(
      "; "
    )}. Received ${JSON.stringify({
      maxTokens: rateLimit.maxTokens,
      tokensToAdd: rateLimit.tokensToAdd,
      interval: rateLimit.interval,
    })}.`
  );
}

/**
 * Rejects a concurrency ceiling that cannot admit anything.
 *
 * `NaN` is the dangerous value: `used + cost <= NaN` is false in JavaScript,
 * which removes the cap, and nil-ish in Lua, which refuses everything. The usual
 * source is a `rateLimitChange` reading a header that was absent once.
 */
export function assertUsableConcurrency(
  rateLimit: ConcurrencyLimitClientOptions,
  clientName: string,
  entryName?: string
): void {
  const { maxConcurrency } = rateLimit;
  if (Number.isFinite(maxConcurrency) && maxConcurrency > 0) return;
  throw new ConfigurationError(
    "invalid_rate_limit",
    `Client "${clientName}"${describeEntry(entryName)} has an unusable concurrencyLimit: maxConcurrency must be a finite number greater than 0. Received ${JSON.stringify(
      maxConcurrency
    )}.`
  );
}

function describeEntry(entryName?: string): string {
  return entryName === undefined ? "" : ` rate limit "${entryName}"`;
}

/**
 * The largest `cost` a config could ever admit, or undefined when nothing in it
 * imposes a ceiling. Several limits mean the tightest one decides.
 *
 * `resolveShared` answers for a `sharedLimit` entry, whose ceiling belongs to the
 * client that owns the budget; it returns undefined when that client is gone,
 * which is reported by the queue path rather than here.
 */
export function costCeilingFor(
  config: RateLimitConfig,
  resolveShared?: (clientName: string) => RateLimitConfig | undefined
): number | undefined {
  let ceiling: number | undefined;
  const consider = (value: number | undefined) => {
    if (value === undefined) return;
    ceiling = ceiling === undefined ? value : Math.min(ceiling, value);
  };
  for (const limit of rateLimitEntries(config)) {
    if (limit.type === "requestLimit") consider(limit.maxTokens);
    else if (limit.type === "concurrencyLimit") consider(limit.maxConcurrency);
    else if (limit.type === "sharedLimit") {
      const owner = resolveShared?.(limit.clientName);
      // Not recursed through another `sharedLimit`: registration refuses a
      // budget owner that is itself shared, so one hop always reaches the owner.
      if (owner) consider(costCeilingFor(owner));
    }
  }
  return ceiling;
}

/**
 * The shortest refill interval in a config, which is how soon an emptied budget
 * can hand out anything at all.
 *
 * The shortest, not the longest: a client holding a per-second and a per-day
 * limit would otherwise floor every freeze at a day, and a single 429 from the
 * per-second limit would stand the fleet down until tomorrow.
 */
export function shortestRefillInterval(
  config: RateLimitConfig
): number | undefined {
  let shortest: number | undefined;
  for (const limit of rateLimitEntries(config)) {
    if (limit.type !== "requestLimit") continue;
    if (!Number.isFinite(limit.interval) || limit.interval <= 0) continue;
    shortest =
      shortest === undefined
        ? limit.interval
        : Math.min(shortest, limit.interval);
  }
  return shortest;
}
