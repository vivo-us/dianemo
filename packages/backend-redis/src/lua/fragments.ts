/**
 * Lua blocks that more than one script needs to agree on, character for
 * character.
 *
 * Every fragment here reads locals the including script has already bound and
 * declares no ARGV of its own. That restriction is the point: `ARGV` slots are
 * positional, and a fragment that consumed one would renumber the slots of
 * whatever script included it — a mistake no behaviour test can catch. See the
 * warning `ttlArgvWarning` carries.
 *
 * Each is a function taking its own indentation, so a fragment can sit inside a
 * block without the include site having to know how it was written. Interpolate
 * at column zero: `${redisNow()}`, not `  ${redisNow()}`.
 */

const indent = (text: string, prefix: string): string =>
  text
    .split("\n")
    .map((line) => (line === "" ? "" : prefix + line))
    .join("\n");

/**
 * Redis's clock in epoch milliseconds, never the caller's.
 *
 * Every value these scripts compare against a stored timestamp was written by
 * whichever replica got there first. On a caller-supplied clock the replica
 * furthest ahead is the only one that ever measures elapsed time as positive:
 * for the token bucket that means one replica refills and the rest starve, and
 * for a concurrency slot or a freeze deadline it means a fast replica reaps
 * entries that are still live.
 *
 * Requires: nothing. Declares `t` and `now`.
 */
export const redisNow = (prefix = "  "): string =>
  indent(
    `local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)`,
    prefix
  );

/**
 * The two adjacent, differently-denominated TTL arguments the concurrency scripts
 * take, included above the `requestTtl` binding.
 *
 * `acquireConcurrency` and `tryAdmitConcurrency` carry it. `acquireQueuedConcurrency`
 * takes the same `ARGV[4]`-ms / `ARGV[5]`-seconds pair and does NOT — it never had
 * the warning, and the omission is worth closing rather than copying.
 */
export const ttlArgvWarning = (prefix = "  "): string =>
  indent(
    `-- ARGV SLOTS ARE POSITIONAL, AND THE TWO TTLs BELOW ARE ADJACENT AND
-- DIFFERENTLY DENOMINATED: ARGV[4] is requestTtl in MILLISECONDS, ARGV[5]
-- is the key's own TTL in SECONDS. Transposing them yields a script that
-- still parses, still admits, and still expires — just wrongly: a key set
-- to live 120000 SECONDS, and in-flight slots reaped after 86.4s instead
-- of the 120s their owner was promised. No behaviour test can fail on it,
-- because every request still succeeds. So if you add or remove an
-- argument here, renumber deliberately and check it directly: acquire one
-- slot and assert the key's TTL is ~86400, then plant a member scored older
-- than requestTtl and acquire again, asserting the stale member is gone —
-- which only holds while requestTtl is still milliseconds.`,
    prefix
  );

/**
 * Declares `finiteNumber(value, default)` — the Lua half of `parseStoredNumber`.
 *
 * `tonumber` alone is not enough for a value that then flows into `cjson.encode` or
 * `string.format`: it uses `strtod`, so a stored `'nan'` or `'inf'` parses to a
 * number that passes a `nil` test, survives `math.min`, and only fails later, where
 * cjson refuses to serialise it. Use this wherever a stored number reaches either.
 *
 * Requires: nothing. Declares `finiteNumber`.
 */
export const finiteNumberFn = (prefix = "  "): string =>
  indent(
    `local function finiteNumber(value, default)
  local n = tonumber(value)
  if n == nil or n ~= n or n == math.huge or n == -math.huge then return default end
  return n
end`,
    prefix
  );

/**
 * Declares `usableConcurrency(max)`, the one test for a ceiling that can admit.
 *
 * Each of the three concurrency scripts refuses differently — two encode an `error`
 * object, one returns `0` — so this declares a predicate rather than the gate itself,
 * and the caller supplies its own refusal.
 *
 * Requires: nothing. Declares `usableConcurrency`.
 */
export const usableConcurrencyFn = (prefix = "  "): string =>
  indent(
    `-- Lua's tonumber uses strtod, so both 'nan' and 'inf' parse to numbers. A nan
-- compares false against everything and inf is never exceeded, so an unguarded
-- 'currentCost + cost > maxConcurrency' admits without any ceiling at all.
local function usableConcurrency(max)
  return max ~= nil and max == max and max ~= math.huge and max > 0
end`,
    prefix
  );

/**
 * Declares `usableCost(cost)`, the one test for a cost a slot may be given.
 *
 * Zero and fractional costs are usable; a negative or non-finite one is not.
 * Unlike the token bucket's cost, this one is persisted — it goes into the
 * `:costs` hash and comes back as the occupancy every later admission on the key
 * is measured against — so it has to be refused before the write rather than at
 * the comparison.
 *
 * Requires: nothing. Declares `usableCost`.
 */
export const usableCostFn = (prefix = "  "): string =>
  indent(
    `local function usableCost(cost)
  return cost ~= nil and cost == cost and cost ~= math.huge and cost >= 0
end`,
    prefix
  );

/**
 * Refuses the no-queue fast path while anything is waiting.
 *
 * Requires: `queueKey`. Declares nothing. Refuses with `0`, so only a script
 * whose refusal value is `0` may include this.
 */
export const queueEmptyGate = (prefix = "  "): string =>
  indent(
    `-- Anything waiting means the queue is doing real ordering work; taking
-- the fast path here would let a later request overtake an earlier one.
if redis.call('ZCARD', queueKey) > 0 then return 0 end`,
    prefix
  );

/**
 * Refuses the no-queue fast path while the client is frozen or still owes thaw
 * probes. Both must block: once the freeze lapses with probes outstanding,
 * recovery is one request at a time and only the queue path can arrange that.
 *
 * An unusable `frozenUntil` is no freeze at all, and the probe budget beside it
 * is not consulted — the same reading `readFreezeState` and the memory twin
 * give it, so a client this gate blocks is one `getFreezeState` also reports as
 * frozen.
 *
 * Requires: `freezeKey`, `now` and `finiteNumber`. Declares `freeze` and
 * `frozenUntil`. Refuses with `0`, so only a script whose refusal value is `0`
 * may include this.
 */
export const freezeGate = (prefix = "  "): string =>
  indent(
    `local freeze = redis.call('HMGET', freezeKey, 'frozenUntil', 'thawRequestCount')
local frozenUntil = finiteNumber(freeze[1], nil)
if frozenUntil ~= nil then
  if now < frozenUntil then return 0 end
  if finiteNumber(freeze[2], 0) > 0 then return 0 end
end`,
    prefix
  );

/**
 * Credits the whole intervals elapsed since `lastUpdate` and clamps to the
 * ceiling. The two admission paths share one bucket, so they must compute a
 * balance identically or the fast path can spend a token the queue path has
 * already spent.
 *
 * `lastUpdate` advances in interval-sized steps rather than to `now`, which is
 * what leaves the remainder available to the next caller. The trailing clamp
 * runs even when no refill did: `maxTokens` can shrink under a stocked bucket
 * via `rateLimitChange`, and the old balance would otherwise stay spendable.
 *
 * Requires: `now`, `tokens`, `lastUpdate`, `interval`, `tokensToAdd`,
 * `maxTokens`. Mutates `tokens` and `lastUpdate`; declares `elapsed` and
 * `intervalsElapsed`.
 */
export const lazyRefill = (prefix = "  "): string =>
  indent(
    `local elapsed = now - lastUpdate
if elapsed > 0 and interval > 0 then
  local intervalsElapsed = math.floor(elapsed / interval)
  if intervalsElapsed > 0 then
    tokens = math.min(tokens + (intervalsElapsed * tokensToAdd), maxTokens)
    lastUpdate = lastUpdate + (intervalsElapsed * interval)
  end
end

if tokens > maxTokens then tokens = maxTokens end`,
    prefix
  );

/**
 * Reaps slots whose request never reported completion, then sums what the
 * survivors hold.
 *
 * Expired members are read before they are removed because the cost entries are
 * keyed by member and would otherwise leak. `requestId` is excluded from the sum
 * so a re-submission of the same request is not counted against itself.
 *
 * Requires: `key`, `now`, `requestTtl`, `requestId` and `finiteNumber`. Declares
 * `expireTime`, `costKey`, `expiredMembers`, `members`, `currentCost` and the
 * loop's `memberCost` — check for a name already in scope before adding an
 * include site, since `getConcurrencyState` declares its own `expireTime`.
 */
export const slotAccounting = (prefix = "  "): string =>
  indent(
    `local expireTime = now - requestTtl
local costKey = key .. ':costs'
local expiredMembers = redis.call('ZRANGEBYSCORE', key, '-inf', expireTime)
if #expiredMembers > 0 then
  redis.call('ZREMRANGEBYSCORE', key, '-inf', expireTime)
  for _, member in ipairs(expiredMembers) do
    redis.call('HDEL', costKey, member)
  end
end

local members = redis.call('ZRANGE', key, 0, -1)
local currentCost = 0
for i, member in ipairs(members) do
  if member ~= requestId then
    local memberCost = finiteNumber(redis.call('HGET', costKey, member), 1)
    currentCost = currentCost + memberCost
  end
end`,
    prefix
  );
