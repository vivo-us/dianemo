import {
  finiteNumberFn,
  freezeGate,
  lazyRefill,
  queueEmptyGate,
  redisNow,
} from "./fragments.js";

/**
 * Token-bucket Lua. Refill is lazy — a request credits the whole intervals
 * elapsed since `lastUpdate` rather than a timer adding tokens — which is why
 * `lastUpdate` advances in interval-sized steps and not to `now`.
 *
 * Twin of the memory backend's token-bucket section.
 */

export const tokenBucket = `
  local key = KEYS[1]
  local cost = tonumber(ARGV[1])
  local maxTokens = tonumber(ARGV[2])
  local tokensToAdd = tonumber(ARGV[3])
  local interval = tonumber(ARGV[4])
  -- ARGV is positional: this moved from [6] when the dead caller clock
  -- at [5] was removed. See acquireConcurrency in concurrency.ts for the trap.
  local ttl = tonumber(ARGV[5]) or 86400

${redisNow()}

${finiteNumberFn()}
  -- 'not (x > 0)', not 'x <= 0': a nan compares false either way round, so the
  -- second form admits it and the division below yields a nan balance.
  if not (tokensToAdd > 0) then
    return cjson.encode({ acquired = false, waitTime = 0, remainingTokens = 0, error = 'tokensToAdd must be greater than 0' })
  end
  -- Reported through 'error' rather than raised: the contract says an unusable
  -- configuration is distinguishable from a merely empty bucket, and a non-finite
  -- maxTokens or cost would otherwise reach cjson.encode and throw.
  if not (maxTokens > 0) or maxTokens == math.huge then
    return cjson.encode({ acquired = false, waitTime = 0, remainingTokens = 0, error = 'maxTokens must be a finite number greater than 0' })
  end
  if cost == nil or cost ~= cost or cost == math.huge then
    return cjson.encode({ acquired = false, waitTime = 0, remainingTokens = 0, error = 'cost must be a finite number' })
  end
  -- Guarded even though the refill below tolerates it: waitTime is derived from
  -- interval, and a non-finite one clears the '< 0' clamp on the way to
  -- cjson.encode.
  if interval == nil or interval ~= interval or interval == math.huge or interval == -math.huge then
    return cjson.encode({ acquired = false, waitTime = 0, remainingTokens = 0, error = 'interval must be a finite number' })
  end

  -- finiteNumber, not tonumber: a stored 'nan' passes a nil test, survives every
  -- comparison and math.min, and then fails in cjson.encode below — so one corrupt
  -- byte made every acquire on this bucket throw, with nothing rewriting the field.
  local tokens = finiteNumber(redis.call('HGET', key, 'tokens'), maxTokens)
  local lastUpdate = finiteNumber(redis.call('HGET', key, 'lastUpdate'), now)

${lazyRefill()}

  -- Check if we have enough tokens
  if tokens >= cost then
    -- Deduct tokens and update state
    tokens = tokens - cost
    redis.call('HSET', key, 'tokens', tokens, 'lastUpdate', lastUpdate)
    redis.call('EXPIRE', key, ttl)
    return cjson.encode({ acquired = true, remainingTokens = tokens })
  else
    -- Wait until enough tokens have accrued.
    --
    -- lastUpdate only advances in whole intervals, so (now - lastUpdate)
    -- is progress already made toward the next refill and must be
    -- subtracted. Without that a bucket 9.9s into a 10s interval reported a
    -- full 10s wait instead of 100ms, and the caller sleeps on this value
    -- directly, so it became a real ten-second stall.
    local needed = cost - tokens
    local intervalsNeeded = math.ceil(needed / tokensToAdd)
    local waitTime = intervalsNeeded * interval - (now - lastUpdate)
    if waitTime < 0 then waitTime = 0 end

    -- Update state to reflect any tokens that were added
    redis.call('HSET', key, 'tokens', tokens, 'lastUpdate', lastUpdate)
    redis.call('EXPIRE', key, ttl)

    return cjson.encode({ acquired = false, waitTime = waitTime, remainingTokens = tokens })
  end
`;

/**
 * Refuses while a freeze is in force: the freeze empties the bucket on
 * purpose, so crediting a refund into it would hand back budget the fleet has
 * just been told not to spend. A missing bucket is left missing rather than
 * created — an absent bucket already reads as full.
 *
 * `lastUpdate` is deliberately untouched. It marks where refill accounting has
 * reached, and moving it would either forfeit or duplicate accrued intervals.
 */
export const refundTokens = `
  local key = KEYS[1]
  local freezeKey = KEYS[2]
  local cost = tonumber(ARGV[1])
  local maxTokens = tonumber(ARGV[2])

  if freezeKey ~= '' then
    local frozenUntil = tonumber(redis.call('HGET', freezeKey, 'frozenUntil')) or 0
${redisNow("    ")}
    if frozenUntil > now then return 0 end
  end

  -- The raw reply distinguishes an absent bucket, which must stay absent because an
  -- absent bucket already reads as full. Parseability is then a separate question:
  -- 'abc' is truthy in Lua, so testing the string alone let a nil reach the addition.
  local raw = redis.call('HGET', key, 'tokens')
  if raw == false then return 0 end
  local tokens = tonumber(raw)
  if tokens == nil or tokens ~= tokens then tokens = 0 end
  redis.call('HSET', key, 'tokens', math.min(maxTokens, tokens + cost))
  return 1
`;

/**
 * Admits without queueing when nothing is contending, since an empty queue has no
 * ordering to arrange. One atomic script — refuse if anything is queued, refuse if
 * frozen, else spend — so the fast path can never overtake queued work or
 * double-spend. Uses the refill arithmetic of `tokenBucket` above verbatim, so
 * both paths agree on the bucket.
 */
export const tryAdmitImmediately = `
  local queueKey = KEYS[1]
  local bucketKey = KEYS[2]
  local freezeKey = KEYS[3]
  local cost = tonumber(ARGV[1])
  local maxTokens = tonumber(ARGV[2])
  local tokensToAdd = tonumber(ARGV[3])
  local interval = tonumber(ARGV[4])
  -- Positional, and moved from [6]: see acquireConcurrency in concurrency.ts.
  local ttl = tonumber(ARGV[5]) or 86400

${redisNow()}

${finiteNumberFn()}
  if not (tokensToAdd > 0) then return 0 end
  if not (maxTokens > 0) or maxTokens == math.huge then return 0 end
  if cost == nil or cost ~= cost or cost == math.huge then return 0 end
  if interval == nil or interval ~= interval or interval == math.huge or interval == -math.huge then return 0 end

${queueEmptyGate()}

${freezeGate()}

  -- Same reading as the queue path's, so both agree on one bucket: a stored 'nan'
  -- otherwise left this script admitting every request unmetered.
  local tokens = finiteNumber(redis.call('HGET', bucketKey, 'tokens'), maxTokens)
  local lastUpdate = finiteNumber(redis.call('HGET', bucketKey, 'lastUpdate'), now)

${lazyRefill()}

  if tokens < cost then
    redis.call('HSET', bucketKey, 'tokens', tokens, 'lastUpdate', lastUpdate)
    redis.call('EXPIRE', bucketKey, ttl)
    return 0
  end

  tokens = tokens - cost
  redis.call('HSET', bucketKey, 'tokens', tokens, 'lastUpdate', lastUpdate)
  redis.call('EXPIRE', bucketKey, ttl)
  return 1
`;
