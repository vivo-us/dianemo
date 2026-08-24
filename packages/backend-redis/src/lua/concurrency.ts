import {
  finiteNumberFn,
  freezeGate,
  queueEmptyGate,
  redisNow,
  slotAccounting,
  ttlArgvWarning,
  usableConcurrencyFn,
  usableCostFn,
} from "./fragments.js";

/**
 * Concurrency Lua. A slot is a member of a sorted set scored with the time it
 * was claimed, and its cost lives in a parallel `:costs` hash because a sorted
 * set has only the one score.
 *
 * Twin of the memory backend's concurrency section.
 */

/**
 * Nothing expires a slot on its own. A slot outlives its request until some
 * later acquire scans for members older than `requestTtl` and reaps them — the
 * key's own TTL is `keyTtl`, an unrelated 24 hours, and reaping never happens
 * on a read.
 */
export const acquireConcurrency = `
  local key = KEYS[1]
  local cost = tonumber(ARGV[1])
  local maxConcurrency = tonumber(ARGV[2])
  local requestId = ARGV[3]
${ttlArgvWarning()}
  local requestTtl = tonumber(ARGV[4])
${redisNow()}

  local keyTtl = tonumber(ARGV[5]) or 86400

${finiteNumberFn()}
${usableConcurrencyFn()}
${usableCostFn()}
  if not usableConcurrency(maxConcurrency) then
    return cjson.encode({ acquired = false, currentConcurrency = 0, error = 'maxConcurrency must be a finite number greater than 0' })
  end
  if not usableCost(cost) then
    return cjson.encode({ acquired = false, currentConcurrency = 0, error = 'cost must be a finite number that is not negative' })
  end

${slotAccounting()}

  -- Refusal on '>', in all three scripts: an admission written as the negation
  -- instead puts the boundary in two places that can drift apart.
  if currentCost + cost > maxConcurrency then
    return cjson.encode({ acquired = false, currentConcurrency = currentCost })
  end

  redis.call('ZADD', key, now, requestId)
  redis.call('HSET', costKey, requestId, cost)
  redis.call('EXPIRE', key, keyTtl)
  redis.call('EXPIRE', costKey, keyTtl)
  return cjson.encode({ acquired = true, currentConcurrency = currentCost + cost })
`;

export const acquireQueuedConcurrency = `
  local key = KEYS[1]
  local metadataKey = KEYS[2]
  local cost = tonumber(ARGV[1])
  local maxConcurrency = tonumber(ARGV[2])
  local requestId = ARGV[3]
${ttlArgvWarning()}
  local requestTtl = tonumber(ARGV[4])
  local keyTtl = tonumber(ARGV[5]) or 86400

${redisNow()}

${finiteNumberFn()}
${usableConcurrencyFn()}
${usableCostFn()}
  -- 0, not currentCost: an unusable ceiling makes the occupancy meaningless, and
  -- acquireConcurrency and both memory paths already report 0 here.
  if not usableConcurrency(maxConcurrency) then
    return cjson.encode({ acquired = false, currentConcurrency = 0, error = 'maxConcurrency must be a finite number greater than 0' })
  end
  if not usableCost(cost) then
    return cjson.encode({ acquired = false, currentConcurrency = 0, error = 'cost must be a finite number that is not negative' })
  end

${slotAccounting()}

  if redis.call('HGET', metadataKey, 'status') ~= 'inProgress' then
    return cjson.encode({ acquired = false, currentConcurrency = currentCost })
  end
  if currentCost + cost > maxConcurrency then
    return cjson.encode({ acquired = false, currentConcurrency = currentCost })
  end

  redis.call('ZADD', key, now, requestId)
  redis.call('HSET', costKey, requestId, cost)
  redis.call('EXPIRE', key, keyTtl)
  redis.call('EXPIRE', costKey, keyTtl)
  return cjson.encode({ acquired = true, currentConcurrency = currentCost + cost })
`;

export const releaseConcurrency = `
  local key = KEYS[1]
  local requestId = ARGV[1]
  local costKey = key .. ':costs'

  redis.call('ZREM', key, requestId)
  redis.call('HDEL', costKey, requestId)

  return 1
`;

export const getConcurrencyState = `
  local key = KEYS[1]
  local requestTtl = tonumber(ARGV[1])
  local costKey = key .. ':costs'

${redisNow()}
${finiteNumberFn()}
  local expireTime = now - requestTtl

  -- Observability is non-destructive. Admission performs stale-slot cleanup;
  -- a stats read must never change who may be admitted.
  local members = redis.call('ZRANGEBYSCORE', key, '(' .. expireTime, '+inf')
  local totalCost = 0

  for i, member in ipairs(members) do
    -- finiteNumber, as getQueueStats reads its costs: a stored 'nan' survives the
    -- addition and then fails in cjson.encode, failing the read for the whole key
    -- rather than defaulting the one corrupt slot.
    local cost = finiteNumber(redis.call('HGET', costKey, member), 1)
    totalCost = totalCost + cost
  end

  -- cjson encodes an empty Lua table as '{}', not '[]', so with no slots
  -- held activeRequests would come back as an object the caller cannot
  -- iterate. Emit the array form explicitly.
  if #members == 0 then
    return '{"currentConcurrency":' .. totalCost .. ',"activeRequests":[]}'
  end
  return cjson.encode({ currentConcurrency = totalCost, activeRequests = members })
`;

/**
 * Concurrency-limited counterpart to `tryAdmitImmediately` in `tokenBucket.ts`.
 *
 * Same contract — refuse if anything is queued or the client is frozen,
 * otherwise admit — but the budget here is in-flight slots rather than
 * tokens. The slot accounting mirrors `acquireConcurrency` above exactly,
 * including expiry of slots whose request never reported completion.
 */
export const tryAdmitConcurrency = `
  local queueKey = KEYS[1]
  local key = KEYS[2]
  local freezeKey = KEYS[3]
  local cost = tonumber(ARGV[1])
  local maxConcurrency = tonumber(ARGV[2])
  local requestId = ARGV[3]
${ttlArgvWarning()}
  local requestTtl = tonumber(ARGV[4])
${redisNow()}

  local keyTtl = tonumber(ARGV[5]) or 86400

${finiteNumberFn()}
${usableConcurrencyFn()}
${usableCostFn()}
  if not usableConcurrency(maxConcurrency) then return 0 end
  if not usableCost(cost) then return 0 end

${queueEmptyGate()}

${freezeGate()}

${slotAccounting()}

  if currentCost + cost > maxConcurrency then return 0 end

  redis.call('ZADD', key, now, requestId)
  redis.call('HSET', costKey, requestId, cost)
  redis.call('EXPIRE', key, keyTtl)
  redis.call('EXPIRE', costKey, keyTtl)
  return 1
`;
