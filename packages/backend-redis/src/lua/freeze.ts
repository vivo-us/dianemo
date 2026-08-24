import {
  finiteNumberFn,
  freezeGate,
  queueEmptyGate,
  redisNow,
} from "./fragments.js";

/**
 * Freeze and thaw Lua. Every script here takes its clock from redis `TIME`:
 * these decide when a back-off lapses, and on a caller-supplied clock one
 * skewed replica could unfreeze a client the rest of the fleet still holds.
 *
 * Twin of the memory backend's freeze/thaw section.
 */

/**
 * A `noLimit` client has no bucket and no slots, so its whole admission
 * decision is these two reads — which is exactly why they must not be two
 * round trips. Between a separate ZCARD and a separate freeze read, a peer can
 * enqueue, and the arrival that read "queue empty" then overtakes it.
 */
export const tryAdmitNoLimit = `
  local queueKey = KEYS[1]
  local freezeKey = KEYS[2]

${redisNow()}

${finiteNumberFn()}
${queueEmptyGate()}

${freezeGate()}

  return 1
`;

export const writeFreezeState = `
  local key = KEYS[1]
${finiteNumberFn()}
  -- Sanitised before the merge, not after: every comparison against a nan is false,
  -- so the monotone merge below would keep it and string.format('%d', nan) writes
  -- '0' — cancelling a standing deadline and the probe budget a 429 was owed.
  local frozenUntil = finiteNumber(ARGV[1], 0)
  local thawRequestCount = finiteNumber(ARGV[2], 0)
  local mode = ARGV[3]

  if mode == 'max' then
    local existing = redis.call('HMGET', key, 'frozenUntil', 'thawRequestCount')
    -- The nil test asks whether there is any state to merge with; finiteNumber then
    -- decides what that state is worth.
    if tonumber(existing[1]) ~= nil then
      local existingUntil = finiteNumber(existing[1], 0)
      -- Never shortened, and never a lower probe budget. See armFreeze.
      if existingUntil > frozenUntil then frozenUntil = existingUntil end
      local existingThaw = finiteNumber(existing[2], 0)
      if existingThaw > thawRequestCount then
        thawRequestCount = existingThaw
      end
    end
  end

  -- Clamped after the merge, so a deadline already stored beyond the ceiling is
  -- brought down rather than carried forward by "take the larger". 2^53-1 is
  -- where an integer reply stops surviving the trip back into the caller's
  -- Number; past it string.format('%d') clamps to 2^63-1 instead of raising,
  -- and this is also what keeps the ttl below within EXPIRE's range.
  if frozenUntil > 9007199254740991 then frozenUntil = 9007199254740991 end

  -- Sized against redis TIME, the same clock frozenUntil is written from.
  -- On the caller's clock, a process running behind sized the key to expire
  -- BEFORE the freeze it was holding. Derived above the writes because EXPIRE
  -- is what rejects an out-of-range ttl, and it runs second.
${redisNow()}
  local ttl = math.ceil((frozenUntil - now) / 1000) + 60
  if ttl < 60 then ttl = 60 end

  -- '%d', not tostring: a millisecond epoch is 13 digits and Lua 5.1's
  -- default number formatting is %.14g — correct today, and silently lossy
  -- the moment either value grows a digit or gains a fraction.
  redis.call('HSET', key,
    'frozenUntil', string.format('%d', frozenUntil),
    'thawRequestCount', string.format('%d', thawRequestCount))
  redis.call('EXPIRE', key, ttl)

  return { frozenUntil, thawRequestCount }
`;

/**
 * Reads the freeze state and decides, on Redis's clock, in one round trip.
 *
 * `getFreezeState`, `isFrozen` and `canProcessRequest` all answer questions
 * about the same two fields against the same clock, and each fetched TIME
 * separately — `canProcessRequest` twice, because it called `getFreezeState`
 * (which had already read TIME) and then read it again. Three commands for one
 * decision, and the decision was taken against a clock sampled AFTER the state,
 * so it leaned toward "lapsed" by a round trip. One script is one clock.
 */
export const readFreezeState = `
  local key = KEYS[1]
  local state = redis.call('HMGET', key, 'frozenUntil', 'thawRequestCount')
${finiteNumberFn()}
  -- An unusable deadline counts as no freeze state at all. A nan parses through
  -- tonumber and then survives every comparison, which reported a client frozen
  -- with zero probes owed and no deadline any wake-up could be booked against.
  local frozenUntil = finiteNumber(state[1], nil)
  if frozenUntil == nil then return nil end
  local thawRequestCount = finiteNumber(state[2], 0)

${redisNow()}

  -- Reports expiry without deleting. Deleting on a READ would be judged
  -- against whichever replica happened to look, so one fast process could
  -- cancel a live freeze for the whole fleet just by polling a health
  -- endpoint. updateThawProgress is the only place that deletes.
  if now >= frozenUntil and thawRequestCount <= 0 then return nil end

  return { frozenUntil, thawRequestCount, (now < frozenUntil) and 1 or 0 }
`;

export const updateThawProgress = `
  local key = KEYS[1]
  local completionKey = KEYS[2]
  local completionId = ARGV[1]
  local success = ARGV[2] == '1'
${finiteNumberFn()}

  if completionId ~= '' and redis.call('SISMEMBER', completionKey, completionId) == 1 then
    local duplicateState = redis.call('HMGET', key, 'frozenUntil', 'thawRequestCount')
    local duplicateUntil = finiteNumber(duplicateState[1], nil)
    if duplicateUntil == nil then return nil end
    return cjson.encode({
      frozenUntil = duplicateUntil,
      thawRequestCount = finiteNumber(duplicateState[2], 0)
    })
  end
  if completionId ~= '' then
    redis.call('SADD', completionKey, completionId)
    redis.call('EXPIRE', completionKey, 86400)
  end

  local state = redis.call('HMGET', key, 'frozenUntil', 'thawRequestCount')
  local frozenUntil = finiteNumber(state[1], nil)
  if frozenUntil == nil then return nil end
  local thawRequestCount = finiteNumber(state[2], 0)

${redisNow()}

  -- The one place expiry deletes the key. A read must not: see getFreezeState.
  if now >= frozenUntil and thawRequestCount <= 0 then
    redis.call('DEL', key)
    return cjson.encode({ cleared = true })
  end

  if not success then
    return cjson.encode({ frozenUntil = frozenUntil, thawRequestCount = thawRequestCount })
  end

  local newCount = thawRequestCount - 1
  if newCount <= 0 then
    redis.call('DEL', key)
    return cjson.encode({ cleared = true })
  end

  redis.call('HSET', key, 'thawRequestCount', newCount)
  return cjson.encode({ frozenUntil = frozenUntil, thawRequestCount = newCount })
`;

/**
 * Marks the grant frozen, checks for an in-progress probe and stakes the claim in
 * one step, so two controllers cannot both probe the same grant.
 */
export const tryStartThawRequest = `
  local frozenGrantsKey = KEYS[1]
  local queueKey = KEYS[2]
  local metadataKeyPrefix = KEYS[3]
  local metadataKey = KEYS[4]
  local grantId = ARGV[1]
  local requestId = ARGV[2]

  -- Step 1: Track the frozen grant. A client-level thaw (no grant) has no
  -- entry here — the frozen-grants set exists to let getNextRequest skip
  -- individual grants, and a client-level freeze blocks everything anyway.
  if grantId ~= '' then
    redis.call('SADD', frozenGrantsKey, grantId)
    -- The 24 hours every other key in the system is sized to. Membership is
    -- normally dropped by cleanupStaleFrozenGrants, which only runs while a
    -- controller does; without an expiry a fleet that stops holds the set, and
    -- getNextRequest skips those grants for as long as it survives.
    redis.call('EXPIRE', frozenGrantsKey, 86400)
  end

  -- Step 2: Check if there's already an in-progress thaw request for this grant
  local requestIds = redis.call('ZRANGE', queueKey, 0, -1)

  for i, reqId in ipairs(requestIds) do
    -- Skip checking our own request
    if reqId ~= requestId then
      local reqMetadataKey = metadataKeyPrefix .. ':' .. reqId
      local fields = redis.call('HMGET', reqMetadataKey, 'status', 'grantId', 'isThawRequest')
      local status = fields[1]
      -- HMGET yields false for an absent field; normalise so a
      -- client-level request ('' grant) matches its own kind.
      local reqGrantId = fields[2] or ''
      local isThawRequest = fields[3]

      -- Check if this is an in-progress thaw request for the same grant
      if status == 'inProgress' and reqGrantId == grantId and isThawRequest == 'true' then
        return 'exists'
      end
    end
  end

  -- Step 3: No existing thaw request found, mark this request as the thaw request.
  -- Only if the entry still exists: HSET would otherwise CREATE a hash holding one
  -- field and no TTL, for a request whose metadata expired or whose peer completed
  -- it since selection. Nothing reclaims that — cleanupOrphanedRequests walks queue
  -- members and this key has none — and the grant's single probe slot would be held
  -- by a request that is gone.
  if redis.call('EXISTS', metadataKey) == 0 then return 'exists' end
  redis.call('HSET', metadataKey, 'isThawRequest', 'true')

  return 'started'
`;

/**
 * Scans all requests in the queue and checks if any match:
 * - status === "inProgress"
 * - grantId === target grantId
 * - isThawRequest === "true"
 */
export const hasThawRequestInProgress = `
  local queueKey = KEYS[1]
  local metadataKeyPrefix = KEYS[2]
  local targetGrantId = ARGV[1]

  -- Get all request IDs from the queue
  local requestIds = redis.call('ZRANGE', queueKey, 0, -1)

  for i, requestId in ipairs(requestIds) do
    local metadataKey = metadataKeyPrefix .. ':' .. requestId
    -- Fetch all three fields in a single call
    local fields = redis.call('HMGET', metadataKey, 'status', 'grantId', 'isThawRequest')
    local status = fields[1]
    -- Normalised exactly as tryStartThawRequest does: HMGET yields false for an
    -- absent field, and false is not equal to the empty string, so a client-level
    -- probe went unnoticed here while that script counted it.
    local grantId = fields[2] or ''
    local isThawRequest = fields[3]

    if status == 'inProgress' and grantId == targetGrantId and isThawRequest == 'true' then
      return 1
    end
  end

  return 0
`;

// If this ever needs another ARGV, add it after ARGV[1] and leave TIME alone.
export const cleanupStaleFrozenGrants = `
  local frozenGrantsKey = KEYS[1]
  local queueKey = KEYS[2]
  local metadataKeyPrefix = KEYS[3]
  local freezeStateKeyPrefix = ARGV[1]
${redisNow()}
${finiteNumberFn()}

  local frozenGrantIds = redis.call('SMEMBERS', frozenGrantsKey)
  local cleaned = 0

  -- Pre-fetch all request metadata to check for thaw requests
  local requestIds = redis.call('ZRANGE', queueKey, 0, -1)
  local thawRequestGrants = {}

  for i, requestId in ipairs(requestIds) do
    local metadataKey = metadataKeyPrefix .. ':' .. requestId
    local fields = redis.call('HMGET', metadataKey, 'status', 'grantId', 'isThawRequest')
    local status = fields[1]
    local grantId = fields[2]
    local isThawRequest = fields[3]

    if status == 'inProgress' and isThawRequest == 'true' and grantId then
      thawRequestGrants[grantId] = true
    end
  end

  -- Check each frozen grant
  for i, grantId in ipairs(frozenGrantIds) do
    -- Skip if there's an in-progress thaw request
    if not thawRequestGrants[grantId] then
      -- Check freeze state (key format: prefix + grantId + ":freezeState")
      local freezeStateKey = freezeStateKeyPrefix .. grantId .. ':freezeState'
      -- A default of 0 releases the grant, as an absent freeze state does. Under
      -- a bare tonumber a stored 'nan' or 'inf' looks like neither: not absent,
      -- and never passed by the clock either.
      local frozenUntil = finiteNumber(redis.call('HGET', freezeStateKey, 'frozenUntil'), 0)

      if now >= frozenUntil then
        redis.call('SREM', frozenGrantsKey, grantId)
        cleaned = cleaned + 1
      end
    end
  end

  return cleaned
`;
