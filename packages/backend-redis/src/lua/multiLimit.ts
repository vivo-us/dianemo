import {
  finiteNumberFn,
  lazyRefill,
  redisNow,
  slotAccounting,
  usableConcurrencyFn,
  usableCostFn,
} from "./fragments.js";

/**
 * Several budgets claimed as one operation. Twin of the memory backend's
 * `acquireMultiLimit` / `planMultiLimit` pair.
 *
 * These three scripts are the only ones registered without a fixed key count,
 * because the number of budgets is a property of the client's configuration
 * rather than of the script. Every key is still declared in `KEYS` — passing
 * them through `ARGV` would work on a single node and hide the cross-slot
 * problem from Redis Cluster, which is exactly the failure worth keeping loud.
 *
 * `ARGV[2]` carries the spec list as JSON: one object per budget, holding the
 * kind, the 1-based `KEYS` index it lives at, and the configuration to meter it
 * against. A positional ARGV per field would renumber every slot whenever a
 * budget was added — see `ttlArgvWarning` in `fragments.ts`.
 */

/**
 * The check phase both acquiring scripts share, declared as a function so the
 * two agree on the arithmetic character for character.
 *
 * Refill credit is written whether or not the request gets to spend it: the
 * intervals really did elapse. Slot reaping is likewise unconditional — an
 * expired slot is expired whoever asked.
 *
 * Requires: `KEYS`, `finiteNumber`, `usableConcurrency` and `usableCost`.
 * Declares `planMultiLimit`, `commitMultiLimit` and `persistRefills`.
 */
const planFn = `
local function planMultiLimit(specs, cost, now)
  local plan = { acquired = true, buckets = {}, slots = {} }
  for _, spec in ipairs(specs) do
    local key = KEYS[spec.k]
    if spec.kind == 'tokenBucket' then
      local maxTokens = spec.maxTokens
      local tokensToAdd = spec.tokensToAdd
      local interval = spec.interval
      -- The same four refusals the single-budget token bucket makes, in the same
      -- order, so one unusable entry is reported rather than metered.
      if not (tokensToAdd > 0) then
        return { acquired = false, error = 'tokensToAdd must be greater than 0' }
      end
      if not (maxTokens > 0) or maxTokens == math.huge then
        return { acquired = false, error = 'maxTokens must be a finite number greater than 0' }
      end
      if cost == nil or cost ~= cost or cost == math.huge then
        return { acquired = false, error = 'cost must be a finite number' }
      end
      if interval == nil or interval ~= interval or interval == math.huge or interval == -math.huge then
        return { acquired = false, error = 'interval must be a finite number' }
      end

      local tokens = finiteNumber(redis.call('HGET', key, 'tokens'), maxTokens)
      local lastUpdate = finiteNumber(redis.call('HGET', key, 'lastUpdate'), now)

${lazyRefill("      ")}

      table.insert(plan.buckets, { key = key, tokens = tokens, lastUpdate = lastUpdate })
      if tokens < cost then
        local intervalsNeeded = math.ceil((cost - tokens) / tokensToAdd)
        local wait = intervalsNeeded * interval - (now - lastUpdate)
        if wait < 0 then wait = 0 end
        -- The longest wait any budget asks for: waking sooner only to be refused
        -- by this same budget is a wake-up that cannot succeed.
        if plan.waitTime == nil or wait > plan.waitTime then
          plan.waitTime = wait
          plan.blockedBy = key
        end
        plan.acquired = false
      end
    else
      local maxConcurrency = spec.maxConcurrency
      local requestTtl = spec.requestTtl
      local requestId = spec.slotId
      if not usableConcurrency(maxConcurrency) then
        return { acquired = false, error = 'maxConcurrency must be a finite number greater than 0' }
      end
      if not usableCost(cost) then
        return { acquired = false, error = 'cost must be a finite number that is not negative' }
      end

${slotAccounting("      ")}

      if currentCost + cost > maxConcurrency then
        plan.acquired = false
        -- No deadline of its own: a slot is freed by a completion, and the caller
        -- decides what to fall back on. Recorded only if nothing with a real wait
        -- already claimed the report.
        if plan.blockedBy == nil then plan.blockedBy = key end
      else
        table.insert(plan.slots, { key = key, slotId = requestId })
      end
    end
  end
  return plan
end

local function commitMultiLimit(plan, cost, now, ttl)
  for _, bucket in ipairs(plan.buckets) do
    redis.call('HSET', bucket.key, 'tokens', bucket.tokens - cost, 'lastUpdate', bucket.lastUpdate)
    redis.call('EXPIRE', bucket.key, ttl)
  end
  for _, slot in ipairs(plan.slots) do
    redis.call('ZADD', slot.key, now, slot.slotId)
    redis.call('HSET', slot.key .. ':costs', slot.slotId, cost)
    redis.call('EXPIRE', slot.key, ttl)
    redis.call('EXPIRE', slot.key .. ':costs', ttl)
  end
end

local function persistRefills(plan, ttl)
  for _, bucket in ipairs(plan.buckets) do
    redis.call('HSET', bucket.key, 'tokens', bucket.tokens, 'lastUpdate', bucket.lastUpdate)
    redis.call('EXPIRE', bucket.key, ttl)
  end
end
`;

export const acquireMultiLimit = `
  local cost = tonumber(ARGV[1])
  local specs = cjson.decode(ARGV[2])
  local ttl = tonumber(ARGV[3]) or 86400

${redisNow()}

${finiteNumberFn()}

${usableConcurrencyFn()}

${usableCostFn()}

${planFn}

  local plan = planMultiLimit(specs, cost, now)
  if plan.error then
    return cjson.encode({ acquired = false, error = plan.error })
  end
  if not plan.acquired then
    persistRefills(plan, ttl)
    return cjson.encode({ acquired = false, waitTime = plan.waitTime or 0, blockedBy = plan.blockedBy })
  end
  commitMultiLimit(plan, cost, now, ttl)
  return cjson.encode({ acquired = true })
`;

/**
 * The no-queue fast path across several budgets.
 *
 * `KEYS[1]` is the queue; the freeze keys and the budgets follow, addressed by
 * the indices in `ARGV[2]`. Refusing while anything is queued and while any
 * governing freeze stands are both part of the same atomic decision, for the
 * reason `tryAdmitImmediately` gives: answering them separately lets a peer
 * enqueue in between and be overtaken.
 */
export const tryAdmitMultiLimit = `
  local queueKey = KEYS[1]
  local cost = tonumber(ARGV[1])
  local payload = cjson.decode(ARGV[2])
  local ttl = tonumber(ARGV[3]) or 86400
  local specs = payload.specs

${redisNow()}

${finiteNumberFn()}

${usableConcurrencyFn()}

${usableCostFn()}

${planFn}

  if redis.call('ZCARD', queueKey) > 0 then return 0 end

  for _, index in ipairs(payload.freezeKeys) do
    local freeze = redis.call('HMGET', KEYS[index], 'frozenUntil', 'thawRequestCount')
    local frozenUntil = finiteNumber(freeze[1], nil)
    if frozenUntil ~= nil then
      if now < frozenUntil then return 0 end
      -- Once the freeze lapses with probes outstanding, recovery is one request
      -- at a time and only the queue path can arrange that.
      if finiteNumber(freeze[2], 0) > 0 then return 0 end
    end
  end

  local plan = planMultiLimit(specs, cost, now)
  if plan.error then return 0 end
  if not plan.acquired then
    persistRefills(plan, ttl)
    return 0
  end
  commitMultiLimit(plan, cost, now, ttl)
  return 1
`;

/**
 * Hands back what `acquireMultiLimit` claimed.
 *
 * A token refund is refused while the freeze governing that bucket stands, for
 * the reason `refundTokens` gives: the freeze empties buckets on purpose, so
 * crediting one hands back budget the fleet has just been told not to spend. A
 * slot is released either way — nothing else will, and a held slot is not
 * budget the freeze meant to withhold.
 */
export const releaseMultiLimit = `
  local cost = tonumber(ARGV[1])
  local specs = cjson.decode(ARGV[2])

${redisNow()}

  for _, spec in ipairs(specs) do
    local key = KEYS[spec.k]
    if spec.kind == 'concurrency' then
      redis.call('ZREM', key, spec.slotId)
      redis.call('HDEL', key .. ':costs', spec.slotId)
    else
      local frozen = false
      if spec.fk ~= nil then
        local frozenUntil = tonumber(redis.call('HGET', KEYS[spec.fk], 'frozenUntil')) or 0
        if frozenUntil > now then frozen = true end
      end
      if not frozen then
        -- An absent bucket stays absent: an absent bucket already reads as full,
        -- so creating one here would invent a balance rather than restore it.
        local raw = redis.call('HGET', key, 'tokens')
        if raw ~= false then
          local tokens = tonumber(raw)
          if tokens == nil or tokens ~= tokens then tokens = 0 end
          redis.call('HSET', key, 'tokens', math.min(spec.maxTokens, tokens + cost))
        end
      end
    end
  end
  return 1
`;
