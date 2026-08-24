/** Instance-coordination Lua. Twin of the memory backend's instance section. */

export const getInstances = `
  local instanceSetKey = KEYS[1]
  local instanceKeyPrefix = ARGV[1]
  local currentInstanceId = ARGV[2]

  local ids = redis.call('SMEMBERS', instanceSetKey)
  local instances = {}
  local staleIds = {}

  for i, id in ipairs(ids) do
    -- Skip current instance (caller will add it separately)
    if id ~= currentInstanceId then
      local data = redis.call('GET', instanceKeyPrefix .. id)
      -- '~= false', not truthiness: GET yields false for a missing key, and an
      -- empty string is truthy in Lua but falsy in the memory twin's JavaScript.
      if data ~= false and data ~= '' then
        table.insert(instances, { id = id, data = data })
      else
        table.insert(staleIds, id)
      end
    end
  end

  -- Chunked: 'unpack' raises above LUAI_MAXCSTACK (8000) and does so BEFORE the
  -- SREM, so one outage large enough to strand that many heartbeats made every
  -- later call fail identically with nothing pruned — instance discovery dead
  -- fleet-wide, with no path that recovers on its own.
  for i = 1, #staleIds, 500 do
    local last = math.min(i + 499, #staleIds)
    redis.call('SREM', instanceSetKey, unpack(staleIds, i, last))
  end

  if #instances == 0 then
    return "[]"
  end
  return cjson.encode(instances)
`;
