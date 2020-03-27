local resty_lock = require("resty.lock")
local util = require("resty.util")

local string_format = string.format
local ngx_log = ngx.log
local INFO = ngx.INFO

local DECAY_TIME = 10 -- this value is in seconds
local LOCK_KEY = ":ewma_key"
local PICK_SET_SIZE = 2

local ewma_lock, ewma_lock_err = resty_lock:new("balancer_ewma_locks", {timeout = 0, exptime = 0.1})
if not ewma_lock then
  error(ewma_lock_err)
end

local _M = { name = "ewma" }

local function lock(upstream)
  local _, err = ewma_lock:lock(upstream .. LOCK_KEY)
  if err then
    if err ~= "timeout" then
      ngx.log(ngx.ERR, string.format("EWMA Balancer failed to lock: %s", tostring(err)))
    end
  end

  return err
end

local function unlock()
  local ok, err = ewma_lock:unlock()
  if not ok then
    ngx.log(ngx.ERR, string.format("EWMA Balancer failed to unlock: %s", tostring(err)))
  end

  return err
end

local function decay_ewma(ewma, last_touched_at, rtt, now)
  local td = now - last_touched_at
  td = (td > 0) and td or 0
  local weight = math.exp(-td/DECAY_TIME)

  ewma = ewma * weight + rtt * (1.0 - weight)
  return ewma
end

local function store_stats(upstream, ewma, now)
  local success, err, forcible = ngx.shared.balancer_ewma_last_touched_at:set(upstream, now)
  if not success then
    ngx.log(ngx.WARN, "balancer_ewma_last_touched_at:set failed " .. err)
  end
  if forcible then
    ngx.log(ngx.WARN, "balancer_ewma_last_touched_at:set valid items forcibly overwritten")
  end

  success, err, forcible = ngx.shared.balancer_ewma:set(upstream, ewma)
  if not success then
    ngx.log(ngx.WARN, "balancer_ewma:set failed " .. err)
  end
  if forcible then
    ngx.log(ngx.WARN, "balancer_ewma:set valid items forcibly overwritten")
  end
end

local function get_or_update_ewma(upstream, rtt, update)
  local lock_err = nil
  if update then
    lock_err = lock(upstream)
  end
  local ewma = ngx.shared.balancer_ewma:get(upstream) or 0
  if lock_err ~= nil then
    return ewma, lock_err
  end

  local now = ngx.now()
  local last_touched_at = ngx.shared.balancer_ewma_last_touched_at:get(upstream) or 0
  ewma = decay_ewma(ewma, last_touched_at, rtt, now)

  if not update then
    return ewma, nil
  end

  store_stats(upstream, ewma, now)

  unlock()

  return ewma, nil
end


local function score(upstream)
  -- Original implementation used names
  -- Endpoints don't have names, so passing in IP:Port as key instead
  local upstream_name = upstream.address .. ":" .. upstream.port
  return get_or_update_ewma(upstream_name, 0, false)
end

-- implementation similar to https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
-- or https://en.wikipedia.org/wiki/Random_permutation
-- loop from 1 .. k
-- pick a random value r from the remaining set of unpicked values (i .. n)
-- swap the value at position i with the value at position r
local function shuffle_peers(peers, k)
  for i=1, k do
    local rand_index = math.random(i,#peers)
    peers[i], peers[rand_index] = peers[rand_index], peers[i]
  end
  -- peers[1 .. k] will now contain a randomly selected k from #peers
end

local function pick_and_score(peers, k)
  shuffle_peers(peers, k)
  local lowest_score_index = 1
  local lowest_score = score(peers[lowest_score_index])
  for i = 2, k do
    local new_score = score(peers[i])
    if new_score < lowest_score then
      lowest_score_index, lowest_score = i, new_score
    end
  end
  return peers[lowest_score_index], lowest_score
end

-- slow_start_ewma is something we use to avoid sending too many requests
-- to the newly introduced endpoints. We currently use average ewma values
-- of existing endpoints.
local function calculate_slow_start_ewma(self)
  local total_ewma = 0
  local endpoints_count = 0

  for _, endpoint in pairs(self.peers) do
    local endpoint_string = endpoint.address .. ":" .. endpoint.port
    local ewma = ngx.shared.balancer_ewma:get(endpoint_string)

    if ewma then
      endpoints_count = endpoints_count + 1
      total_ewma = total_ewma + ewma
    end
  end

  if endpoints_count == 0 then
    ngx.log(ngx.INFO, "no ewma value exists for the endpoints")
    return nil
  end

  return total_ewma / endpoints_count
end

function _M.balance(self)
  local peers = self.peers
  local endpoint, ewma_score = peers[1], -1

  if #peers > 1 then
    local k = (#peers < PICK_SET_SIZE) and #peers or PICK_SET_SIZE
    local peer_copy = util.deepcopy(peers)
    endpoint, ewma_score = pick_and_score(peer_copy, k)
  end

  return endpoint.address .. ":" .. endpoint.port
end

function _M.after_balance(_, resp_time, upstream_addr)
  local response_time = tonumber(resp_time) or 0
  local connect_time = 0
  local rtt = connect_time + response_time
  local upstream = upstream_addr

  if util.is_blank(upstream) then
    print("something wrong")
    return
  end

  get_or_update_ewma(upstream, rtt, true)
end

function _M.sync(self, backend)
  local normalized_endpoints_added, normalized_endpoints_removed = util.diff_endpoints(self.peers, backend.endpoints)

  if #normalized_endpoints_added == 0 and #normalized_endpoints_removed == 0 then
    ngx.log(ngx.INFO, "endpoints did not change for backend " .. tostring(backend.name))
    return
  end

  ngx_log(INFO, string_format("[%s] peers have changed for backend %s", self.name, backend.name))

  self.peers = backend.endpoints

  for _, endpoint_string in ipairs(normalized_endpoints_removed) do
    ngx.shared.balancer_ewma:delete(endpoint_string)
    ngx.shared.balancer_ewma_last_touched_at:delete(endpoint_string)
  end

  local slow_start_ewma = calculate_slow_start_ewma(self)
  if slow_start_ewma ~= nil then
    local now = ngx.now()
    for _, endpoint_string in ipairs(normalized_endpoints_added) do
      store_stats(endpoint_string, slow_start_ewma, now)
    end
  end
end

function _M.new(self, endpoints)
  local o = { peers = endpoints }
  setmetatable(o, self)
  self.__index = self
  return o
end

return _M
