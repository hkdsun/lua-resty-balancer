require "resty.core"
require "jit.opt".start("minstitch=2", "maxtrace=4000",
                        "maxrecord=8000", "sizemcode=64",
                        "maxmcode=4000", "maxirconst=1000")

local local_dir = arg[1]

-- ngx.say("local dir: ", local_dir)

package.path = local_dir .. "/lib/?.lua;" .. package.path
package.cpath = local_dir .. "/?.so;" .. package.cpath

local base_time

-- should run typ = nil first
local function bench(num, name, func, typ, ...)
    ngx.update_time()
    local start = ngx.now()

    for i = 1, num do
        func(...)
    end

    ngx.update_time()
    local elasped = ngx.now() - start

    if typ then
        elasped = elasped - base_time
    end

    local per_call = elasped * 1000 * 1000 / num

    ngx.say(name)
    ngx.say(num, " times")
    ngx.say("elasped: ", elasped)
    ngx.say("lb time per request: " .. per_call .. " (us)")
    ngx.say("")

    if not typ then
        base_time = elasped
    end
end


local resty_rr = require "resty.roundrobin"
local resty_ewma = require "resty.ewma"

local servers = {
    ["server1"] = 10,
    ["server2"] = 2,
    ["server3"] = 1,
}

local servers2 = {
    ["server1"] = 100,
    ["server2"] = 20,
    ["server3"] = 10,
}

local servers3 = {
    ["server0"] = 1,
    ["server1"] = 1,
    ["server2"] = 1,
    ["server3"] = 1,
    ["server4"] = 1,
    ["server5"] = 1,
    ["server6"] = 1,
    ["server7"] = 1,
    ["server8"] = 1,
    ["server9"] = 1,
    ["server10"] = 1,
    ["server11"] = 1,
    ["server12"] = 1,
}

local backends = {}
local servers4 = {}
for i=1,1000 do
  servers4["server"..i] = 1
  backends[i] = { address = "10.10.10." .. i, port = "8080", maxFails = 0, failTimeout = 0 }
end

print(backends[1])

local rr = resty_rr:new(servers4)
local ewma = resty_ewma:new({ endpoints = backends })
ewma:sync({ endpoints = backends })

local function gen_func(typ)
    local i = 0

    if typ == 0 then
        return function ()
            i = i + 1

            resty_rr:new(servers)
        end
    end

    if typ == 1 then
        return function ()
            i = i + 1

            local servers = {
                ["server1" .. i] = 10,
                ["server2" .. i] = 2,
                ["server3" .. i] = 1,
            }
            local rr = resty_rr:new(servers)
        end
    end

    if typ == 2 then
        return function ()
            i = i + 1

            local servers = {
                ["server1" .. i] = 10,
                ["server2" .. i] = 2,
                ["server3" .. i] = 1,
            }
            local rr = resty_rr:new(servers)
            rr:incr("server3" .. i)
        end, typ
    end

    if typ == 100 then
        return function ()
            i = i + 1
        end
    end

    if typ == 101 then
        return function ()
            i = i + 1

            rr:find(i)
            i = i + 1
        end, typ
    end

    -- Weighted round robin load balancing
    if typ == 102 then
      return function()
          i = i + 1

          -- balance
          local sid = rr:find(i)

          -- after_balance
          local util = math.random()
          rr:set(sid, util)

          i = i + 1
      end, typ
    end

    -- EWMA based load balancing
    if typ == 103 then
      return function()
        i = i + 1

        -- balance
        local upstream_addr = ewma:balance()

        -- after_balance
        ewma:after_balance(math.random(100, 500), upstream_addr)

        i = i + 1
      end, type
    end
end

bench(1000 * 1000, "base for find", gen_func(100))
bench(1000 * 1000, "round_robin with no after_balance", gen_func(101))
bench(100 * 100, "round_robin with after_balance { rr:set(new_weight) }", gen_func(102))
bench(100 * 100, "ewma with resp_time after_balance", gen_func(103))
