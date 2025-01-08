-- redis.lua
local cjson = require("cjson")
local redis = require("libs.redis")
local global = require("utils.global")

local RedisClient = {}
RedisClient.__index = RedisClient

function RedisClient:new(host, port, t)
    local instance = setmetatable({}, RedisClient)
    instance.host = global:get_redis_conf().host or "localhost"
    instance.port = global:get_redis_conf().port or 6379
    instance.timeout = global:get_redis_conf().timeout or 1000
    instance.ssl = global:get_redis_conf().ssl or false
    instance.redis = redis:new()
    ngx.log(ngx.INFO, "Redis host: ", instance.host, " port: ", instance.port)
    return instance
end

function RedisClient:connect()
    ngx.log(ngx.INFO, "Redis host: ", self.host, " port: ", self.port)
    if not self.host or not self.port then
        ngx.log(ngx.ERR, "Host and port must be specified")
        return nil, "Host and port must be specified"
    end    
    if self.redis then
        self.redis:set_timeout(self.timeout)
    else
        return false, "Failed to create Redis client"
    end
    local ok,err
    ngx.log(ngx.INFO, "Redis not ssl connect host: ", self.host, " port: ", self.port)
    if not self.ssl then
        ok, err = self.redis:connect(self.host, self.port)
    else
        ok, err = self.redis:connect(self.host, self.port, {ssl = self.ssl})
    end
    --ok, err = self.redis:connect(self.host, self.port)

    if not ok then
        ngx.log(ngx.ERR, "Failed to connect to Redis: ", err)
        return false, err
    end

    -- local ok, err = self.redis:connect(self.host, self.port)
    if not ok then
        ngx.log(ngx.ERR, "Failed to connect to Redis: ", err)
        return false, err
    end
    return true, ""
end

function RedisClient:close()
    if not self.redis then
        return true
    end
    local ok, err = self.redis:close()
    if not ok then
        return nil, err
    end
    return true
end

--[[
    Function: get

    Description:
    This function retrieves the specified data from the system.

    Parameters:
    - key (string): The key used to identify the data.
    Returns:
    - value (any): The retrieved data associated with the specified key.
    - error (string): The error message if the operation fails.
]]
function RedisClient:get(key)
    if not self.redis then
        return nil, "Not connected to Redis"
    end
    local res, err = self.redis:get(key)
    if not res then
        return nil, err
    end
    return res,nil
end

--[[
    Function: set

    Description:
    This function retrieves the specified data from the system.

    Parameters:
    - key (string): The key used to identify the data.
    - value (any): The data to be stored.
    Returns:
    - value (boolean): The retrieved data associated with the specified key.
    - error (string): The error message if the operation fails.
]]
function RedisClient:set(key, value)
    if not self.redis then
        return false, "Not connected to Redis"
    end
    local res, err = self.redis:set(key, value)
    if not res then
        return false, err
    end
    return true, nil
end

function RedisClient:exists(key)
    if not self.redis then
        return nil, "Not connected to Redis"
    end
    local res, err = self.redis:exists(key)
    if not res then
        return nil, err
    end
end

function RedisClient:set_expire(key, t)
    if not self.redis then
        return nil, "Not connected to Redis"
    end
    local res, err = self.redis:expire(key, t)
    if not res then
        return nil, err
    end
    return true
end

function RedisClient:pfadd(key, ...)
    if not self.redis then
        return nil, "Not connected to Redis"
    end
    local res, err = self.redis:pfadd(key, ...)
    if not res then
        return nil, err
    end
    return res,nil
end

function RedisClient:pfcount(key,...)
    if not self.redis then
        return nil, "Not connected to Redis"
    end
    local res, err = self.redis:pfcount(key,...)
    if not res then
        return nil, err
    end
    return res,nil
end

function RedisClient:hget(key,field,...)
    if not self.redis then
        return nil, "Not connected to Redis"
    end
    local res, err = self.redis:hget(key,field,...)
    if not res then
        return nil, err
    end
    return res,nil
end

function RedisClient:hmget(key,field,...)
    if not self.redis then
        return nil, "Not connected to Redis"
    end
    local res, err = self.redis:hmget(key,field,...)
    if not res then
        return nil, err
    end
    return res,nil
end

function RedisClient:hset(key, field, value, ...)
    if not self.redis then
        return nil, "Not connected to Redis"
    end
    local res, err = self.redis:hset(key,field, value)
    if not res then
        return nil, err
    end
    return res,nil
end

function RedisClient:hmset(key, value,...)
    if not self.redis then
        return nil, "Not connected to Redis"
    end
    local res, err = self.redis:hmset(key, value,...)
    if not res then
        return nil, err
    end
    return res,nil
end

function RedisClient:hdel(key, field,...)
    if not self.redis then
        return nil, "Not connected to Redis"
    end
    local res, err = self.redis:hdel(key, field,...)
    if not res then
        return nil, err
    end
    return res,nil
end

function RedisClient:hkeys(key)
    if not self.redis then
        return nil, "Not connected to Redis"
    end
    local res, err = self.redis:hkeys(key)
    if not res then
        return nil, err
    end
    return res,nil
end

-- -- 编写 Lua 脚本
-- local script = [[
--     local keys = redis.call('HKEYS', KEYS[1])
--     local result = {}
--     for i, key in ipairs(keys) do
--         if string.sub(key, 1, string.len(ARGV[1])) == ARGV[1] then
--             table.insert(result, key)
--         end
--     end
--     return result
-- ]]


-- redis中执行脚本
function RedisClient:eval(script,  ...)
    if not self.redis then
        return nil, "Not connected to Redis"
    end
    -- script 是你要执行的 Lua 脚本。
    -- arg1, arg2, ... 是要传递的额外参数（非键）。
    local numKeys = select('#',...)
    local res, err = self.redis:eval(script, numKeys, ...)
    if not res then
        return nil, err
    end
    return res,nil
end

function RedisClient:hgetall(key)
    if not self.redis then
        return nil, "Not connected to Redis"
    end
    local res, err = self.redis:hgetall(key)
    ngx.log(ngx.DEBUG,string.format("redis hgetall data:%s",cjson.encode(res)))

    if not res then
        return nil, err
    end
    return res,nil
end

function RedisClient:hgetKeyValue(key)
    local res, err = self:hgetall(key)
    ngx.log(ngx.DEBUG,string.format("redis client keys %s,hgetKeyValue: %s",key,cjson.encode(res)))
    if res==nil then
        return nil, err
    end
    local result = {}
    for i = 1, #res, 2 do
        result[res[i]] = res[i + 1]
    end
return result, nil
end

function RedisClient:incr(key)
    local res,err = self.redis:incr(key)
    ngx.log(ngx.DEBUG,string.format("redis client incr: %s",cjson.encode(res)))
    if res == nil then
        return nil,err
    end
    return res,err
end



return RedisClient
