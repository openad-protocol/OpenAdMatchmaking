local redisc = require "core.redis_client"

-- key为 唯一会话ID
local function get_number(key)
    local sn = redisc.get(key)
    if sn == nil then
        return 0
    end
    n = tonumber(sn)
    if n == nil then
        return 0
    end
    return n
end

-- key为 唯一会话ID
local function set_number(key, number)
    redisc.set(key, number)
end

return {
    get_number = get_number,
    set_number = set_number
}