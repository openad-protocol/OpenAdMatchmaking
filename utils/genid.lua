math.randomseed(tostring(os.time()):reverse():sub(1, 7))
local http = require "http"
local global = require "global"
local uuid = require "resty.uuid"
local genid = {}



function genid.gentoken(n)
    n = n or 16
    local char = {
        "0","1","2","3","4","5","6","7","8","9",
        "a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z",
        "A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z",
    }
    local s = ""
    for i=1, n do
        local len = math.random(1, #char)
        s = string.format("%s%s", s, char[len])
    end
    return s
end


function genid.gen(red)
    local min = 100001
    local max= 999999
    local script = [[
        local k = ARGV[1]
        local r = ARGV[2]
        local v = redis.call('exists',k)
        if v==1 then
            return nil
        end
        redis.call('set',k,r)
        return r
    ]]
    while true do
        local r = math.random(min,max)
        local k = string.format("%s-useruid-%d",global.get_appname(),r)
        local a = red:eval(script,2,"k","r",k,r)
        if a then
            return r
        end
    end
end


function genid.genuuid()
    return uuid.generate()
end

function genid.genorderid()
    return "WEBDD_"..uuid.generate()
end

function genid.genotherid(red)
    local k = string.format("%s-genotherid",global.get_appname())
    return red:incr(k)
end

return genid
