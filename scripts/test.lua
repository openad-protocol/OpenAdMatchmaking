local redis    = require("core.redis_client")
local m_global = require("utils.global")
local str_utils = require("utils.str_utils")
local cjson = require("cjson")
local rule_filter = require("scripts.rule_filter")


--local red = redis:new()
--local res,err = red:connect()
--if not res then
--    ngx.say("Failed to connect to Redis: ", err)
--    return
--end
--ngx.say("Redis object created host:" .. m_global.get_redis_conf().host .. " port:" .. m_global.get_redis_conf().port)
--
--res,err = red:set("mykey", "myvalue")
--if not res then
--    ngx.say("Failed to set key: ", err)
--    return
--end
--

local args  = {}

local function get_args()
    ngx.req.read_body()  -- 解析 body 参数之前一定要先读取 body
    args = ngx.req.get_uri_args()
    ngx.log(ngx.DEBUG, "nginx post input params data: " .. cjson.encode(args))
end

args = get_args()

local filter = rule_filter:new(true)
local tbString = "{\"dayPvLimit\":1,\"threeSecLimit\":-1,\"singleMaxpv\":1,\"totalMaxPv\":250000,\"weght\":0,\"rule\":\"[{\\\"type\\\":\\\"country\\\",\\\"tags\\\":\\\"NG|BD\\\",\\\"reverse\\\":1},{\\\"type\\\":\\\"language\\\",\\\"tags\\\":\\\"vi\\\",\\\"reverse\\\":0}]\"}"
local data = {}
data.country="CN"
data.language="zh"
local tb = cjson.decode(tbString)
ngx.log(ngx.DEBUG, string.format("tb: %s" ,cjson.encode(tb.rule)))
for k,v in ipairs(cjson.decode(tb.rule)) do
    ngx.log(ngx.DEBUG, string.format("key: %s,value:%s" ,k , cjson.encode(v)))
    local ok = filter:ruleFilter(data,v)
    ngx.log(ngx.DEBUG, string.format("ruleFilter: %s" ,ok))
end

local res = str_utils.splitString("NG|BD","|")

ngx.say(cjson.encode(res))