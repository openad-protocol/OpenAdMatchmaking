-- Import required modules
local cjson = require("cjson")
local defalutMsg = require("scripts.error_info")
local redis_data = require("scripts.redis_data")
local nats = require("scripts.nast_publish")
local ipDB = require("core.get_ipaddress_info")
local str_utils = require("utils.str_utils")

local redisData = redis_data:new()
if redisData == nil then
    ngx.say(defalutMsg.generateResponse(10002,"Failed to create Redis object",nil))
    return
end

-- ip数据库初始化
local ipdatabases = ipDB:new()

-- 取得请求参数
local function get_args()
    ngx.req.read_body()  -- 解析 body 参数之前一定要先读取 body
    local args = ngx.req.get_uri_args()
    ngx.log(ngx.DEBUG, "nginx post input params data: " .. cjson.encode(args))

    local data = {
        zoneId = tostring(args.zoneId) or '',
        publisherId = tostring(args.publisherId) or '',
        eventId = tostring(args.eventId) or '',
        -- telegram
        userId = args.userId or '',  -- telegram id
        firstName = args.firstName or '',
        lastName = args.lastName or '',
        userName = args.userName or '',
        -- Line
        channeId = args.channeId or '',
        liffId = args.liffId or '',
        displayName = args.displayName or '',

        language = args.language or '',
        version = args.version or '',
        channel = args.channel or '',
        platform = args.platform or '',
        timeStamp = tostring(ngx.now()),
        location = ngx.encode_base64(args.location or ''),
        traceId = args.traceId or '',
        cb = args.cb or '',
        requestType = "ad_in_call"
    }
    ngx.log(ngx.DEBUG, "Data: " .. cjson.encode(data))
    return data
end

local data = get_args()

if str_utils.checkData(data) == false and data.eventId == "" then
    ngx.say(defalutMsg.generateResponse(10003,"Failed to get request args",nil))
    return
end
-- 验证追踪ID
local ok,err = redisData:updateAdTraceHashElement(data,"cb_advertiser")
if not ok then
    ngx.say(defalutMsg.generateResponse(10002,"Failed to update ad trace hash element",nil))
    return
end

-- 记录统计数据
local ipAddress = ngx.var.http_x_forwarded_for or ngx.var.http_x_real_ip or ngx.var.remote_addr or ''
redisData:dataStatisticsToRedis(data.zoneId,data.eventId,"ad_in_call",ipAddress,data.traceId)


-- 发送消息给后端记录
data.ip_address = ipAddress
data.country = ipdatabases:getCountry(ipAddress) or ''
nats.publisher_message("ad_info.ad_in_call", cjson.encode(data))

-- 是否订阅消息
local publsiherInfo = redisData:getPublisherInfo(data.publisherId)
if publsiherInfo ~= nil then
    if publsiherInfo.sub == 1 then
        local publisherData = {
            publisherId = data.publisherId,
            zoneId = data.zoneId,
            eventId = data.eventId,
            traceId = data.traceId,
            ipAddress = ipAddress,
            country = data.country,
        }
        nats.publisher_message("ad_info.publisherSub", cjson.encode(publisherData))
    end
end

-- 返回成功
ngx.say(defalutMsg.generateResponseLogClick())