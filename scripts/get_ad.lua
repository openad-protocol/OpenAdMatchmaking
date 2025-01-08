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

-- 检查是否在检护模式中
local maintenanceInfo,err =  redisData:checkGuardMode()
if err ~= nil then
    ngx.say(defalutMsg.generateResponse(10002,"Guard mode info error",nil))
    return
end

if maintenanceInfo ~= nil and type(maintenanceInfo) == "table" then
    ngx.log(ngx.DEBUG,"server Maintenance ,please wait...")
    local tp = nil
    local rs = redisData:getDefaultResource(tp)
    ngx.say(defalutMsg.generateResponse(0,"server Maintenance ,please wait... ",rs))
    return
end

-- 取得请求参数
local function get_args()
    ngx.req.read_body()  -- 解析 body 参数之前一定要先读取 body
    local args = ngx.req.get_uri_args()
    ngx.log(ngx.DEBUG, "nginx post input params data: " .. cjson.encode(args))
    local data = {
        zoneId = tostring(args.zoneId) or '',
        publisherId = tostring(args.publisherId) or '',
        eventId = tostring(args.eventId) or '',
        userId = args.userId or '',  -- telegram identifier
        firstName = args.firstName or '',
        lastName = args.lastName or '',
        userName = args.userName or '',
        --timeStamp = tonumber(args.timeStamp) or 0,
        timeStamp = tostring(ngx.now()),
        signature = args.signature or '',
        language = args.language or '',
        version = args.version or '',
        channel = args.channel or '',
        platform = args.platform or '',
        fromType = args.fromType or '',
        location = ngx.encode_base64(args.location or ''),
        traceId = args.traceId or '',
        requestType = "getAd"
    }
    return data
end

local data = get_args()
if str_utils.checkData(data) == false then
    ngx.say(defalutMsg.generateResponse(10003,"Failed to get request args",nil))
    return
end

ngx.log(ngx.DEBUG, "Data: " .. cjson.encode(data))

local ipAddress = ngx.var.http_x_forwarded_for or ngx.var.http_x_real_ip or ngx.var.remote_addr or ''
data.ip_address = ipAddress
-- 给请求数据添加IP地址和国家
local ipdatabases = ipDB:new()
data.country = ipdatabases:getCountry(ipAddress) or ''

local threeIpPv ,err = redisData:getThreeIpPv("getAd",ipAddress)
if threeIpPv > 30 then
    ngx.say(defalutMsg.generateResponse(10002,"Failed to get three ip pv",nil))
    return
end

ngx.log(ngx.DEBUG,string.format("data:%s,ipAddress:%s",cjson.encode(data),ipAddress))
-- 取得有效事件
local eventId,eventData = redisData:getEventId(data,data.zoneId,data.publisherId)
if eventId == nil or eventData == nil then
    ngx.log(ngx.DEBUG,"no ads available")
    -- 记录无效访问
    nats.publisher_message("ad_info.get_ad_miss",cjson.encode(data))
    ngx.say(defalutMsg.generateResponse(10002,"no ads available",nil))
    return
end

if eventId == "" then
    ngx.say(defalutMsg.generateResponse(10003,"no ads available",nil))
    return
end

data.eventId = eventId
ngx.log(ngx.DEBUG,"event id:",eventId,"    event data:",cjson.encode(eventData))

local adResourceId,adResourceData
adResourceId,adResourceData = redisData:getEventResource(data.eventId)
if adResourceId == nil or adResourceData == nil then
    -- 返回默认数据
    ngx.say(redisData:getDefaultDataFromRedis())
    return
end

local adResourceHash = cjson.encode(adResourceData)
ngx.log(ngx.DEBUG,"adResourceId:",adResourceId," adResourceData:",adResourceHash)
-- 追踪hash
local loginfo_hash,clickinfo_hash,cb_hash = redisData:setAdTracerHashAll(data.zoneId,data.publisherId,data.traceId,tostring(adResourceHash))
local adResourceDataTable = cjson.decode(adResourceData)
adResourceDataTable.signature = loginfo_hash
adResourceDataTable.hash = clickinfo_hash
adResourceDataTable.cb = cb_hash
ngx.log(ngx.DEBUG,"eventId:",eventId," data.eventId:",data.eventId)
adResourceDataTable.eventId = eventId
-- adResourceData.eventId = data.eventId
local tracertHash = {
    trace_id = data.traceId,
    event_id = data.eventId,
    loginfo_hash = loginfo_hash,
    clickinfo_hash = clickinfo_hash,
    cb_hash = cb_hash
}

local sendNatsStr = cjson.encode(tracertHash)
ngx.log(ngx.DEBUG,"send ad_info.traceHash to nats:",sendNatsStr)
nats.publisher_message("ad_info.tracerHash", sendNatsStr)  -- 保存追踪hash
ngx.log(ngx.DEBUG,"response to client success")

data.cb = cb_hash

local sendNatsStrGetAd = cjson.encode(data)
ngx.log(ngx.DEBUG,"send data to nats :",sendNatsStrGetAd)

-- 发送消息给后端记录
nats.publisher_message("ad_info.get_ad", sendNatsStrGetAd)

-- 记录统计数据
local ok = redisData:dataStatisticsToRedis(data.zoneId,data.eventId,"getAd",ipAddress,data.traceId)
if ok then
    ngx.log(ngx.DEBUG,"dataStatisticsToRedis success")
else
    ngx.log(ngx.DEBUG,"dataStatisticsToRedis failed")
end


redisData:addThreeIpPv(data.zoneId,"getAd",data.ip_address,data.traceId)
ngx.say(defalutMsg.generateResponse(0,"User click success",adResourceDataTable))