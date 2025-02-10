local red = require("core.redis_client")
local defalutMsg = require("scripts.error_info")
local m_global = require("utils.global")
local cjson = require("cjson")
local str_utils = require("utils.str_utils")
local sys_utils = require("utils.system")
local rule_filter = require("scripts.rule_filter")



local RedisData = {}
RedisData.__index = RedisData

-- 构造函数
function RedisData:new()
    local instance = setmetatable({}, RedisData)
    local redis = red:new()
    local filter = rule_filter:new()
    if redis == nil then
        ngx.log(ngx.DEBUG,"Failed to create Redis object")
        return nil
    end
    local ok, err = redis:connect()
    if not ok then
        ngx.log(ngx.DEBUG,"Failed to connect to Redis: ", err)
        return nil
    end
    -- todo: 取过滤器方法 and 或是 or 或者是 not 
    instance.redis = redis
    instance.filter = filter
    instance.useWeight = true
    return instance
end

-- 关闭连接
function RedisData:close()
    local ok, err = self.redis:set_keepalive(10000, 100)
    if not ok then
        ngx.log(ngx.DEBUG, "Failed to set keepalive: ", err)
        return nil, err
    end
    self.redis:close()
    return true
end


-- 设置数据到Redis
function RedisData:hset(key,field,value)
    local res,err = self.redis:hset(key,field,value)
    if res == nil then
        ngx.log(ngx.DEBUG, "Failed to set data to Redis: ", err)
        return false,"Failed to set data to Redis"
    end
    return true,""
end

-- 取得数据从Redis
function RedisData:hget(key,field)
    local res,err = self.redis:hget(key,field)
    if res == nil then
        ngx.log(ngx.DEBUG, "Failed to set data to Redis: ", err)
        return res,"Failed to set data to Redis"
    end
    return res,""
end

function RedisData:checkGuardMode()
    local key = string.format("%s:global:maintenance",m_global:get_appname())
    ngx.log(ngx.DEBUG,"key is :",key)
    local res ,err = self.redis:get(key)
    if err ~=nil or res == nil or res == cjson.null then
        ngx.log(ngx.DEBUG, "Failed to get GuardMode from Redis: ", err)
        return nil,err
    end
    ngx.log(ngx.DEBUG,"get GuardMode success,return :",cjson.encode(res))
    return cjson.decode(res),nil
end

-- 增加用户来访计数 appname:zoneId:eventId:session value
-- @param zoneId 区域id
-- @param eventId 事件id
-- @param session 对应前端的traceId
-- @return boolean 是否成功
function RedisData:addSession(zoneId,eventId,type,session)
    local key = m_global:get_appname().. ":zoneId" .. zoneId..":eventId" .. eventId ..":type"  .. type..":session".. session
    local singlekey = m_global:get_appname().. ":type"  .. type..":session".. session

    local res,err = self.redis:incr(key)
    local singleRes,err = self.redis:incr(singlekey)
    if res == nil or res == 0 or singleRes== nil or singleRes == 0 then
        ngx.log(ngx.DEBUG, "Failed to add session to Redis: ", err)
        return false
    end
    ngx.log(ngx.DEBUG,"session number:",res)
    if res == 1 then
        local res,err = self.redis:set_expire(key,m_global:get_statistics_expire())
        if res == nil then
            ngx.log(ngx.DEBUG, "Failed to set expire to Redis: ", err)
            return false
        else 
            ngx.log(ngx.DEBUG,"set expire success")
        end
    end
    if singleRes == 1 then
        local res,err = self.redis:set_expire(singlekey,m_global:get_statistics_expire())
        if res == nil then
            ngx.log(ngx.DEBUG, "Failed to set expire to Redis: ", err)
            return false
        else 
            ngx.log(ngx.DEBUG,"set expire success")
        end
    end
    return true
end


-- 取得用户来访计数 appname:zoneId:eventId:session value
-- @param zoneId 区域id
-- @param eventId 事件id
-- @param session 对应前端的traceId
-- @return res,err 计数 和 错误信息
function RedisData:getSession(zoneId,eventId,type,session)
    local key = m_global:get_appname() .. ":zoneId" .. zoneId..":eventId" .. eventId ..":type" .. type .. ":session" .. session
    local singlekey = m_global:get_appname().. ":type"  .. type..":session".. session

    local res,err = self.redis:get(key)
    if res == nil then
        ngx.log(ngx.ERR, "Failed to get session from Redis: ", err)
        return 0,0
    end
    local singleRes,err = self.redis:get(singlekey)
    if singleRes == nil then
        ngx.log(ngx.ERR, "Failed to get session from Redis: ", err)
        return 0,0
    end
    return res,singleRes
end

function RedisData:getDefaultResource(tp)
    local key = string.format("%s:defaultResource",m_global:get_appname())

    if tp ~= nil then
        key = key .. ":" .. tp
    end
    local defaultResource ,err= self.redis:get(key)
    if err ~=nil  then
        ngx.log(ngx.DEBUG, "Failed to get defaultResource from Redis: ", err)
        return nil
    end
    if defaultResource ~= nil and defaultResource == cjson.null then
        ngx.log(ngx.DEBUG, "Failed to get defaultResource from Redis: ", err)
        return nil
    end
    return cjson.decode(defaultResource)
end


function RedisData:addEventCalc(zoneId,eventId,tp,traceId)
    local singleKey = m_global:get_appname() .. ":event:" .. eventId ..":" .. tp .. ":singleMaxpv:" .. traceId -- 按traceId计数
    local eventMaxUvKey = m_global:get_appname() .. ":event:" .. eventId .. ":".. tp .. ":eventMaxUv"  -- 按eventId计数
    local eventMaxPvKey = m_global:get_appname() .. ":event:" .. eventId .. ":".. tp .. ":eventMaxPv"  -- 按eventId计数

    -- 单个tracerId的key
    ngx.log(ngx.DEBUG,"singleKey is :",singleKey,"  eventMaxUvKey is :",eventMaxUvKey)

    local skn,err = self.redis:incr(singleKey)
    if skn == nil then
        ngx.log(ngx.DEBUG, string.format("add Event Calc singleKey Failed : %s", err))
        return false
    end
    self.redis:set_expire(singleKey,m_global:get_statistics_expire())

    -- event的uv计数
    local uvn,err = self.redis:pfadd(eventMaxUvKey,traceId)
    if uvn == nil then
        ngx.log(ngx.DEBUG, string.format("add Event Calc eventMaxUvKey Failed: %s", err))
        return false
    end

    -- event的pv计数
    local pvn,err = self.redis:incr(eventMaxPvKey)
    if pvn == nil then
        ngx.log(ngx.DEBUG, string.format("add Event Calc eventMaxPvKey Failed: %s", err))
        return false
    end

    ngx.log(ngx.DEBUG,string.format("addEventCalc success zoneId:%s,singleNumber :%s,eventNumber:%s",zoneId,skn,uvn))
    return true
end

function RedisData:getEventCalc(zoneId,eventId,tp,traceId)
    local singleKey = m_global:get_appname() .. ":event:" .. eventId ..":" .. tp .. ":singleMaxpv:" .. traceId -- 按traceId计数
    local eventMaxUvKey = m_global:get_appname() .. ":event:" .. eventId .. ":".. tp .. ":eventMaxUv"  -- 按eventId计数
    local eventMaxPvKey = m_global:get_appname() .. ":event:" .. eventId .. ":".. tp .. ":eventMaxPv"  -- 按eventId计数
    ngx.log(ngx.DEBUG,
        string.format("singleKey is:%s eventMaxUvKey is:%s eventMaxPvKey is:%s",
        singleKey,eventMaxUvKey,eventMaxPvKey)
     )
    -- event的pv计数
    local eventPv,err = self.redis:get(eventMaxPvKey)
    if err ~= nil then
        ngx.log(ngx.DEBUG, "Failed to get event calc from Redis: ", err)
        return 0,0,0
    end
    if eventPv == nil or eventPv == cjson.null then
        eventPv = 0
    end
    -- 单个tracerId的key
    local singleNumber,err = self.redis:get(singleKey)
    if err ~= nil then
        ngx.log(ngx.DEBUG, "Failed to get event calc from Redis: ", tp)
        return 0,0,tonumber(eventPv)
    end
    if singleNumber == nil or singleNumber == cjson.null then
        singleNumber = 0
    end
    -- event的uv计数
    local eventNumber,err = self.redis:pfcount(eventMaxUvKey)
    if err ~= nil then
        ngx.log(ngx.DEBUG, "Failed to get event calc from Redis: ", err)
        return tonumber(singleNumber),0,tonumber(eventPv)
    end
    if eventNumber == nil or eventNumber == cjson.null then
        eventNumber = 0
    end
    ngx.log(ngx.DEBUG,"zondId:",zoneId," eventId:",eventId," singleNumber:",singleNumber,"  eventNumber:",eventNumber,"  eventPv:",eventPv)
    return tonumber(singleNumber),tonumber(eventNumber),tonumber(eventPv)
end

-- 增加用户来访计数 appname:zoneId:eventId:session values
-- 同一个traceId只能增加一次
-- @param zoneId 区域id
-- @param eventId 事件id
-- @param session 对应前端的traceId
-- @return boolean 是否成功
function RedisData:addZoneEventIdUv(zoneId,eventId,type,traceId)
    local key = m_global:get_appname() .. ":zoneId" .. zoneId..":eventId" .. eventId .. ":type" .. type .. ":uv"
    local res,err = self.redis:pfadd(key, traceId)
    if res == nil then
        ngx.log(ngx.DEBUG, "Failed to add session_uv to Redis: ", err)
        return false
    end
    if res == 1 then
        local res,err = self.redis:set_expire(key, m_global:get_statistics_expire())
        if res == nil then
            ngx.log(ngx.DEBUG, "Failed to set expire to Redis: ", err)
            return false
        else 
            ngx.log(ngx.DEBUG,"set expire success")
        end
    end
    return true
end

-- 取得用户来访计数 appname:zoneId:eventId:session values
-- @param zoneId 区域id
-- @param eventId 事件id
-- @param session 对应前端的traceId
function RedisData:getZoneEventIdUv(zoneId,eventId,type)
    local key = m_global:get_appname() ..":zoneId" .. zoneId..":eventId" .. eventId .. ":type" .. type .. ":uv"
    local res,err = self.redis:pfcount(key)
    if res == nil then
        ngx.log(ngx.DEBUG, "Failed to get session_uv from Redis: ", err)
        return 0,err
    end
    return res,""
end

-- 增加用户来访计数 appname:zoneId:eventId values
-- 广告被点击就增加
-- @param zoneId 区域id
-- @param eventId 事件id
-- @return boolean 是否成功
function RedisData:addZoneEventIdPv(zoneId,eventId,tp)
    local key = m_global:get_appname() ..":zoneId" .. zoneId..":eventId" .. eventId .. ":type" .. tp .. ":pv"
    local res,err = self.redis:incr(key)
    if res == nil then
        ngx.log(ngx.DEBUG, "add zone event id pv failed from redis: ", err)
        return false
    end
    if res == 1 then
        local res,err = self.redis:set_expire(key, m_global:get_statistics_expire())
        if res == nil then
            ngx.log(ngx.DEBUG, "Failed to set expire to Redis: ", err)
            return false
        else 
            ngx.log(ngx.DEBUG,"set expire success")
        end
    end
end


function RedisData:getZoneEventIdPv(zoneId,eventId,tp)
    local key = m_global:get_appname() ..":zoneId" .. zoneId..":eventId" .. eventId .. ":type" .. tp .. ":pv"
    local res,err = self.redis:get(key)
    if res == nil  then
        ngx.log(ngx.DEBUG, "get zone event id pv failed from redis:", err)
        return 0,err
    end
    return res,""
end

-- 取得默认返回数据
function RedisData:getDefaultDataFromRedis()
    -- todo: 从redis中取得默认数据
    local data = {}
    return defalutMsg.generateResponse(10004,"No data found",data)
end

-- 取得Redis数据
function RedisData:getDataFromRedis(zoneId,eventId)
    ad_unid = m_global:get_appname() .. ":zoneId" ..zoneId .. ":eventId" .. eventId
    -- 取得计数
    local adCount, err = self.redis:hget(ad_unid, "adCount")
    if not adCount then
        ngx.log(ngx.DEBUG, "Failed to get ad count from Redis: ", err)
        return nil
    end
    if adCount == 0 then -- 广告展示次数为0，返回默认数据
        return self:getDefaultDataFromRedis()
    end
    local jsonData, err = self.redis:hget(ad_unid, "data")
    if not jsonData then
        ngx.log(ngx.DEBUG, "Failed to get data from Redis: ", err)
        return nil
    end
    ngx.log(ngx.DEBUG, "Data from Redis: " .. jsonData)

    -- 计数减一
    local res, err = self.redis:hset(ad_unid, "adCount", adCount - 1)
    if not res then
        ngx.log(ngx.DEBUG, "Failed to set ad count to Redis: ", err)
        return nil
    end
    -- 返回数据
    local result = cjson.decode(jsonData)
    return result
end

-- 根据eventId增加IP地址UV
function RedisData:addIpAddrUv(tp,ip,traceId)
    --local key = m_global:get_appname() .. ":ip1duv" .. ":type" .. tp .. ":" .. ip
    local key = string.format("%s:global:ip1dayuv:%s:%s",m_global:get_appname(),tp,ip)
    -- 检查 key是否存在,存在的不重设过期时间
    local res,err = self.redis:pfcount(key)
    local newCreate = false
    if res == nil or tonumber(res) == 0 or res == cjson.null then
        newCreate =true
    end
    -- 增加IP地址UV
     res,err = self.redis:pfadd(key,traceId)
    if  err ~= nil then
        ngx.log(ngx.DEBUG, "Failed to add ip to Redis: ", err)
        return false
    end
    -- 新创建设置过期
    if newCreate then
        res,err = self.redis:set_expire(key,m_global:get_statistics_expire())
        if res == nil then
            ngx.log(ngx.DEBUG, "Failed to set expire to Redis: ", err)
            return false
        end
    end
    ngx.log(ngx.DEBUG,"set expire success")
    return true
end

-- 取得IP地址ZoneId下所有eventId的总共UV
function RedisData:getIpAddrUv(tp,ip)
    --local key = m_global:get_appname() .. ":ip1duv" .. ":type" .. tp .. ":" .. ip
    local key = string.format("%s:global:ip1dayuv:%s:%s",m_global:get_appname(),tp,ip)
    local res,err = self.redis:pfcount(key)
    if err ~=nil  or res == nil or res==cjson.null then
        ngx.log(ngx.DEBUG, "Failed to get ip from Redis: ", err)
        return 0,err
    end
    return tonumber(res),""
end

-- 增加IP地址PV
function RedisData:addIpAddrPv(tp,ip)
    local key = string.format("%s:global:ip1day:%s:%s",m_global:get_appname(),tp,ip)
    local res,err = self.redis:incr(key)
    if res == nil then
        ngx.log(ngx.DEBUG, "Failed to add ip to Redis: ", err)
        return false
    end
    if res == 1 then
        res,err = self.redis:set_expire(key,m_global:get_statistics_expire())
        if res == nil then
            ngx.log(ngx.DEBUG, "Failed to set expire to Redis: ", err)
            return false
        else 
            ngx.log(ngx.DEBUG,"set expire success")
        end
    end
end

-- 取得IP地址PV
function RedisData:getIpAddrPv(tp,ip)
    local key = string.format("%s:global:ip1day:%s:%s",m_global:get_appname(),tp,ip)
    local res,err = self.redis:get(key)
    if res == nil then
        ngx.log(ngx.DEBUG, "Failed to get ip from Redis: ", err)
        return 0,err
    end
    return tonumber(res),""
end

-- 取得全局规则
function RedisData:getGlobalIPRule()
    local key = string.format("%s:dictionary:GlobalRule",m_global:get_appname())
    local res,err = self.redis:hgetKeyValue(key)
    if err ~= nil or res == "" or type(res) == cjson.null then
        ngx.log(ngx.DEBUG, "Failed to get rule from Redis: ", err)
        return nil,err
    end
    ngx.log(ngx.DEBUG,"getGlobalIPRule success:",cjson.encode(res))
    local globalRule = res
    if globalRule == nil or type(globalRule) ~= "table" then
        ngx.log(ngx.DEBUG, "Failed to get rule from Redis: ", err)
        return nil,err
    end
    -- 返回基于IP地址记录的dayPvLimit和threeSecLimit配置
    return globalRule,nil
end

-- 取得用户最大的PV/UV
function RedisData:getUserMax(zoneId,eventId)
    local key = m_global:get_appname() .. ":" .. zoneId..":" .. eventId
    local maxPv,err = self.redis:hget(key,"max_pv")
    if maxPv == nil then
        ngx.log(ngx.DEBUG, "Failed to get user max_pv from Redis: ", err)
        return 0,0,err
    end
    local maxUv,err = self.redis:hget(key,"max_uv")
    if maxUv == nil then
        ngx.log(ngx.DEBUG, "Failed to get user max_uv from Redis: ", err)
        return 0,0,err
    end
    return maxPv,maxUv,""
end

function RedisData:subUserMax(zoneId,eventId)
    local key = m_global:get_appname() .. ":" .. zoneId..":" .. eventId
    local maxPv,maxUv,err = self:getUserMax(zoneId,eventId)
    local res,err = self.redis:hset(key,"max_pv",maxPv -1)
    if res == nil then
        ngx.log(ngx.DEBUG, "Failed to set user max_pv to Redis: ", err)
        return false
    end
    local rule err = self.redis:hset(key,"max_uv",maxUv -1)
    if rule == nil then
        ngx.log(ngx.DEBUG, "Failed to set user max_uv to Redis: ", err)
        return false
    end

    return true
end


function RedisData:dataStatisticsToRedis(zoneId,eventId,tp,ip,traceId)
    if zoneId == nil and eventId == nil and tp == nil or traceId == nil then
        ngx.log(ngx.DEBUG, "request data is nil")
        return false
    end
    self:addSession(zoneId,eventId,tp,traceId)
    self:addZoneEventIdUv(zoneId,eventId,tp,traceId)
    self:addZonePvUv(zoneId,tp)
    self:addZoneEventIdPv(zoneId,eventId,tp)
    -- 以下两个是全局的
    self:addIpAddrPv(tp,ip)   -- 记录事件的IP PV数
    self:addIpAddrUv(tp,ip,traceId)  -- 记录事件的IP UV数
    -- 只对eventId有效
    self:addEventCalc(zoneId,eventId,tp,traceId)
    return true
end

function RedisData:addZonePvUv(zoneId,tp,traceId)
    local pvKey = m_global:get_appname() .. ":zoneId" .. zoneId.. ":type" .. tp .. ":pv"
    local uvKey = m_global:get_appname() .. ":zoneId" .. zoneId.. ":type" .. tp .. ":uv"

    ngx.log(ngx.DEBUG,"key is :",pvKey)
    local pvRes,err = self.redis:incr(pvKey)
    if pvRes == nil then
        ngx.log(ngx.DEBUG, "Failed to add ip to Redis: ", err)
        return false
    end
    if pvRes == 1 then
        local res,err = self.redis:set_expire(pvKey,m_global:get_statistics_expire())
        if res == nil then
            ngx.log(ngx.DEBUG, "Failed to set expire to Redis: ", err)
            return false
        else 
            ngx.log(ngx.DEBUG,"set expire success")
        end
    end
    ngx.log(ngx.DEBUG,"key is :",uvKey)
    local pvRes,err = self.redis:pfadd(uvKey,traceId)
    if pvRes == nil then
        ngx.log(ngx.DEBUG, "Failed to add ip to Redis: ", err)
        return false
    end
    if pvRes == 1 then
        local res,err = self.redis:set_expire(uvKey,m_global:get_statistics_expire())
        if res == nil then
            ngx.log(ngx.DEBUG, "Failed to set expire to Redis: ", err)
            return false
        else 
            ngx.log(ngx.DEBUG,"set expire success")
        end
    end

    return true
end

function RedisData:getZonePvUv(zoneId,tp)
    local pvKey = m_global:get_appname() .. ":zoneId" .. zoneId.. ":type" .. tp .. ":pv"
    local uvKey = m_global:get_appname() .. ":zoneId" .. zoneId.. ":type" .. tp .. ":uv"
    ngx.log(ngx.DEBUG,"key is :",pvKey)
    local pvRes,err = self.redis:get(pvKey)
    if pvRes == nil then
        ngx.log(ngx.DEBUG, "Failed  get ip pv from Redis: ", err,"  data:",pvRes)
        return 0,0
    end
    local uvRes,err = self.redis:pfcount(uvKey)
    if uvRes == nil then
        ngx.log(ngx.DEBUG, "Failed get ip uv to Redis: ", err,"  data:",uvRes)
        return 0,0
    end
    return tonumber(pvRes),tonumber(uvRes)
end

function RedisData:addThreeIpUv(zoneId,tp,ip,traceId)
    local key = m_global:get_appname() .. ":zoneId" .. zoneId.. ":type" .. tp .. "30s:ippv" .. ip
    ngx.log(ngx.DEBUG,"key is :",key)
    local res,err = self.redis:pfadd(key,traceId)
    if res == nil then
        ngx.log(ngx.DEBUG, "Failed to add ip to Redis: ", err)
        return false
    end
    if res == 1 then
        local res,err = self.redis:set_expire(key, m_global:get_three_statics_expire())
        if res == nil then
            ngx.log(ngx.DEBUG, "Failed to set expire to Redis: ", err)
            return false
        else 
            ngx.log(ngx.DEBUG,"set expire success")
        end
    end
    return true
end

function RedisData:getThreeIpUv(zoneId,tp,ip)
    local key = m_global:get_appname() .. ":zoneId" .. zoneId.. ":type" .. tp .. "30s:ippv" .. ip
    ngx.log(ngx.DEBUG,"key is :",key)
    local res,err = self.redis:pfcount(key)
    if res == nil then
        ngx.log(ngx.ERR, "Failed to add ip to Redis: ", err)
        return 0,err
    end
    return tonumber(res),err
end

function RedisData:addThreeIpPv(tp,ip)
    local key =string.format("%s:global:ip30s:%s:%s",m_global:get_appname(),tp,ip)
    ngx.log(ngx.DEBUG,string.format("add ThreeIpPv:%s,key is :",key))
    local res,err = self.redis:incr(key)
    if res == nil then
        ngx.log(ngx.DEBUG, "Failed to add ip to Redis: ", err)
        return false
    end
    if tonumber(res) == 1 then
        res,err = self.redis:set_expire(key, m_global:get_three_statics_expire())
        if err ~= nil  or res == nil or res == cjson.null then
            ngx.log(ngx.DEBUG, "Failed to set expire to Redis: ", err)
            return false
        else
            ngx.log(ngx.DEBUG,"set expire success")
        end
    end
    return true
end

function RedisData:getThreeIpPv(tp,ip)
    local key =string.format("%s:global:ip30s:%s:%s",m_global:get_appname(),tp,ip)
    local res,err = self.redis:get(key)
    if err ~=nil or res == nil or res == cjson.null then
        return 0,err
    end
    return tonumber(res),err
end

function RedisData:whiteList(publisherId)
    -- local key = string.format("%s:publisherId:%s:whiteList",m_global:get_appname(),publisherId)
    -- local res,err = self.redis:smembers(key)
end

function RedisData:getEventId(data,zoneId,publisherId)
    -- 全局规则判断
    local globalDayIpPvNumber = self:getIpAddrUv("loginfo",data.ip_address)
    local globalThreeNumber = self:getThreeIpPv("loginfo",data.ip_address)
    if globalDayIpPvNumber ~= nil and globalThreeNumber ~=nil then
        -- 取IP取则
        local limitNumber = 0
        local limit30Number = 0
        local ipRule,err =self:getGlobalIPRule()
        if err ==nil and  ipRule ~= nil then
            limitNumber =tonumber(ipRule.dayPvLimit)
            limit30Number = tonumber(ipRule.threeSecLimit)
            -- 日全局判定
            if (globalDayIpPvNumber > limitNumber) or (globalThreeNumber > limit30Number)  then
                ngx.log(ngx.DEBUG,"global rule failed")
                return nil,nil
            end
        end
    end

    local key = string.format("%s:zone:%s:%s",m_global:get_appname() ,zoneId , publisherId)
    ngx.log(ngx.DEBUG,string.format("key: %s  data: %s:",key,cjson.encode(data)))
    local eventTable,err = self.redis:hgetKeyValue(key)  -- 对象是一个表
    if eventTable == nil or eventTable == cjson.null or type(eventTable) ~= "table" then
        ngx.log(ngx.ERR, string.format("Failed to get event_id from Redis: %s", err))
        return nil,err
    end
    -- redis 取得权重如果没有，则重新计算
    local weight_key = string.format("%s:zone:%s:%s:weight",m_global:get_appname(),zoneId,publisherId)
    local weight,err = self.redis:get(weight_key)
    if err ~= nil or weight == nil or weight == cjson.null then
        ngx.log(ngx.DEBUG, string.format("Failed to get key:%s weight from Redis null",weight_key))
        weight = 5
        -- return nil,err
    end
    ngx.log(ngx.DEBUG,string.format("weight:%s",weight))
    local totalWeight = tonumber(weight) -- 计算总权重
    local eventArry = sys_utils.tableToArry(eventTable)
    ngx.log(ngx.DEBUG,string.format("EventTable:%s totalWeight:%s eventArry:%s",cjson.encode(eventTable),totalWeight,cjson.encode(eventArry)))
    -- 取得 zoneId 和 publisherId 的规则
    local zoneKey = string.format("%s:rule",key)
    ngx.log(ngx.DEBUG,zoneKey)
    local rule,err = self.redis:get(zoneKey)
    local zoneRule = cjson.decode(rule)

    -- 取30秒内pv
    local threeSecNumber = 0
    local err = nil
    -- 取日的PV/UV数
    local    dayZonePvNumber ,dayZoneUvNumber= 0,0
    -- todo: 从redis取得zone的日PV/UV数
    -- todo: end
    ngx.log(ngx.DEBUG,string.format("dayZonePvNumber:%s dayZoneUvNumber:%s",dayZonePvNumber, dayZoneUvNumber))
    -- 取IP的日PV
    local dayPvNumber = 0
    ngx.log(ngx.DEBUG,string.format("dayPvNumber: %s",dayPvNumber))
    -- 全局策略超过限量
    local dayZonePvLimit = zoneRule.dayZonePvLimit or 100000
    local dayZoneUvLimit = zoneRule.dayZoneUvLimit or 100000
    local dayPvLimit = zoneRule.dayPvLimit or 100000
    
    ngx.log(ngx.DEBUG,string.format("dayZonePvLimit:%s dayZoneUvLimit:%s dayPvLimit:%s",dayZonePvLimit,dayZoneUvLimit,dayPvLimit))
    if dayZonePvLimit <0 or dayZoneUvLimit <0 or dayPvLimit <0 then
        else if dayZonePvNumber > dayZonePvLimit or dayZoneUvNumber > dayZoneUvLimit or dayPvNumber > dayPvLimit then
            return nil,nil
        end
    end

    -- 全局策略配置为空，或是小于零，直接通过
    local zoneFilterResult = false

    -- 平台tags过滤
    ngx.log(ngx.DEBUG,string.format("publisherRule: %s",cjson.encode(zoneRule.rule)))
    if  zoneRule.rule ~= nil and zoneRule.rule ~= "" and (type(zoneRule.rule) =="table" and  zoneRule.rule ~= cjson.null) then
        for k,v in pairs(zoneRule) do
            zoneFilterResult = self.filter:ruleFilter(data,v)
            if zoneFilterResult== true then -- 只要有一条是通过的，就通过
                break
            end
            ngx.log(ngx.DEBUG,string.format("zoneRule:%s  user data is:%s",cjson.encode(v),cjson.encode(data)))
        end
        if zoneFilterResult == false then
            ngx.log(ngx.DEBUG,"zone filter failed")
            return nil,nil
        end
    end
    local bNotFindEvent = true
    local eventId,tbEvent
    -- 事件过滤
    repeat
        local arrayIndex,eventElement
        local tbEventRule = {}
        if self.useWeight then
            -- 权重选择事件
            math.randomseed(os.time()) -- 随机
            local randomWeight = math.random() * totalWeight
            randomWeight = math.floor(randomWeight)
            arrayIndex,eventElement= self.filter:selectAdByWeight(eventArry,randomWeight)
        else
            -- 随机取值
            arrayIndex,eventElement = sys_utils.getRandomValueFromArray(eventArry)
        end
        if type(eventElement) ~= "table" then
            ngx.log(ngx.DEBUG,string.format("eventElement is not table:%s",eventElement))
            return nil,nil
        end
        ngx.log(ngx.DEBUG,string.format("arrayIndex:%s eventElement:%s",arrayIndex,cjson.encode(eventElement)))
        eventId = eventElement.id
        tbEvent = cjson.decode(eventElement.value)
        bNotFindEvent = true
        local singleNumberPv,totalMaxUv,totalMaxPv= self:getEventCalc(zoneId,eventId,"loginfo",data.traceId) -- 从loginfo取得pv/uv统计
        ngx.log(ngx.DEBUG,string.format("eventId:%s, singleNumberPv:%s,totalMaxUv:%s,totalMaxPv:%s",
         eventId,singleNumberPv,totalMaxUv,totalMaxPv))
        -- UV判断
        if tbEvent.totalMaxUv ~=nil and totalMaxUv > tbEvent.singleMaxUv then
            ngx.log(ngx.DEBUG,string.format("event id:%s, totalMaxUv:%s,tbEvent.totalMaxUv:%s, traceId:%s",
                eventId,totalMaxUv,tbEvent.totalMaxUv,data.traceId))
            bNotFindEvent = false
            goto continue
        end
        -- PV判断
        if tbEvent.totalMaxPv ~= nil and totalMaxPv > tbEvent.totalMaxPv then
            ngx.log(ngx.DEBUG,string.format("event id:%s, singleMaxpv:%s,tbEvent.totalMaxPv:%s,traceId:%s",
                eventId,totalMaxPv,tbEvent.totalMaxPv,data.traceId))
            bNotFindEvent = false
            goto continue
        end
        -- 单用户PV判断
        if tbEvent.singleMaxpv ~= nil  and singleNumberPv > tbEvent.singleMaxpv then
            ngx.log(ngx.DEBUG,string.format("event id:%s, singleMaxpv:%s,singleNumber:%s",
                eventId,tbEvent.singleMaxpv,singleNumberPv))
            bNotFindEvent = false
            goto continue
        end
        -- 30秒判断
        if tbEvent.threeSecLimit ~=nil and threeSecNumber > tbEvent.threeSecLimit then
            ngx.log(ngx.DEBUG,string.format("event id:%s,three sec pv is more than 30:%s three sec limit:%s",eventId,threeSecNumber,tbEvent.threeSecLimit))
            bNotFindEvent = false
            goto continue
        end

        -- 判断event的rule
        tbEventRule = cjson.decode(tbEvent.rule)
        for k,v in ipairs(tbEventRule) do
            local ruleFilterResult = self.filter:ruleFilter(data,v)
            if ruleFilterResult == false then
                ngx.log(ngx.DEBUG,string.format("remove event id:%s rule filter failed:%s",eventId,cjson.encode(v)))
                bNotFindEvent = false
                goto continue
            end
        end
        ::continue::
        if not bNotFindEvent then
            table.remove(eventArry,arrayIndex)
            totalWeight = totalWeight - tbEvent.weight
            ngx.log(ngx.DEBUG,string.format("continue event id:%s",eventId))
        end
    until bNotFindEvent
    ngx.log(ngx.DEBUG,string.format("return eventId:%s tbEvent:%s,traceId:%s",eventId,cjson.encode(tbEvent),data.traceId))
    return eventId,tbEvent
end

-- 设置广告追踪器的hash
function RedisData:setAdTracerHashAll(zoneId,publisherId,traceId,data)
    ngx.log(ngx.DEBUG,"setAdTracerHashAll",data)
    local data_hash = ngx.md5(data)
    ngx.log(ngx.DEBUG,"data_hash:",data_hash)
    local data_loginfo = data_hash .. "loginfo" .. str_utils.generateUniqueString()
    local data_clickinfo = data_hash .. "clickinfo" .. str_utils.generateUniqueString()
    local data_cb_advertiser = data_hash .. "cb_advertiser" .. str_utils.generateUniqueString()

    local loginfo_hash = ngx.md5(data_loginfo)
    local clickinfo_hash = ngx.md5(data_clickinfo)
    local cb_advertiser_hash = ngx.md5(data_cb_advertiser)
    
    local key = m_global:get_appname() .. ":" .. zoneId..":"  .. publisherId.. ":"..  traceId .. ":traceHash"
    local ok,err = self.redis:hmset(key,"loginfo",loginfo_hash,"clickinfo",clickinfo_hash,"cb_advertiser",cb_advertiser_hash)
    if not ok then
        ngx.log(ngx.DEBUG, "Failed to set ad tracer hash to Redis: ", err)
        return "","",""
    end
    local res,err = self.redis:set_expire(key,60*60) -- 60分钟过期
    if res == nil then
        ngx.log(ngx.DEBUG, "Failed to set expire to Redis: ", err)
        return "","",""
    else 
        ngx.log(ngx.DEBUG,"set expire success")
    end
    return loginfo_hash,clickinfo_hash,cb_advertiser_hash
end

-- 更新广告追踪器的hash
function RedisData:updateAdTraceHashElement(data,t)
    ngx.log(ngx.DEBUG,string.format("updateAdTraceHashElement data:%s,type:%s",cjson.encode(data),t))

    local data_hash = ""
    -- 使用表模拟 switch 语句
    if t == "loginfo" then
        data_hash = data.signature
    elseif t == "clickinfo" then
        data_hash = data.hash
    elseif  t== "cb_advertiser" then
        data_hash = data.cb
    else
        ngx.log(ngx.ERR, "Unknown type: ", t)
        return false, "Unknown type"
    end


    local key = m_global:get_appname() .. ":" .. data.zoneId ..":" .. data.publisherId.. ":" .. data.traceId .. ":traceHash"
    local lockKey = key .. ":lock"
    ngx.log(ngx.DEBUG,string.format("updateAdTraceHashElement key:%s",key))
    local script = [[
        local lockKey = KEYS[1]
        local key = KEYS[2]
        local traceId= KEYS[3]
        local hash = KEYS[4]
        local t = KEYS[5]
        local res = redis.call('set' ,lockKey,"lock","NX","EX",1)
        if res==0  then
            return 0
        end
        res = redis.call('hget',key,t)
        if not res then
            return 0
        end
        if res ~= hash then
            return 0
        end
        res = redis.call('hdel',key,t)
        if res == nil then
            return 0
        end
        return 1
    ]]

    local res,err = self.redis:eval(script,tostring(lockKey),tostring(key),tostring(data.traceId),tostring(data_hash),tostring(t))
    if res == 0 then
        ngx.log(ngx.DEBUG, "Failed to get ad tracer hash from Redis: ", err)
        return false,err
    end
    return true,""
end


function RedisData:getEventResource(eventId)
   local resourceKey = m_global:get_appname() .. ":event:" .. eventId
   local resourceTable,err = self.redis:hgetKeyValue(resourceKey)
    if resourceTable == nil or resourceTable == cjson.null or type(resourceTable) ~= "table" then
        ngx.log(ngx.DEBUG, "Failed to get event_id from Redis: ", err)
        return nil,err
    end
    ngx.log(ngx.DEBUG,"EventTable",cjson.encode(resourceTable))
    local rId,rTb = sys_utils.getRandomKeyValueFromDict(resourceTable)
    ngx.log(ngx.DEBUG,"resourceId:",rId," resourceTable:",rTb)
    return rId,rTb
end

-- 一次取多个keys
function RedisData:getMultipleItems(key, items)
    local res, err = self.redis:hmget(key, unpack(items))
    if not res then
        ngx.log(ngx.ERR, "Failed to get keys from Redis: ", err)
        return nil, err
    end

    local results = {}
    for i, item in ipairs(items) do
        results[item] = res[i]
    end
    return results
end

return RedisData