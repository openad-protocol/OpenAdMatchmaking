-- 读取redis中的规则数据，数据是否在规则内
local red = require("core.redis_client")
local str_utils = require("utils.str_utils")
local ipDB = require("core.get_ipaddress_info")
local system_utils = require("utils.system")
local cjson = require("cjson")
local m_global = require("utils.global")

local RuleFilter = {}
RuleFilter.__index = RuleFilter

-- 构造函数
function RuleFilter:new(ruleMethod)
    local instance = setmetatable({}, RuleFilter)
    --local redis = red:new()
    --if redis == nil then
    --    ngx.log(ngx.ERR,"Failed to create Redis object")
    --    return nil
    --end
    --local ok, err = redis:connect()
    --if not ok then
    --    ngx.log(ngx.ERR,"Failed to connect to Redis: ", err)
    --    return nil
    --end
    -- ip数据库初始化
    --local ipDatabase = ipDB:new()
    --if ipDatabase == nil then
    --    ngx.log(ngx.ERR,"Failed to create IP database object")
    --    return nil
    --end

    instance.redis = redis
    instance.ipDatabase = ipDatabase
    instance.ruleMethod = ruleMethod or true
    return instance
end

-- 过滤函数，判断字符串以 | 分割的字符串是否包含某个元素
function RuleFilter:filter(inputStr,value)
    local isContained = false
    local tb = str_utils.splitString(inputStr,"|")
    ngx.log(ngx.INFO,string.format("tb:%s,value:%s",cjson.encode(tb),value))
    if #tb == 0 then -- 没有规则直接展示
        isContained=  true
    else
        for _,token in ipairs(tb) do
            if token == value then
                isContained = true
                break
            end
        end
    end
    ngx.log(ngx.INFO,string.format("isContained:%s",tostring(isContained)))
    return isContained
end

--function RuleFilter:getRuleFromRedis(zoneId,publisherId)  -- publisherId 为广告商id
--   -- [[
--    -- 1. 从redis中获取广告的平台规则
--    -- rule: [{"type":"platform","rules": "1|2|3|4", "revese": 0}]
--    --]]
--    local ad_unid = m_global:get_appname() .. ":" .. zoneId .. ":" .. publisherId .. ":rule"
--    local res,err = self.redis:get(ad_unid,"rule")
--    if res == nil then
--        ngx.log(ngx.ERR, "Failed to get rule from Redis: ", err)
--        return nil
--    end
--    return res
--end


function RuleFilter:ruleFilter(data,rulesInfo)
    ngx.log(ngx.INFO,string.format("data: %s,rules:%s",cjson.encode(data),cjson.encode(rulesInfo)))
    if rulesInfo== nil or rulesInfo==cjson.null or type(rulesInfo) ~="table" then
        ngx.log(ngx.ERR, "rulesInfo is not table: ", rulesInfo)
        return true
    end
    if type(data) ~= "table"  then
        ngx.log(ngx.ERR, "in date is not table: ", data)
        return false
    end

    local ruleMethod = self.ruleMethod -- true 为and false 为or
    if ruleMethod then
        ngx.log(ngx.INFO,"rule method is true")
    else
        ngx.log(ngx.INFO,"rule method is false")
    end
    local countryRule, languageRule, platformRule, datetimeRule, weekRule, loopTimeRule,channelRule = true, true, true, true, true, true,true
    local rule = rulesInfo

    if rule.type == "country" then  -- 国家规则
        ngx.log(ngx.INFO,"run country rule.tags: ",rule.tags)
        if rule.tags ~= nil then
            local country = data.country  -- 取得IP地址对应的国家
            ngx.log(ngx.INFO,string.format("run country rule.tags: %s, country:%s",rule.tags,country))
            if ruleMethod then
                countryRule = self:filter(rule.tags,country)
            else 
                countryRule = self:filter(rule.tags,country)
            end
        end
        if rule.reverse ~= nil and rule.reverse == 1 then
            countryRule = not countryRule
        end
        if countryRule then ngx.log(ngx.INFO,"run country rule allow!") else ngx.log(ngx.INFO,"country rule deny!") end
    end

    if rule.type == "language" then -- 语言规则
        ngx.log(ngx.INFO,string.format("run language rule.tags: %s, language data %s",rule.tags,data.language))
        if rule.tags ~= nil then
            if ruleMethod then
                languageRule = self:filter(rule.tags,data.language)
            else
                languageRule = self:filter(rule.tags,data.language)
            end
            ngx.log(ngx.DEBUG,string.format("languageRule:%s,reverse:%s",tostring(languageRule),tostring(rule.reverse)))
        end
        if rule.reverse ~= nil and rule.reverse == 1 then
            ngx.log(ngx.DEBUG,string.format("languageRule:%s,reverse:%s",tostring(languageRule),tostring(rule.reverse)))
            languageRule = not languageRule
        end
        if languageRule then ngx.log(ngx.INFO,string.format("run language rule allow,is bool:%s!",languageRule)) else ngx.log(ngx.INFO,string.format("language rule deny! bool %s",languageRule)) end
    end

    if rule.type == "platform" then -- 平台规则
        ngx.log(ngx.INFO,"run platform rule.tags: ",rule.tags)
        if rule.tags ~= nil then
            if ruleMethod then 
                platformRule = self:filter(rule.tags,data.platform)
            else 
                platformRule = self:filter(rule.tags,data.platform)
            end
        end
        if rule.reverse ~= nil and rule.reverse == 1 then
            platformRule = not platformRule
        end
        if platformRule then ngx.log(ngx.INFO,"run platform rule allow!") else ngx.log(ngx.INFO,"platform rule deny!") end
    end

    if rule.type == "datetime" then -- 时间规则
        ngx.log(ngx.INFO,"run datetime rule.tags: ",rule.tags)
        local eventStarTime,eventEndTime
            ngx.log(ngx.INFO,"run datetime rule.tags: ",rule.tags)
            -- tags转为开始和结束时间戳
            if rule.tags ~= nil then
                local evenTm = str_utils.splitString(rule.tags,"|")
                if #evenTm == 1 then
                    eventStarTime,eventEndTime = tonumber(evenTm[1]),4094978576 -- 2099-10-06 22:02:56
                else if #evenTm == 2 then
                    eventStarTime,eventEndTime = tonumber(evenTm[1]),tonumber(evenTm[2])
                else
                    ngx.log(ngx.ERR, "timestamp format error")
                    eventStarTime,eventEndTime = 0,4094978576
                end
                datetimeRule = system_utils.isTimestampInRange(eventStarTime,eventEndTime)
            end
            if rule.reverse ~= nil and rule.reverse == 1 then
                datetimeRule = not datetimeRule
            end
            if datetimeRule then ngx.log(ngx.INFO,"run datetime rule allow!") else ngx.log(ngx.INFO,"datetime rule deny!") end
            end
        end

    if rule.type == "week"  then -- 星期规则
        ngx.log(ngx.INFO,"run datetime rule.tags: ",rule.tags)
        if rule.tags ~= nil then
            local weekday = os.date("*t").wday
            weekRule = self:filter(rule.tags,tostring(weekday))
        end
        if rule.reverse ~= nil and rule.reverse == 1 then
            weekRule = not weekRule
        end
        if weekRule then ngx.log(ngx.INFO,"run week rule allow!") else ngx.log(ngx.INFO,"datetime week deny!") end
    end

    if rule.type == "loopTime" then -- 循环时间
        ngx.log(ngx.INFO,"run loopTime rule.tags: ",rule.tags)
        if rule.tags ~= nil then
            ngx.log(ngx.INFO,"run loopTime rule.tags: ",rule.tags)
            local eventLoopTime = str_utils.splitString(rule.tags,"|")      -- 事件时间
            ngx.log(ngx.INFO,"Event Time range:",table.concat(eventLoopTime,","))
            if system_utils.isArray(eventLoopTime) then -- 如果事件时间为数组
                for _,eTime in ipairs(eventLoopTime) do
                    local eventTimeArray = str_utils.splitString(eTime,"-")
                    if #eventTimeArray == 2 then
                        local eventStartHm = str_utils.splitString(eventTimeArray[1],":") -- 分割开始时间
                        local eventEndHm = str_utils.splitString(eventTimeArray[2],":") -- 分割结束时间
                        if #eventStartHm == 2 and #eventEndHm == 2 then
                            local eventTimeRule = system_utils.isCurrentTimeInRange(tonumber(eventStartHm[1]),tonumber(eventStartHm[2]),tonumber(eventEndHm[1]),tonumber(eventEndHm[2]))
                            if ruleMethod then
                                loopTimeRule = eventTimeRule
                            else
                                loopTimeRule = not eventTimeRule
                            end
                            if loopTimeRule then
                                break
                            end
                        else
                            ngx.log(ngx.ERR,"Event Rule Time format error")
                            loopTimeRule = true
                            break
                        end
                    end
                end
                if rule.reverse == 1 then
                    loopTimeRule = not loopTimeRule
                end
                if loopTimeRule then -- 命中一个条件就退出
                    if loopTimeRule then ngx.log(ngx.INFO,"run loopTime rule allow!") else ngx.log(ngx.INFO,"loopTime week deny!") end
                end
            end
        end
    end

    if rule.type == "channel" then -- 按渠道
        ngx.log(ngx.INFO,"run channel rule.tags: ",rule.tags)
        if rule.tags ~= nil then
            ngx.log(ngx.INFO,"run channel rule.tags: ",rule.tags)
            channelRule = self:filter(rule.tags,data.channel)
        end
        if rule.reverse ~= nil and rule.reverse == 1 then
            channelRule = not channelRule
        end
        if channelRule then ngx.log(ngx.INFO,"run channel rule allow!") else ngx.log(ngx.INFO,"channel channel deny! user channel:",data.channel," zone allow channel:",rule.tags) end
    end
    ngx.log(ngx.INFO,string.format("countryRule:%s,language rule:%s, platform rule%s,datetime rule%s, week rule:%s,loopTime rule:%s,channel rule:%s",
            tostring(countryRule),tostring(languageRule),tostring(platformRule),tostring(datetimeRule),tostring(weekRule),tostring(loopTimeRule),tostring(channelRule)))
    if ruleMethod then
        return countryRule and languageRule and platformRule and datetimeRule and weekRule and loopTimeRule and channelRule
    else
        return countryRule or languageRule or platformRule or datetimeRule or weekRule or loopTimeRule and channelRule
    end
end



-- 计算总权重
function RuleFilter:calculateTotalWeight(adData)
    local totalWeight = 0
    for key, ad in pairs(adData) do
        -- local tb = cjson.decode(ad)
        totalWeight = totalWeight + (ad.weight or 1)
    end
    return totalWeight
end

-- 根据权重选择广告,权重越大的，显示出来的概率越大
function RuleFilter:selectAdByWeight(eventDatas,randomWeight)
    if #eventDatas == 0 then
        return nil,nil  -- 如果数组为空，返回 nil
    end
    ngx.log(ngx.INFO,"randomWeight: ",randomWeight)
    local cumulativeWeight = 0
    for index, eventData in ipairs(eventDatas) do
        local tb = cjson.decode(eventData.value)
        cumulativeWeight = cumulativeWeight + (tb.weight or 1)
        if cumulativeWeight >= randomWeight then -- 计算数大于随机权重
            return index,eventData
        end
    end
    return nil,nil  -- 未找到
end

function RuleFilter:randoSelect(adData)
    local randomIndex = math.random(1, #adData)
    return adData[randomIndex]
end

return RuleFilter