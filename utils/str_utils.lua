local function splitString(inputStr, delimiter)
    local result = {}
    for match in (inputStr .. delimiter):gmatch("(.-)" .. delimiter) do
        table.insert(result, match)
    end
    return result
end


local function convertToTimestamp(timeStr)
    -- 解析时间字符串
    local pattern = "(%d+)%-(%d+)%-(%d+) (%d+):(%d+):(%d+)"
    local year, month, day, hour, min, sec = timeStr:match(pattern)
    if not (year and month and day and hour and min and sec) then
        ngx.log(ngx.ERR, "Failed to parse time string: ", timeStr) -- 无效时间格式
        return nil
    end
    -- 创建时间表
    local timeTable = {
        year = year,
        month = month,
        day = day,
        hour = hour,
        min = min,
        sec = sec
    }

    -- 转换为时间戳
    local timestamp = os.time(timeTable)
    return timestamp
end

-- 生成唯一字符串
local function generateUniqueString()
    -- 取得当前时间戳
    local timestamp = tostring(os.time())

    -- 生成一个随机数
    local randomNumber = tostring(math.random(100000, 999999))

    -- 连接时间戳和随机数
    local uniqueString = timestamp .. randomNumber

    return uniqueString
end

local function checkData(data)
    if data.zoneId == "" or data.publisherId == "" or data.traceId=="" then
        ngx.log(ngx.INFO,"Failed to get zoneId or publisherId or eventId or traceId")
        return false
    end
    return true
end

local function safe2number(s)
    if value == nil or type(value) ~= "number" then
        ngx.log(ngx.ERR, "Invalid value for tonumber: ", value)
        return default
    end
    return tonumber(value)
end

return {
    convertToTimestamp = convertToTimestamp,
    splitString = splitString,
    generateUniqueString = generateUniqueString,
    checkData = checkData,
    safe2number = safe2number
}