-- 取当前时间截
local function getCurrentTimestamp()
    -- 获取当前系统时间戳
    local timestamp = os.time()
    return timestamp
end

-- 随机打乱一个数组
-- Fisher-Yates 洗牌算法
local function shuffle(array)
    local n = #array
    for i = n, 2, -1 do
        local j = math.random(i)
        array[i], array[j] = array[j], array[i]
    end
end

-- 从数组中删除指定索引的元素
local function removeElement(array, index)
    for i = index, #array - 1 do
        array[i] = array[i + 1]
    end
    array[#array] = nil
end

-- 判断一个变量是否是数组
local function isArray(t)
    if type(t) ~= "table" then
        return false
    end

    local i = 1
    for _ in pairs(t) do
        if t[i] == nil then
            return false
        end
        i = i + 1
    end

    return true
end

local function isCurrentTimeInRange(startHour, startMinute, endHour, endMinute)
    -- 获取当前时间的小时和分钟
    local currentHour = os.date("*t").hour
    local currentMinute = os.date("*t").min

    -- 将时间转换为分钟数
    local currentTimeInMinutes = currentHour * 60 + currentMinute
    local startTimeInMinutes = startHour * 60 + startMinute
    local endTimeInMinutes = endHour * 60 + endMinute

    -- 判断当前时间是否在指定的时间范围内
    return currentTimeInMinutes >= startTimeInMinutes and currentTimeInMinutes <= endTimeInMinutes
end

-- 获取一个 table 的长度
local function getTableLength(t)
    local count = 0
    for _ in pairs(t) do
        count = count + 1
    end
    return count
end

-- 判断给定的时间戳是否在指定的时间范围内
local function isTimestampInRange(startTimestamp, endTimestamp)
    -- 获取当前时间戳
    local currentTimestamp = os.time()

    -- 判断当前时间戳是否在指定的时间范围内
    if currentTimestamp >= startTimestamp and currentTimestamp <= endTimestamp then
        return true
    else
        return false
    end
end

-- 从数组中随机获取一个元素
local function getRandomValueFromArray(array)
    if #array == 0 then
        return nil  -- 如果数组为空，返回 nil
    end
    local randomIndex = math.random(1, #array)
    return randomIndex,array[randomIndex]
end


local function tableToArry(dict)
    local keys = {}
    for key,value in pairs(dict) do
        table.insert(keys, {id=key,value=value})
    end
    return keys
end

-- 从字典字随机取一个元素
local function getRandomKeyValueFromDict(dict)
    local keys = {}
    for key in pairs(dict) do
        table.insert(keys, key)
    end

    if #keys == 0 then
        return nil, nil  -- 如果字典为空，返回 nil
    end

    local randomIndex = math.random(1, #keys)
    local randomKey = keys[randomIndex]
    return randomKey, dict[randomKey]
end


return {
    getCurrentTimestamp = getCurrentTimestamp,
    shuffle = shuffle,
    removeElement = removeElement,
    isArray = isArray,
    isCurrentTimeInRange = isCurrentTimeInRange,
    isTimestampInRange = isTimestampInRange,
    getTableLength = getTableLength,
    getRandomValueFromArray = getRandomValueFromArray,
    getRandomKeyValueFromDict = getRandomKeyValueFromDict,
    tableToArry = tableToArry
}


