local cjson = require "cjson"

local function generateResponse(code,message, data)
    local errorJson = {
            errmsg = message,
            errcode = code,
            data = data
    }
    return cjson.encode(errorJson)
end

local function generateResponseLogClick()
    local errorJson = {
        errmsg = "success",
        errcode = 0,
        data = false
    }
    return cjson.encode(errorJson)
end

return {
    generateResponse = generateResponse,
    generateResponseLogClick = generateResponseLogClick
}