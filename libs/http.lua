local request = {}
local http = require "libs.resty.http"

-- http请求函数
function request.get(url)
    local httpc = http.new()
    local methods = "GET"
    local res, err = httpc:request_uri(url, {
        ssl_verify = false,
        method = methods,
        headers = {
            ["Content-Type"] = "application/x-www-form-urlencoded",
        }
    })
    if not res then
        ngx.log(ngx.INFO,"failed to request: ", err)
        return false
    else
        if res.status == 200 then
            return res.body
        else
            return false
        end
    end
end

function request.post(url, body, headers)
    local httpc = http.new()
    local body = body or ""
    local headers = headers or {
            ["Content-Type"] = "application/json",
        }
    local res, err = httpc:request_uri(url, {
        ssl_verify = false,
        method =  "POST",
        body = body,
        headers = headers
    })
    if not res then
        ngx.log(ngx.INFO,"failed to request: ", err)
        return false
    else
        if res.status == 200 then
            return res.body
        else
            return false
        end
    end
end

return request