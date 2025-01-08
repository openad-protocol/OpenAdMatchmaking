local ngx = ngx
local md5 = ngx.md5

local function get_session_cookeid_id()
    -- 获取 HTTP 请求的 Cookie 和 Session
    local cookie_value = ngx.var.cookie_SESSIONID

    -- 检查是否获取到 Cookie 和 Session
    if not cookie_value then
        ngx.log(ngx.ERR, "Missing cookie or session")
        return nil, "Missing cookie or session"
    end

    -- 组合 Cookie 和 Session 生成唯一字符串
    local unique_string = cookie_value .. session_value

    -- 使用 MD5 哈希算法生成唯一 ID
    local unique_id = md5(unique_string)

    return unique_id
end

local function getmd5(key)
    local md5 = ngx.md5
    local md5_str = md5(key)
    return md5_str
end

return {
    getmd5 = getmd5,
    get_session_cookeid_id = get_session_cookeid_id
}


