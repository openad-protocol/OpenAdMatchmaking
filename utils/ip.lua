
local global = require("utils/global")
-- 获取客户端 IP
function getClientIp ()
    local client_ip = ngx.var.http_x_forwarded_for or ngx.var.http_x_real_ip or ngx.var.remote_addr or ''
    return client_ip
end