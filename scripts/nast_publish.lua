local NatsClient = require("core.nats_client")
local m_global = require("utils.global")
local cjson = require("cjson")
-- 发布消息
local function publisher_message(topic,message)
    local host = m_global:get_nats_conf().host
    local port = m_global:get_nats_conf().port
    local client = NatsClient:new({host=host ,port=port})
    ngx.log(ngx.INFO,"publisher_message to nats:",message)
    client:publish(topic, message)
    ngx.log(ngx.INFO,"publisher_message success")
 end


return {
    publisher_message = publisher_message
}