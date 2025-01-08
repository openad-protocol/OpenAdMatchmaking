local nats = require("libs.nats")
local socket = require("libs.socket")
local m_global = require("utils.global")

-- 定义 sleep 函数
local function sleep(seconds)
    if package.config:sub(1,1) == '\\' then
        -- Windows
        os.execute("timeout /t " .. tonumber(seconds) .. " /nobreak >nul")
    else
        -- Unix
        os.execute("sleep " .. tonumber(seconds))
    end
end


-- NATS client class
local NatsClient = {}
NatsClient.__index = NatsClient

function NatsClient:new()
    local conf = m_global:get_nats_conf()
    setmetatable({}, NatsClient)
    self = setmetatable({}, NatsClient)

    self.host = conf.host or "localhost"
    self.port = conf.port or 4222
    self.connection = nats.connect(self.host,self.port)
    self.subscriptions = {}
    self.running = true
    return self
end

function NatsClient:subscribe(subject, callback)
    local sid = self.connection:subscribe(subject, callback)
    self.subscriptions[sid] = {subject = subject, callback = callback}
end


function NatsClient:publish(subject, message)
    self.connection:publish(subject, message)
end

function NatsClient:reconnect()
    ngx.log(ngx.INFO, "Reconnecting to NATS server...")
    local ok, err = pcall(function()
        ngx.log(ngx.INFO, "Closing connection to NATS server...")
        self.connection = nats.connect(self.host, self.port)
        ngx.log(ngx.INFO, string.format("Reconnected to NATS server. host:%s,prot %s." ,self.host ,self.port))
    end)
    if not ok then
        ngx.log(ngx.ERR, "Failed to reconnect to NATS server: ", err)
        return false
    end
    for sid, sub in pairs(self.subscriptions) do
        self.connection:subscribe(sub.subject, sub.callback)
    end
    return true
end

function NatsClient:start()
    self.running = true
    local function message_handler(premature)
        if premature then
            return
        end
        while self.running do
            local ok, err = pcall(function()
                ngx.log(ngx.INFO, "Waiting for NATS messages...")
                self.connection:wait(1) -- 等待消息，超时时间为1秒
            end)
            if not ok then
                ngx.log(ngx.ERR, "Error in NATS message handler: ", err)
                local reconnected = self:reconnect()
                if not reconnected then
                    ngx.log(ngx.ERR, "Reconnection failed, stopping NATS client.")
                    self:stop()
                end
            end
            ngx.sleep(0.1) -- 防止CPU占用过高
        end
    end
    ngx.timer.at(0, message_handler)
end

function NatsClient:stop()
    self.running = false
end

function NatsClient:close()
    self:stop()
end

return NatsClient