local _M = {}


local appname
local appversion
local redisconf
local redis_queueconf
local natsconf
local nats_partner_conf
local maxminddb_conf
local statistics_expire
local three_sec_expire

function _M:get_appname()
    if not appname then
        appname = APPNAME
    end
    local obj = {
        appname = appname
    }
    setmetatable(obj, _M)
    self.__index = self
    return appname
end

function _M:get_appversion()
    if not appversion then
        appversion = APPVERSION
    end
    return appversion
end

function _M:get_statistics_expire()
    if not statistics_expire then
        statistics_expire = STATISTICS_EXPIRE
    end
    return statistics_expire
end

function _M:get_three_statics_expire()
    if not three_sec_expire then
        three_sec_expire = THREEN_SEC_EXPIRE
    end
    return three_sec_expire
end

function _M:get_redis_conf()
    if not redisconf then
        redisconf = {}
        redisconf.host = REDIS_CONFIG.ip
        redisconf.port = REDIS_CONFIG.port
        redisconf.timeout = REDIS_CONFIG.timeout
        redisconf.ssl = REDIS_CONFIG.ssl
    end
    return redisconf
end

function _M:get_nats_conf()
    if not natsconf then
        natsconf = {}
        natsconf.host = NATS_QUQEUE_CONFIG.ip
        natsconf.port = NATS_QUQEUE_CONFIG.port
    end
    return natsconf
end

function _M:get_nats_partner_conf()
    if not nats_partner_conf then
        nats_partner_conf = {}
        nats_partner_conf.host = NATS_QUQEUE_PARTNER_CONFIG.ip
        nats_partner_conf.port = NATS_QUQEUE_PARTNER_CONFIG.port
    end
    return nats_partner_conf
end

function _M:get_redis_queue_conf()
    if not redis_queueconf then
        redis_queueconf = {}
        redis_queueconf.host = QUEUE_CONFIG.ip
        redis_queueconf.port = QUEUE_CONFIG.port
    end
    return natsconf
end

function _M:get_maxminddb_conf()
    if not maxminddb_conf then
        maxminddb_conf = {}
        maxminddb_conf.path = MAXMINDDB_CONFIG.path
    end
    return maxminddb_conf.path
end



return _M
