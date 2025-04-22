local cjson             = require "cjson"
local redis             = require "libs.redis"
local toml              = require "libs.toml"
local http              = require "libs.http"

require "libs.functions"

math.randomseed(tostring(ngx.time()):reverse():sub(1, 6));

local workerId = ngx.worker.id()

local config = nil

DEBUG=nil

local function file_exists(name)
    local f=io.open(name,"r")
    if f~=nil then io.close(f) return true else return false end
end

local function read_file(path)
    local file = io.open(path, "r") -- r read mode and b binary mode
    if not file then return nil end
    local content = file:read "*a" -- *a or *all reads the whole file
    file:close()
    return content
end

if file_exists("config/dev.toml") then
    DEBUG = true
    local content = read_file("config/dev.toml")
    config=toml.parse(content)
else
    local content = read_file("config/prod.toml")
    config=toml.parse(content)
end
ngx.log(ngx.INFO,"config =======================>",cjson.encode(config))

APPNAME = config.global.appname
ngx.log(ngx.DEBUG, "APPNAME=", APPNAME)

APPVERSION = config.global.appversion
ngx.log(ngx.DEBUG, "APPVERSION=", APPVERSION)

STATISTICS_EXPIRE = config.global.statistics_expire
ngx.log(ngx.DEBUG, "STATISTICS_EXPIRE=", STATISTICS_EXPIRE)

THREEN_SEC_EXPIRE = config.global.three_sec_expire
ngx.log(ngx.DEBUG, "STATISTICS_EXPIRE=", THREEN_SEC_EXPIRE)

REDIS_CONFIG = config.redis
ngx.log(ngx.DEBUG, "REDIS_CONFIG=", cjson.encode(REDIS_CONFIG))

QUEUE_CONFIG = config.queue
ngx.log(ngx.DEBUG, "QUEUE_CONFIG=", cjson.encode(QUEUE_CONFIG))

NATS_QUQEUE_CONFIG = config.nats
ngx.log(ngx.DEBUG, "NATS_QUQEUE_CONFIG=", cjson.encode(NATS_QUQEUE_CONFIG))

NATS_QUQEUE_PARTNER_CONFIG = config.nats_partner
ngx.log(ngx.DEBUG, "NATS_QUQEUE_CONFIG=", cjson.encode(NATS_QUQEUE_CONFIG))

MAXMINDDB_CONFIG = config.maxminddb
ngx.log(ngx.DEBUG, "MAXMINDDB_CONFIG=", cjson.encode(MAXMINDDB_CONFIG))

ngx.log(ngx.DEBUG, "workerId=", workerId)


