local m_global = require("utils.global")
local geo = require('resty.maxminddb')
local cjson = require('cjson')


local IPInfo = {}
IPInfo.__index = IPInfo

function IPInfo:new()
    local self = setmetatable({}, IPInfo)

    if not geo.initted() then
        geo.init(m_global.get_maxminddb_conf())
    end    
    self.ip_db = geo
    return self
end

function IPInfo:getCountry(ipAddress)
    local res,err = self.ip_db.lookup(ipAddress)
    if not res then
        return "NULL"
    end
    local country = res["country"]
    return country["iso_code"]
end

function IPInfo:getCity(ipAddress)
    local res,err = self.ip_db:lookup(ipAddress)
    if not res then
        return "NULL"
    end
    local city = res["city"]
    return city["names"]["en"]
end

return IPInfo