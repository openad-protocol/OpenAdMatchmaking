local cjson = require('cjson')
local geo = require('resty.maxminddb')
if not geo.initted() then
    geo.init("/data/proj/apiserver/GeoLite2-City.mmdb")
end

local args  = {}

local function get_args()
    ngx.req.read_body()  -- 解析 body 参数之前一定要先读取 body
    args = ngx.req.get_uri_args()
    ngx.log(ngx.DEBUG, "nginx post input params data: " .. cjson.encode(args))
end

get_args()

--local res,err = geo.lookup(ngx.var.arg_ip or ngx.var.remote_addr) --support ipv6 e.g. 2001:4860:0:1001::3004:ef68
local res,err = geo.lookup(args.ip)

ngx.log(ngx.DEBUG, "ip address: " .. cjson.encode(res))
if res==nil or cjson.encode(res) == cjson.null or type(res)~="table" then
    ngx.log(ngx.ERR,'failed to lookup by ip ,reason:',err)
    ngx.say("NULL")
    return
end
local country = res["country"]
--return country["iso_code"]
ngx.say(cjson.encode(country["iso_code"]))
if ngx.var.arg_node then
   ngx.say("node name:",ngx.var.arg_node," ,value:", cjson.encode(res[ngx.var.arg_node] or {}))
end
