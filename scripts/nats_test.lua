local nats = require("scripts.nast_publish")

nats.publisher_message("getad", "getad", "testinfo")
