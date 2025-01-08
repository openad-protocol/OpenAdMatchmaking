-- Load the rule_filer module
local rule_filter = require("scripts.rule_filter")

print("Test weight selection")
-- Create an instance of RuleFilter
local RuleFilter = rule_filter:new()

-- Define an event (you may need to adjust this based on your actual event structure)
local event = {
    -- event properties
}

-- Call the selectAdByWeight function
local selectedAd = RuleFilter:selectAdByWeight(event)

-- Print the selected ad
print(selectedAd)

-- 设置随机数种子
local totalWeight =2 
math.randomseed(os.time())
print(math.floor(math.random() * totalWeight) )