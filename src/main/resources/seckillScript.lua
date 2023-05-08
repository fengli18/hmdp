-- 获取优惠券ID
local voucherId = ARGV[1]
-- 获取用户ID
local userId = ARGV[2]
-- 获取redisKey
local stokeKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
-- 判断库存数量
if(tonumber(redis.call('get',stokeKey)) <= 0)
then
  return 1
end
-- 判断用户是否下单
if(redis.call('sismember',orderKey,userId) == 1)
then
  return 2
end
-- 扣除库存
redis.call('incrby',stokeKey,-1)
-- 添加到订单信息
redis.call('sadd',orderKey,userId)
return 0
