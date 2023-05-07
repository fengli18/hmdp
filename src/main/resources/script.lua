local threadId = ARGV[1]
local redisThreadId = KEYS[1]

if(threadId == redisThreadId)
then
  redis.call('del',redisThreadId)
end
return 0