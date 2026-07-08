package fairness

// EnqueueLua mirrors Fairness::Scheduler::ENQUEUE_LUA (Ruby).
const EnqueueLua = `
local ring    = KEYS[1]
local vth     = KEYS[2]
local tenant  = ARGV[1]
local payload = ARGV[2]
local window  = tonumber(ARGV[3])
local rk      = ARGV[4] .. tenant

if window > 0 and redis.call('LLEN', rk) >= window then return 0 end

if redis.call('ZSCORE', ring, tenant) == false then
  local vt = tonumber(redis.call('HGET', vth, tenant) or '0')
  local mn = redis.call('ZRANGE', ring, 0, 0, 'WITHSCORES')
  if mn[2] and tonumber(mn[2]) > vt then vt = tonumber(mn[2]) end
  redis.call('ZADD', ring, vt, tenant)
  redis.call('HSET', vth, tenant, vt)
end

redis.call('RPUSH', rk, payload)
return 1
`
