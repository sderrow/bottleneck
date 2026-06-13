local queued = tonumber(ARGV[num_static_argv + 1])

local groupTimeout = tonumber(redis.call('hget', settings_key, 'groupTimeout'))
if groupTimeout ~= nil and queued > 0 then
  refresh_expiration(now, now, groupTimeout)
end

process_tick(now, true)
