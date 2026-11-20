local pending = tonumber(ARGV[num_static_argv + 1])

-- A client with jobs keeps the keys alive. Once they expire, the next script
-- recreates them with running = 0 and a full reservoir, and queued jobs would
-- start next to the jobs still running. The TTL never ends before
-- nextRequest, as register and submit set it, so minTime and blocks hold.
if pending ~= nil and pending > 0 then
  local settings = redis.call('hmget', settings_key, 'nextRequest', 'groupTimeout')
  local nextRequest = tonumber(settings[1]) or now
  local groupTimeout = tonumber(settings[2])
  refresh_expiration(now, math.max(now, nextRequest), groupTimeout)
end

return process_tick(now, true)['orphaned']
