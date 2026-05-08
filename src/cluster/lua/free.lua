local index = ARGV[num_static_argv + 1]

-- Only enqueue the job for expiry-processing if it still exists in job_weights_key.
-- process_tick may have already expired the job (via its timestamp) before free.lua
-- runs, in which case job_weights_key was already cleared.  Re-adding the job to
-- job_expirations_key with score 0 in that case causes process_tick to decrement
-- client_running a second time, leaving it at -1 and permanently blocking cleanup.
if redis.call('hexists', job_weights_key, index) == 1 then
  redis.call('zadd', job_expirations_key, 0, index)
end

return process_tick(now, false)['running']
