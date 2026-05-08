import { createCallLog, flushLimiter as scheduleFlush } from "./call-log.js";
import { createTaskFns } from "./job-tasks.js";

export { createCallLog, flushLimiter } from "./call-log.js";
export { createTaskFns } from "./job-tasks.js";

/**
 * Default test helper: {@link createCallLog} + {@link createTaskFns} + `flushLimiter` bound to this log.
 * For low-level use, import `createCallLog` and `createTaskFns` separately.
 */
export function createJobHarness() {
  const log = createCallLog();
  const tasks = createTaskFns(log);

  return {
    job: tasks.job,
    slowJob: tasks.slowJob,
    deferredJob: tasks.deferredJob,
    promise: tasks.promise,
    slowPromise: tasks.slowPromise,
    deferredPromise: tasks.deferredPromise,
    getResults: log.getResults,
    results: log.results,
    flushLimiter: function (limiter, scheduleOptions) {
      return scheduleFlush(limiter, log.getResults, scheduleOptions);
    },
    checkResultsOrder: log.checkResultsOrder,
    checkDuration: log.checkDuration,
    pNoErrVal: log.pNoErrVal,
    noErrVal: log.noErrVal,
    wait: log.wait,
    calls: log.calls,
  };
}
