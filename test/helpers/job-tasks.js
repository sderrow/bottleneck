import { sleep } from "./clock.js";

/**
 * Manually-released signal for {@link createTaskFns}'s deferredJob/deferredPromise.
 *
 *   const d = deferred();
 *   limiter.schedule(h.deferredPromise, d.signal, null, 1);
 *   ...observe held state...
 *   d.release();
 */
export function deferred() {
  let release;
  const signal = new Promise((resolve) => {
    release = resolve;
  });
  return { signal, release };
}

/**
 * Bottleneck task functions that record into `log.record(err, result)`.
 * Expects a `log` with a `record(err, result)` method.
 */
export function createTaskFns(log) {
  function job(err, ...result) {
    const cb = result.pop();
    log.record(err, result);
    cb.apply(null, [err].concat(result));
  }

  async function slowJob(duration, err, ...result) {
    const cb = result.pop();
    await sleep(duration);
    log.record(err, result);
    cb.apply(null, [err].concat(result));
  }

  async function deferredJob(signal, err, ...result) {
    const cb = result.pop();
    await signal;
    log.record(err, result);
    cb.apply(null, [err].concat(result));
  }

  async function promise(err, ...result) {
    log.record(err, result);
    if (err === null) {
      return result;
    }
    throw err;
  }

  async function slowPromise(duration, err, ...result) {
    await sleep(duration);
    log.record(err, result);
    if (err === null) {
      return result;
    }
    throw err;
  }

  async function deferredPromise(signal, err, ...result) {
    await signal;
    log.record(err, result);
    if (err === null) {
      return result;
    }
    throw err;
  }

  return {
    job,
    slowJob,
    deferredJob,
    promise,
    slowPromise,
    deferredPromise,
  };
}
