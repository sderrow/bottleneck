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

  function slowJob(duration, err, ...result) {
    const cb = result.pop();
    setTimeout(() => {
      log.record(err, result);
      cb.apply(null, [err].concat(result));
    }, duration);
  }

  function deferredJob(signal, err, ...result) {
    const cb = result.pop();
    signal.then(() => {
      log.record(err, result);
      cb.apply(null, [err].concat(result));
    });
  }

  function promise(err, ...result) {
    return new Promise((resolve, reject) => {
      log.record(err, result);
      if (err === null) {
        return resolve(result);
      }
      return reject(err);
    });
  }

  function slowPromise(duration, err, ...result) {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        log.record(err, result);
        if (err === null) {
          return resolve(result);
        }
        return reject(err);
      }, duration);
    });
  }

  function deferredPromise(signal, err, ...result) {
    return new Promise((resolve, reject) => {
      signal.then(() => {
        log.record(err, result);
        if (err === null) {
          return resolve(result);
        }
        return reject(err);
      });
    });
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
