/**
 * Bottleneck task functions that record into `log.record(err, result)`.
 * Expects `log` from {@link createCallLog} in "./call-log.js".
 */
export function createTaskFns(log) {
  function job(err, ...result) {
    const cb = result.pop();
    log.record(err, result);
    cb.apply(null, [err].concat(result));
  }

  function slowJob(duration, err, ...result) {
    const cb = result.pop();
    setTimeout(function () {
      log.record(err, result);
      cb.apply(null, [err].concat(result));
    }, duration);
  }

  function deferredJob(signal, err, ...result) {
    const cb = result.pop();
    signal.then(function () {
      log.record(err, result);
      cb.apply(null, [err].concat(result));
    });
  }

  function promise(err, ...result) {
    return new Promise(function (resolve, reject) {
      log.record(err, result);
      if (err === null) {
        return resolve(result);
      }
      return reject(err);
    });
  }

  function slowPromise(duration, err, ...result) {
    return new Promise(function (resolve, reject) {
      setTimeout(function () {
        log.record(err, result);
        if (err === null) {
          return resolve(result);
        }
        return reject(err);
      }, duration);
    });
  }

  function deferredPromise(signal, err, ...result) {
    return new Promise(function (resolve, reject) {
      signal.then(function () {
        log.record(err, result);
        if (err === null) {
          return resolve(result);
        }
        return reject(err);
      });
    });
  }

  return {
    job: job,
    slowJob: slowJob,
    deferredJob: deferredJob,
    promise: promise,
    slowPromise: slowPromise,
    deferredPromise: deferredPromise,
  };
}
