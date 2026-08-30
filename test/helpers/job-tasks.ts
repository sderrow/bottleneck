import sleep from "../../src/sleep";

/**
 * Manually-released signal for {@link createTaskFns}'s deferredPromise.
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
    promise,
    slowPromise,
    deferredPromise,
  };
}
