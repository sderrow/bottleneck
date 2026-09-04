import sleep from "../../src/sleep";

/**
 * Manually-released signal for {@link createTaskFns}'s deferredPromise.
 *
 *   const d = deferred();
 *   limiter.schedule(h.deferredPromise, d.signal, null, 1);
 *   ...observe held state...
 *   d.release();
 */
export function deferred(): { signal: Promise<unknown>; release: () => void } {
  let release: (() => void) | undefined;
  const signal = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { signal, release: release as () => void };
}

/**
 * Bottleneck task functions that record into `log.record(err, result)`.
 * Expects a `log` with a `record(err, result)` method.
 */
export function createTaskFns(log: { record: (err: unknown, result: unknown) => void }) {
  async function promise(err: unknown, ...result: unknown[]) {
    log.record(err, result);
    if (err === null) {
      return result;
    }
    throw err;
  }

  async function slowPromise(duration: number, err: unknown, ...result: unknown[]) {
    await sleep(duration);
    log.record(err, result);
    if (err === null) {
      return result;
    }
    throw err;
  }

  async function deferredPromise(signal: Promise<unknown>, err: unknown, ...result: unknown[]) {
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
