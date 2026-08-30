/**
 * Promise delay. Reads the global setTimeout at call time, so it respects
 * fake timers when a test environment installs them.
 */
const sleep = (ms = 0): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

export default sleep;
