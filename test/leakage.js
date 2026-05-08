const v8 = require("node:v8");

function forceGc() {
  if (typeof global.gc !== "function") {
    throw new TypeError(
      "global.gc is not exposed. Run memory tests via `pnpm run test:memory` " +
        "(Vitest `memory` project uses `--expose-gc`).",
    );
  }
  global.gc();
  global.gc();
}

async function iterateAsync(fn, { iterations = 25, warmup = 3 } = {}) {
  for (let i = 0; i < warmup; i++) await fn();

  const heapDiffs = [];
  for (let i = 0; i < iterations; i++) {
    forceGc();
    const before = v8.getHeapStatistics().used_heap_size;
    await fn();
    forceGc();
    const after = v8.getHeapStatistics().used_heap_size;
    heapDiffs.push(after - before);
  }

  if (heapDiffs.every((d) => d > 0)) {
    throw new Error(
      `Memory leaked on every iteration (${iterations} iterations).\n` +
        `Heap diffs (bytes): [${heapDiffs.join(", ")}]`,
    );
  }
}

module.exports = { iterateAsync };
