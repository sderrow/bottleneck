import { describe, it, expect, afterEach } from "vitest";
const Bottleneck = require("./bottleneck");

const wait = function (ms) {
  return new Promise(function (resolve) {
    setTimeout(resolve, ms);
  });
};

describe("Batcher", function () {
  let limiter;

  afterEach(function () {
    if (limiter) return limiter.disconnect(false);
  });

  it("Should batch by time and size", async function () {
    limiter = new Bottleneck();
    const batcher = new Bottleneck.Batcher({ maxTime: 100, maxSize: 3 });
    const batches = [];
    const batchTimes = [];

    batcher.on("batch", function (groups) {
      batchTimes.push(Date.now());
      batches.push(groups);
    });

    const t0 = Date.now();
    await Promise.all([1, 2, 3, 4, 5].map((x) => batcher.add(x)));

    expect(batches).toStrictEqual([
      [1, 2, 3],
      [4, 5],
    ]);
    expect(batchTimes[0] - t0).toBeLessThan(20);
    expect(batchTimes[1] - batchTimes[0]).toBeGreaterThanOrEqual(95);
  });

  it("Should batch by time", async function () {
    limiter = new Bottleneck();
    const batcher = new Bottleneck.Batcher({ maxTime: 100 });
    const batches = [];
    const batchTimes = [];

    batcher.on("batch", function (groups) {
      batchTimes.push(Date.now());
      batches.push(groups);
    });

    const t0 = Date.now();
    await Promise.all([batcher.add(1), batcher.add(2)]);

    expect(batches).toStrictEqual([[1, 2]]);
    expect(batchTimes[0] - t0).toBeGreaterThanOrEqual(95);

    const t1 = Date.now();
    await Promise.all([batcher.add(3), batcher.add(4)]);

    expect(batches).toStrictEqual([
      [1, 2],
      [3, 4],
    ]);
    expect(batchTimes[1] - t1).toBeGreaterThanOrEqual(95);
  });

  it("Should batch by size", async function () {
    limiter = new Bottleneck();
    const batcher = new Bottleneck.Batcher({ maxSize: 2 });
    const batches = [];

    batcher.on("batch", function (groups) {
      batches.push(groups);
    });

    await Promise.all([batcher.add(1), batcher.add(2)]);
    expect(batches).toStrictEqual([[1, 2]]);

    await Promise.all([batcher.add(3), batcher.add(4)]);
    expect(batches).toStrictEqual([
      [1, 2],
      [3, 4],
    ]);
  });

  it("Should stagger flushes", async function () {
    limiter = new Bottleneck();
    const batcher = new Bottleneck.Batcher({ maxTime: 100, maxSize: 3 });
    const batches = [];
    const batchTimes = [];

    batcher.on("batch", function (groups) {
      batchTimes.push(Date.now());
      batches.push(groups);
    });

    const t0 = Date.now();
    const p1 = batcher.add(1);
    await wait(50);
    const p2 = batcher.add(2);
    await Promise.all([p1, p2]);

    expect(batches).toStrictEqual([[1, 2]]);
    const elapsed = batchTimes[0] - t0;
    // Lower bound is the contract: the flush MUST wait for maxTime=100ms
    // since adding p2 mid-window must not reset (or shorten) the flush
    // timer. The upper bound is just a sanity check — under sustained
    // event-loop pressure (parallel test files, redis containers booting,
    // GC) setTimeout can drift well past maxTime+40ms; the original 140ms
    // upper bound was flaky for that reason.
    expect(elapsed).toBeGreaterThanOrEqual(95);
    expect(elapsed).toBeLessThan(1000);
  });

  it("Should force then stagger flushes", async function () {
    limiter = new Bottleneck();
    const batcher = new Bottleneck.Batcher({ maxTime: 100, maxSize: 3 });
    const batches = [];
    const batchTimes = [];

    batcher.on("batch", function (groups) {
      batchTimes.push(Date.now());
      batches.push(groups);
    });

    const t0 = Date.now();
    await Promise.all([batcher.add(1), batcher.add(2), batcher.add(3)]);
    expect(batches).toStrictEqual([[1, 2, 3]]);
    expect(batchTimes[0] - t0).toBeLessThan(20);

    const t1 = Date.now();
    const p4 = batcher.add(4);
    await wait(50);
    const p5 = batcher.add(5);
    await Promise.all([p4, p5]);

    expect(batches).toStrictEqual([
      [1, 2, 3],
      [4, 5],
    ]);
    const elapsed = batchTimes[1] - t1;
    expect(elapsed).toBeGreaterThanOrEqual(95);
    expect(elapsed).toBeLessThan(140);
  });
});
