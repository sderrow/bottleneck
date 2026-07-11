import { useFakeClock, wait } from "./helpers/clock.js";
import { test, describe, expect } from "./helpers/test-api.js";
const Bottleneck = require("./bottleneck");

// Batcher is datastore-independent, so this file only runs in the `local`
// project (excluded from the redis projects in vitest.config.ts) and always
// gets the fake clock — timing assertions below are exact virtual times.
useFakeClock();

describe("Batcher", () => {
  test("Should batch by time and size", async function () {
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
    expect(batchTimes[0] - t0).toBe(0);
    expect(batchTimes[1] - batchTimes[0]).toBe(100);
  });

  test("Should batch by time", async function () {
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
    expect(batchTimes[0] - t0).toBe(100);

    const t1 = Date.now();
    await Promise.all([batcher.add(3), batcher.add(4)]);

    expect(batches).toStrictEqual([
      [1, 2],
      [3, 4],
    ]);
    expect(batchTimes[1] - t1).toBe(100);
  });

  test("Should batch by size", async function () {
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

  test("Should stagger flushes", async function () {
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
    expect(batchTimes[0] - t0).toBe(100);
  });

  test("Should force then stagger flushes", async function () {
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
    expect(batchTimes[0] - t0).toBe(0);

    const t1 = Date.now();
    const p4 = batcher.add(4);
    await wait(50);
    const p5 = batcher.add(5);
    await Promise.all([p4, p5]);

    expect(batches).toStrictEqual([
      [1, 2, 3],
      [4, 5],
    ]);
    expect(batchTimes[1] - t1).toBe(100);
  });
});
