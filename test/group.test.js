import { describe, expect } from "vitest";
import sleep from "../src/sleep.js";
import { useFakeClock } from "./helpers/clock.js";
import { test, waitForState } from "./helpers/test-api.js";
const Bottleneck = require("./bottleneck");

useFakeClock();

describe("Group", () => {
  test("Should construct without options", () => {
    const group = new Bottleneck.Group();
    expect(group.limiterOptions).toStrictEqual({});
  });

  test("Should create limiters", async ({ makeGroup }) => {
    expect.hasAssertions();
    const group = makeGroup({
      maxConcurrent: 1,
      minTime: 100,
    });

    const results = [];

    const job = async (...result) => {
      results.push(result);
      await sleep(50);
    };

    group.key("A").schedule(job, 1, 2);
    group.key("A").schedule(job, 3);
    group.key("A").schedule(job, 4);
    setTimeout(() => {
      group.key("B").schedule(job, 5);
    }, 20);
    setTimeout(() => {
      group.key("C").schedule(job, 6);
      group.key("C").schedule(job, 7);
    }, 40);

    // Scheduled last on key "A", so it runs once all other jobs are done and
    // acts as the completion barrier; assertion failures reject the promise.
    await group.key("A").schedule(async () => {
      expect(results.length).toStrictEqual(6);

      const byGroup = {};
      for (let i = 0; i < results.length; i++) {
        const v = results[i][0];
        const key = v === 1 || v === 3 || v === 4 ? "A" : v === 5 ? "B" : "C";
        byGroup[key] = byGroup[key] || [];
        byGroup[key].push(v);
      }
      expect(byGroup.A).toStrictEqual([1, 3, 4]);
      expect(byGroup.B).toStrictEqual([5]);
      expect(byGroup.C).toStrictEqual([6, 7]);
      expect(results[0]).toStrictEqual([1, 2]);
    });
  });

  test("Should set up the limiter IDs (default)", ({ makeGroup }) => {
    const group = makeGroup({
      maxConcurrent: 1,
      minTime: 100,
    });

    expect(group.key("A").id).toStrictEqual("group-key-A");
    expect(group.key("B").id).toStrictEqual("group-key-B");
    expect(group.key("XYZ").id).toStrictEqual("group-key-XYZ");

    const ids = group.keys().map((key) => {
      const lim = group.key(key);
      expect(lim._store.timeout).toStrictEqual(group.timeout);
      return lim.id;
    });
    expect(ids.sort()).toStrictEqual(["group-key-A", "group-key-B", "group-key-XYZ"]);
  });

  test("Should set up the limiter IDs (custom)", ({ makeGroup }) => {
    const group = makeGroup({
      maxConcurrent: 1,
      minTime: 100,
      id: "custom-id",
    });

    expect(group.key("A").id).toStrictEqual("custom-id-A");
    expect(group.key("B").id).toStrictEqual("custom-id-B");
    expect(group.key("XYZ").id).toStrictEqual("custom-id-XYZ");

    const ids = group.keys().map((key) => {
      const lim = group.key(key);
      expect(lim._store.timeout).toStrictEqual(group.timeout);
      return lim.id;
    });
    expect(ids.sort()).toStrictEqual(["custom-id-A", "custom-id-B", "custom-id-XYZ"]);
  });

  test("Should pass new limiter to 'created' event", async ({ makeLimiter, makeGroup }) => {
    const limiter = makeLimiter();
    const group = makeGroup({
      maxConcurrent: 1,
      minTime: 100,
    });

    const keys = [];
    const ids = [];
    const promises = [];
    const recordId = async (created, key) => {
      const lim = await created.updateSettings({ id: key });
      ids.push(lim.id);
    };
    group.on("created", (created, key) => {
      keys.push(key);
      promises.push(recordId(created, key));
    });

    group.key("A");
    group.key("B");
    group.key("A");
    group.key("B");
    group.key("B");
    group.key("BB");
    group.key("C");
    group.key("A");

    await Promise.all(promises);
    expect(keys).toStrictEqual(ids);
    await limiter.ready();
  });

  test("Should pass error on failure", async ({ makeGroup }) => {
    const failureMessage = "SOMETHING BLEW UP!!";
    const group = makeGroup({
      maxConcurrent: 1,
      minTime: 100,
    });
    expect(Object.keys(group.limiters)).toStrictEqual([]);

    const results = [];

    const job = async (...result) => {
      results.push(result);
      await sleep(50);
    };

    group.key("A").schedule(job, 1, 2);
    group.key("A").schedule(job, 3);
    group.key("A").schedule(job, 4);
    // Fire-and-forget: the rejection must be recorded concurrently with the
    // other scheduled jobs; awaiting it inline would delay the schedules below
    // and change the ordering under test.
    group
      .key("B")
      .schedule(() => Promise.reject(new Error(failureMessage)))
      .catch((err) => {
        results.push(["CAUGHT", err.message]);
      });
    setTimeout(() => {
      group.key("C").schedule(job, 6);
      group.key("C").schedule(job, 7);
    }, 40);

    // Scheduled last on key "A", so it runs once all other jobs are done and
    // acts as the completion barrier; assertion failures reject the promise.
    await group.key("A").schedule(async () => {
      expect(results).toStrictEqual([[1, 2], ["CAUGHT", failureMessage], [6], [3], [7], [4]]);
    });
  });

  test("Should update its timeout", async ({ makeGroup }) => {
    const group1 = makeGroup({
      maxConcurrent: 1,
      minTime: 100,
    });
    const group2 = makeGroup({
      maxConcurrent: 1,
      minTime: 100,
      timeout: 5000,
    });

    expect(group1.timeout).toStrictEqual(300000);
    expect(group2.timeout).toStrictEqual(5000);

    const p1 = group1.updateSettings({ timeout: 123 });
    const p2 = group2.updateSettings({ timeout: 456 });
    await Promise.all([p1, p2]);
    expect(group1.timeout).toStrictEqual(123);
    expect(group2.timeout).toStrictEqual(456);
  });

  test("Should update its limiter options", ({ makeGroup }) => {
    const group = makeGroup({
      maxConcurrent: 1,
      minTime: 100,
    });

    const limiter1 = group.key("AAA");
    expect(limiter1._store.storeOptions.minTime).toStrictEqual(100);

    group.updateSettings({ minTime: 200 });
    expect(limiter1._store.storeOptions.minTime).toStrictEqual(100);

    const limiter2 = group.key("BBB");
    expect(limiter2._store.storeOptions.minTime).toStrictEqual(200);
  });

  test("Should support keys(), limiters(), deleteKey()", async ({ harness: h, makeGroup }) => {
    const group1 = makeGroup({
      maxConcurrent: 1,
    });
    const KEY_A = "AAA";
    const KEY_B = "BBB";

    await Promise.all([
      expect(group1.key(KEY_A).schedule(h.promise, null, 1)).resolves.toEqual([1]),
      expect(group1.key(KEY_B).schedule(h.promise, null, 2)).resolves.toEqual([2]),
    ]);

    const keys = group1.keys();
    const limiters = group1.limiters();
    expect(keys).toStrictEqual([KEY_A, KEY_B]);
    expect(limiters.length).toStrictEqual(2);

    limiters.forEach((entry, i) => {
      expect(entry.key).toStrictEqual(keys[i]);
      expect(entry.limiter).toBeInstanceOf(Bottleneck);
    });

    const deleted = await group1.deleteKey(KEY_A);
    expect(deleted).toStrictEqual(true);
    expect(group1.keys().length).toStrictEqual(1);

    const deletedAgain = await group1.deleteKey(KEY_A);
    expect(deletedAgain).toStrictEqual(false);
    expect(group1.keys().length).toStrictEqual(1);
  });

  test("Should call autocleanup", async ({ makeLimiter, makeGroup }) => {
    const KEY = "test-key";
    const group = makeGroup({
      maxConcurrent: 1,
    });
    group.updateSettings({ timeout: 500 });
    const limiter = makeLimiter({ id: "something", timeout: group.timeout });

    group.instances[KEY] = limiter;
    await group.key(KEY).schedule(() => Promise.resolve());
    expect(group.instances[KEY]).toBeDefined();
    await waitForState(() => {
      expect(group.instances[KEY]).toBeUndefined();
    });
    expect(group.instances[KEY]).toBeUndefined();
  });
});
