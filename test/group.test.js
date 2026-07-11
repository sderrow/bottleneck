import { useFakeClock } from "./helpers/clock.js";
import { test, describe, expect, waitForState } from "./helpers/test-api.js";
const Bottleneck = require("./bottleneck");

useFakeClock();

describe("Group", () => {
  test("Should create limiters", function ({ track }) {
    expect.hasAssertions();
    const group = track(
      new Bottleneck.Group({
        maxConcurrent: 1,
        minTime: 100,
      }),
    );

    const results = [];

    const job = function (...result) {
      results.push(result);
      return new Promise(function (resolve, _reject) {
        setTimeout(function () {
          return resolve();
        }, 50);
      });
    };

    group.key("A").schedule(job, 1, 2);
    group.key("A").schedule(job, 3);
    group.key("A").schedule(job, 4);
    setTimeout(function () {
      group.key("B").schedule(job, 5);
    }, 20);
    setTimeout(function () {
      group.key("C").schedule(job, 6);
      group.key("C").schedule(job, 7);
    }, 40);

    return new Promise(function (resolve, reject) {
      group.key("A").submit(function (cb) {
        try {
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
          cb();
          resolve();
        } catch (e) {
          reject(e);
        }
      }, null);
    });
  });

  test("Should set up the limiter IDs (default)", function ({ track }) {
    const group = track(
      new Bottleneck.Group({
        maxConcurrent: 1,
        minTime: 100,
      }),
    );

    expect(group.key("A").id).toStrictEqual("group-key-A");
    expect(group.key("B").id).toStrictEqual("group-key-B");
    expect(group.key("XYZ").id).toStrictEqual("group-key-XYZ");

    const ids = group.keys().map(function (key) {
      const lim = group.key(key);
      expect(lim._store.timeout).toStrictEqual(group.timeout);
      return lim.id;
    });
    expect(ids.sort()).toStrictEqual(["group-key-A", "group-key-B", "group-key-XYZ"]);
  });

  test("Should set up the limiter IDs (custom)", function ({ track }) {
    const group = track(
      new Bottleneck.Group({
        maxConcurrent: 1,
        minTime: 100,
        id: "custom-id",
      }),
    );

    expect(group.key("A").id).toStrictEqual("custom-id-A");
    expect(group.key("B").id).toStrictEqual("custom-id-B");
    expect(group.key("XYZ").id).toStrictEqual("custom-id-XYZ");

    const ids = group.keys().map(function (key) {
      const lim = group.key(key);
      expect(lim._store.timeout).toStrictEqual(group.timeout);
      return lim.id;
    });
    expect(ids.sort()).toStrictEqual(["custom-id-A", "custom-id-B", "custom-id-XYZ"]);
  });

  test("Should pass new limiter to 'created' event", function ({ makeLimiter, track }) {
    const limiter = makeLimiter();
    const group = track(
      new Bottleneck.Group({
        maxConcurrent: 1,
        minTime: 100,
      }),
    );

    const keys = [];
    const ids = [];
    const promises = [];
    group.on("created", function (created, key) {
      keys.push(key);
      promises.push(
        created.updateSettings({ id: key }).then(function (lim) {
          ids.push(lim.id);
        }),
      );
    });

    group.key("A");
    group.key("B");
    group.key("A");
    group.key("B");
    group.key("B");
    group.key("BB");
    group.key("C");
    group.key("A");

    return Promise.all(promises).then(function () {
      expect(keys).toStrictEqual(ids);
      return limiter.ready();
    });
  });

  test("Should pass error on failure", function ({ track }) {
    const failureMessage = "SOMETHING BLEW UP!!";
    const group = track(
      new Bottleneck.Group({
        maxConcurrent: 1,
        minTime: 100,
      }),
    );
    expect(Object.keys(group.limiters)).toStrictEqual([]);

    const results = [];

    const job = function (...result) {
      results.push(result);
      return new Promise(function (resolve, _reject) {
        setTimeout(function () {
          return resolve();
        }, 50);
      });
    };

    group.key("A").schedule(job, 1, 2);
    group.key("A").schedule(job, 3);
    group.key("A").schedule(job, 4);
    group
      .key("B")
      .schedule(() => Promise.reject(new Error(failureMessage)))
      .catch(function (err) {
        results.push(["CAUGHT", err.message]);
      });
    setTimeout(function () {
      group.key("C").schedule(job, 6);
      group.key("C").schedule(job, 7);
    }, 40);

    return new Promise(function (resolve, reject) {
      group.key("A").submit(function (cb) {
        try {
          expect(results).toStrictEqual([[1, 2], ["CAUGHT", failureMessage], [6], [3], [7], [4]]);
          cb();
          resolve();
        } catch (e) {
          reject(e);
        }
      }, null);
    });
  });

  test("Should update its timeout", function ({ track }) {
    const group1 = track(
      new Bottleneck.Group({
        maxConcurrent: 1,
        minTime: 100,
      }),
    );
    const group2 = track(
      new Bottleneck.Group({
        maxConcurrent: 1,
        minTime: 100,
        timeout: 5000,
      }),
    );

    expect(group1.timeout).toStrictEqual(300000);
    expect(group2.timeout).toStrictEqual(5000);

    const p1 = group1.updateSettings({ timeout: 123 });
    const p2 = group2.updateSettings({ timeout: 456 });
    return Promise.all([p1, p2]).then(function () {
      expect(group1.timeout).toStrictEqual(123);
      expect(group2.timeout).toStrictEqual(456);
    });
  });

  test("Should update its limiter options", function ({ track }) {
    const group = track(
      new Bottleneck.Group({
        maxConcurrent: 1,
        minTime: 100,
      }),
    );

    const limiter1 = group.key("AAA");
    expect(limiter1._store.storeOptions.minTime).toStrictEqual(100);

    group.updateSettings({ minTime: 200 });
    expect(limiter1._store.storeOptions.minTime).toStrictEqual(100);

    const limiter2 = group.key("BBB");
    expect(limiter2._store.storeOptions.minTime).toStrictEqual(200);
  });

  test("Should support keys(), limiters(), deleteKey()", function ({ harness: h, track }) {
    const group1 = track(
      new Bottleneck.Group({
        maxConcurrent: 1,
      }),
    );
    const KEY_A = "AAA";
    const KEY_B = "BBB";

    return Promise.all([
      h.pNoErrVal(group1.key(KEY_A).schedule(h.promise, null, 1), 1),
      h.pNoErrVal(group1.key(KEY_B).schedule(h.promise, null, 2), 2),
    ])
      .then(function () {
        const keys = group1.keys();
        const limiters = group1.limiters();
        expect(keys).toStrictEqual([KEY_A, KEY_B]);
        expect(limiters.length).toStrictEqual(2);

        limiters.forEach(function (entry, i) {
          expect(entry.key).toStrictEqual(keys[i]);
          expect(entry.limiter).toBeInstanceOf(Bottleneck);
        });

        return group1.deleteKey(KEY_A);
      })
      .then(function (deleted) {
        expect(deleted).toStrictEqual(true);
        expect(group1.keys().length).toStrictEqual(1);
        return group1.deleteKey(KEY_A);
      })
      .then(function (deleted) {
        expect(deleted).toStrictEqual(false);
        expect(group1.keys().length).toStrictEqual(1);
      });
  });

  test("Should call autocleanup", function ({ makeLimiter, track }) {
    const KEY = "test-key";
    const group = track(
      new Bottleneck.Group({
        maxConcurrent: 1,
      }),
    );
    group.updateSettings({ timeout: 500 });
    const limiter = makeLimiter({ id: "something", timeout: group.timeout });

    group.instances[KEY] = limiter;
    return group
      .key(KEY)
      .schedule(function () {
        return Promise.resolve();
      })
      .then(function () {
        expect(group.instances[KEY]).toBeDefined();
        return waitForState(() => {
          expect(group.instances[KEY]).toBeUndefined();
        });
      })
      .then(function () {
        expect(group.instances[KEY]).toBeUndefined();
      });
  });
});
