var makeTest = require("./context");
var Bottleneck = require("./bottleneck");
const { describe, it, afterEach } = require("mocha");

describe("Batcher", function () {
  var c;

  afterEach(function () {
    return c.limiter.disconnect(false);
  });

  it("Should batch by time and size", function () {
    c = makeTest();
    var batcher = new Bottleneck.Batcher({
      maxTime: 50,
      maxSize: 3,
    });
    var t0 = Date.now();
    var batches = [];

    batcher.on("batch", function (groups) {
      batches.push(groups);
    });

    return Promise.all([
      batcher.add(1).then(() => c.limiter.schedule(c.promise, null, Date.now() - t0, 1)),
      batcher.add(2).then(() => c.limiter.schedule(c.promise, null, Date.now() - t0, 2)),
      batcher.add(3).then(() => c.limiter.schedule(c.promise, null, Date.now() - t0, 3)),
      batcher.add(4).then(() => c.limiter.schedule(c.promise, null, Date.now() - t0, 4)),
      batcher.add(5).then(() => c.limiter.schedule(c.promise, null, Date.now() - t0, 5)),
    ])
      .then(function (data) {
        c.mustEqual(
          data.map(([t, x]) => [Math.floor(t / 50), x]),
          [
            [0, 1],
            [0, 2],
            [0, 3],
            [1, 4],
            [1, 5],
          ],
        );

        return c.last();
      })
      .then(function (_results) {
        c.checkDuration(50, 20);
        c.mustEqual(batches, [
          [1, 2, 3],
          [4, 5],
        ]);
      });
  });

  it("Should batch by time", function () {
    c = makeTest();
    var batcher = new Bottleneck.Batcher({
      maxTime: 50,
    });
    var t0 = Date.now();
    var batches = [];

    batcher.on("batch", function (groups) {
      batches.push(groups);
    });

    return Promise.all([
      batcher.add(1).then((_x) => c.limiter.schedule(c.promise, null, Date.now() - t0, 1)),
      batcher.add(2).then((_x) => c.limiter.schedule(c.promise, null, Date.now() - t0, 2)),
    ])
      .then(function (data) {
        c.mustEqual(
          data.map(([t, x]) => [Math.floor(t / 50), x]),
          [
            [1, 1],
            [1, 2],
          ],
        );

        return Promise.all([
          batcher.add(3).then((_x) => c.limiter.schedule(c.promise, null, Date.now() - t0, 3)),
          batcher.add(4).then((_x) => c.limiter.schedule(c.promise, null, Date.now() - t0, 4)),
        ]);
      })
      .then(function (data) {
        c.mustEqual(
          data.map(([t, x]) => [Math.floor(t / 50), x]),
          [
            [2, 3],
            [2, 4],
          ],
        );

        return c.last();
      })
      .then(function (_results) {
        c.checkDuration(100);
        c.mustEqual(batches, [
          [1, 2],
          [3, 4],
        ]);
      });
  });

  it("Should batch by size", function () {
    c = makeTest();
    var batcher = new Bottleneck.Batcher({
      maxSize: 2,
    });
    var batches = [];

    batcher.on("batch", function (groups) {
      batches.push(groups);
    });

    return Promise.all([
      batcher.add(1).then((_x) => c.limiter.schedule(c.promise, null, 1)),
      batcher.add(2).then((_x) => c.limiter.schedule(c.promise, null, 2)),
    ])
      .then(function () {
        return Promise.all([
          batcher.add(3).then((_x) => c.limiter.schedule(c.promise, null, 3)),
          batcher.add(4).then((_x) => c.limiter.schedule(c.promise, null, 4)),
        ]);
      })
      .then(c.last)
      .then(function (_results) {
        c.checkDuration(0);
        c.mustEqual(batches, [
          [1, 2],
          [3, 4],
        ]);
      });
  });

  it("Should stagger flushes", async function () {
    c = makeTest();
    const batcher = new Bottleneck.Batcher({
      maxTime: 50,
      maxSize: 3,
    });
    const t0 = Date.now();
    const batches = [];

    batcher.on("batch", function (groups) {
      batches.push(groups);
    });

    const [first, second] = await Promise.all([
      batcher.add(1).then(() => c.limiter.schedule(c.promise, null, Date.now() - t0, 1)),
      batcher.add(2).then(() => c.limiter.schedule(c.promise, null, Date.now() - t0, 2)),
    ]);

    c.mustGte(first[0], 50);
    c.mustEqual(first[1], 1);

    c.mustGte(second[0], 50);
    c.mustEqual(second[1], 2);

    const promises = [
      batcher.add(3).then(() => c.limiter.schedule(c.promise, null, Date.now() - t0, 3)),
    ];

    await c.wait(10);

    promises.push(
      batcher.add(4).then(() => c.limiter.schedule(c.promise, null, Date.now() - t0, 4)),
    );

    const [third, fourth] = await Promise.all(promises);

    c.mustGte(third[0], 100);
    c.mustEqual(third[1], 3);

    c.mustGte(fourth[0], 100);
    c.mustEqual(fourth[1], 4);

    await c.last();

    c.checkDuration(120, 20);
    c.mustEqual(batches, [
      [1, 2],
      [3, 4],
    ]);
  });

  it("Should force then stagger flushes", function () {
    c = makeTest();
    var batcher = new Bottleneck.Batcher({
      maxTime: 50,
      maxSize: 3,
    });
    var t0 = Date.now();
    var batches = [];

    batcher.on("batch", function (groups) {
      batches.push(groups);
    });

    var promises = [];
    promises.push(
      batcher.add(1).then((_x) => c.limiter.schedule(c.promise, null, Date.now() - t0, 1)),
    );
    promises.push(
      batcher.add(2).then((_x) => c.limiter.schedule(c.promise, null, Date.now() - t0, 2)),
    );

    return c
      .wait(10)
      .then(function () {
        promises.push(
          batcher.add(3).then((_x) => c.limiter.schedule(c.promise, null, Date.now() - t0, 3)),
        );

        return Promise.all(promises);
      })
      .then(function (data) {
        c.mustEqual(
          data.map(([t, x]) => [Math.floor(t / 50), x]),
          [
            [0, 1],
            [0, 2],
            [0, 3],
          ],
        );

        return Promise.all([
          batcher.add(4).then((_x) => c.limiter.schedule(c.promise, null, Date.now() - t0, 4)),
          batcher.add(5).then((_x) => c.limiter.schedule(c.promise, null, Date.now() - t0, 5)),
        ]);
      })
      .then(function (data) {
        c.mustEqual(
          data.map(([t, x]) => [Math.floor(t / 50), x]),
          [
            [1, 4],
            [1, 5],
          ],
        );

        return c.last();
      })
      .then(function (_results) {
        c.checkDuration(85, 25);
        c.mustEqual(batches, [
          [1, 2, 3],
          [4, 5],
        ]);
      });
  });
});
