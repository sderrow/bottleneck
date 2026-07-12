import { describe, it } from "vitest";
const assert = require("assert");
const Bottleneck = require("../bottleneck");
const { iterateAsync } = require("../leakage");

describe("Limiter memory", () => {
  it("Should not leak memory on instantiation", { timeout: 8000 }, async () => {
    let calls = 0;
    await iterateAsync(
      async () => {
        calls++;
        const limiter = new Bottleneck({ datastore: "local" });
        await limiter.ready();
        return limiter.disconnect(false);
      },
      { iterations: 25 },
    );
    assert.strictEqual(calls, 28);
  });

  it("Should not leak memory running jobs", { timeout: 12000 }, async () => {
    const limiter = new Bottleneck({ datastore: "local", maxConcurrent: 1, minTime: 10 });
    await limiter.ready();

    let i = 0;
    let calls = 0;

    try {
      await iterateAsync(
        async () => {
          calls++;
          await limiter.schedule(
            (zero, one) => {
              i = i + zero + one;
            },
            0,
            1,
          );
          await limiter.schedule(
            (zero, one) => {
              i = i + zero + one;
            },
            0,
            1,
          );
        },
        { iterations: 25 },
      );
      assert.deepStrictEqual(i, calls * 2);
    } finally {
      await limiter.disconnect(false);
    }
  });
});
