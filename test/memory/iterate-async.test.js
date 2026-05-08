import { describe, it, assert, expect } from "vitest";
const { iterateAsync } = require("../leakage");

describe("iterateAsync helper", { timeout: 8000 }, function () {
  it("Should resolve when memory is stable", async function () {
    let calls = 0;
    await iterateAsync(
      async () => {
        calls++;
        const local = Array.from({ length: 50 }, () => 0);
        local.length = 0;
      },
      { iterations: 5, warmup: 1 },
    );
    assert.strictEqual(calls, 6, "fn should run warmup + iterations times");
  });

  it("Should throw when heap grows on every iteration", async function () {
    const sink = [];
    await expect(
      iterateAsync(
        async () => {
          sink.push(Array.from({ length: 10_000 }, () => "leak"));
        },
        { iterations: 5, warmup: 1 },
      ),
    ).rejects.toThrow(/Memory leaked on every iteration \(5 iterations\)/);
  });

  it("Should call fn (warmup + iterations) times in total", async function () {
    let calls = 0;
    await iterateAsync(async () => calls++, { iterations: 10, warmup: 2 });
    assert.strictEqual(calls, 12);
  });

  it("Should default to warmup=3 and iterations=25 when options omitted", async function () {
    let calls = 0;
    await iterateAsync(async () => calls++);
    assert.strictEqual(calls, 28);
  });

  it("Should propagate errors thrown by fn", async function () {
    await expect(
      iterateAsync(
        async () => {
          throw new Error("boom");
        },
        { iterations: 5, warmup: 0 },
      ),
    ).rejects.toThrow(/boom/);
  });

  it("Should not flag a leak when growth is non-monotonic", async function () {
    let i = 0;
    const sink = [];
    await expect(
      iterateAsync(
        async () => {
          if (i++ % 2 === 0) sink.push(Array.from({ length: 1000 }, () => "x"));
          else sink.length = 0;
        },
        { iterations: 6, warmup: 1 },
      ),
    ).resolves.toBeUndefined();
  });
});
