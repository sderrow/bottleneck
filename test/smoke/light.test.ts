import { describe, it, expect } from "vitest";
import Bottleneck from "../bottleneck";

describe("dist/light smoke", () => {
  it("loads", () => {
    expect(Bottleneck).toBeDefined();
    expect(typeof Bottleneck).toBe("function");
  });

  it("schedules locally", async () => {
    const limiter = new Bottleneck({ maxConcurrent: 1 });
    const result = await limiter.schedule(() => 42);
    expect(result).toBe(42);
    await limiter.disconnect(false);
  });

  it("throws when clustering datastore is requested", () => {
    expect(() => {
      void new Bottleneck({ datastore: "redis" });
    }).toThrow(/full version of Bottleneck/i);
  });
});
