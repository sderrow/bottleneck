import { describe, it, expect } from "vitest";
import type { IORedisConnectionOptions, RedisConnectionOptions } from "../../src/types";
import Bottleneck from "../bottleneck";
import { wrongType } from "../helpers/wrong-type";

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

  it("throws when a clustering connection is requested", () => {
    expect(() => new Bottleneck.RedisConnection(wrongType<RedisConnectionOptions>({}))).toThrow(
      /full version of Bottleneck/i,
    );
    expect(() => new Bottleneck.IORedisConnection(wrongType<IORedisConnectionOptions>({}))).toThrow(
      /full version of Bottleneck/i,
    );
  });
});
