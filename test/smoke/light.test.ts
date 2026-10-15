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

  it("aborts a queued job", async () => {
    const limiter = new Bottleneck({ maxConcurrent: 1 });
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const blocker = limiter.schedule(() => gate);
    const controller = new AbortController();
    const victim = limiter.schedule({ signal: controller.signal }, () => "ran");
    await limiter._submitLock.schedule(() => Promise.resolve());
    expect(limiter.queued()).toBe(1);
    controller.abort();
    await expect(victim).rejects.toBe(controller.signal.reason);
    expect(limiter.queued()).toBe(0);
    release?.();
    await blocker;
    await limiter.disconnect(false);
  });

  it("throws when clustering datastore is requested", () => {
    expect(() => {
      void new Bottleneck({ datastore: "redis" });
    }).toThrow(/full version of Bottleneck/i);
  });
});
