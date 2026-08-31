import { describe, expect, expectTypeOf, it } from "vitest";
import type {
  BatcherOptions,
  ClientsList,
  Counts,
  EventInfoDropped,
  EventInfoQueued,
  JobOptions,
  Status,
  StopOptions,
} from "../src/types";
import Bottleneck from "../src/index";

/*
 * Type-level contract test for the published surface. The generated dts
 * (dist/index.d.mts / dist/index.d.cts) is produced from these same source
 * types, so asserting on them here guards the published type contract.
 */

const makeFakeRedisClient = () => ({
  setMaxListeners() {},
  on() {},
  once() {},
  duplicate: () => makeFakeRedisClient(),
});

describe("Bottleneck type contract", () => {
  it("exposes the strategy constants as literal types", () => {
    expectTypeOf(Bottleneck.strategy.LEAK).toEqualTypeOf<1>();
    expectTypeOf(Bottleneck.strategy.OVERFLOW).toEqualTypeOf<2>();
    expectTypeOf(Bottleneck.strategy.BLOCK).toEqualTypeOf<3>();
    expectTypeOf(Bottleneck.strategy.OVERFLOW_PRIORITY).toEqualTypeOf<4>();
  });

  it("accepts ConstructorOptions", () => {
    const limiter = new Bottleneck({ maxConcurrent: 2, minTime: 100, id: "l" });
    expectTypeOf(limiter).toExtend<Bottleneck>();
  });

  it("types schedule() by the task return value", () => {
    const limiter = new Bottleneck();
    expectTypeOf(limiter.schedule(() => 42)).toEqualTypeOf<Promise<number>>();
    expectTypeOf(limiter.schedule({ priority: 1 }, () => "x")).toEqualTypeOf<Promise<string>>();
    expectTypeOf(limiter.schedule({ priority: 1 }, (a: string) => a.length, "abc")).toEqualTypeOf<
      Promise<number>
    >();
    expectTypeOf(limiter.schedule((a: string, b: number) => `${a}${b}`, "x", 1)).toEqualTypeOf<
      Promise<string>
    >();
  });

  it("types wrap() preserving argument types", () => {
    const limiter = new Bottleneck();
    const wrapped = limiter.wrap((a: string, b: number) => Promise.resolve(a.length + b));
    expectTypeOf(wrapped("x", 1)).toEqualTypeOf<Promise<number>>();
    expectTypeOf(wrapped.withOptions({ priority: 2 }, "x", 1)).toEqualTypeOf<Promise<number>>();
  });

  it("types status introspection", () => {
    const limiter = new Bottleneck();
    expectTypeOf(limiter.jobStatus("id")).toEqualTypeOf<Status | null>();
    expectTypeOf(limiter.jobs("RUNNING")).toEqualTypeOf<string[]>();
    expectTypeOf(limiter.counts()).toEqualTypeOf<Counts>();
    expectTypeOf(limiter.running()).toEqualTypeOf<Promise<number>>();
    expectTypeOf(limiter.done()).toEqualTypeOf<Promise<number>>();
    expectTypeOf(limiter.check(1)).toEqualTypeOf<Promise<boolean>>();
    expectTypeOf(limiter.queued()).toEqualTypeOf<number>();
    expectTypeOf(limiter.empty()).toEqualTypeOf<boolean>();
  });

  it("types reservoir methods", () => {
    const limiter = new Bottleneck();
    expectTypeOf(limiter.currentReservoir()).toEqualTypeOf<Promise<number | null>>();
    expectTypeOf(limiter.incrementReservoir(1)).toEqualTypeOf<Promise<number | null>>();
  });

  it("types settings and lifecycle methods", () => {
    const limiter = new Bottleneck();
    expectTypeOf(limiter.updateSettings({ maxConcurrent: 2 })).toEqualTypeOf<Promise<Bottleneck>>();
    expectTypeOf(limiter.stop({ dropWaitingJobs: true })).toEqualTypeOf<Promise<void>>();
    expectTypeOf(limiter.disconnect(true)).toEqualTypeOf<Promise<void>>();
    expectTypeOf(limiter.chain(new Bottleneck())).toEqualTypeOf<Bottleneck>();
  });

  it("types event listeners via the event map", () => {
    const limiter = new Bottleneck();
    limiter.on("error", (error) => expectTypeOf(error).toEqualTypeOf<unknown>());
    limiter.on("message", (message) => expectTypeOf(message).toEqualTypeOf<string>());
    limiter.on("depleted", (empty) => expectTypeOf(empty).toEqualTypeOf<boolean>());
    limiter.on("dropped", (info) => expectTypeOf(info).toEqualTypeOf<EventInfoDropped>());
    limiter.on("queued", (info) => expectTypeOf(info).toEqualTypeOf<EventInfoQueued>());
    // Unknown event names fall through to the string-typed overload, matching
    // the runtime's dynamic event registry.
    limiter.on("nonsense", () => {});
  });

  it("types stop options", () => {
    expectTypeOf<StopOptions>().toExtend<Record<string, unknown>>();
  });

  it("types the Group surface", () => {
    const group = new Bottleneck.Group({ id: "g" });
    expectTypeOf(group.key("a")).toEqualTypeOf<Bottleneck>();
    expectTypeOf(group.keys()).toEqualTypeOf<string[]>();
    expectTypeOf(group.limiters()).toEqualTypeOf<{ key: string; limiter: Bottleneck }[]>();
    expectTypeOf(group.deleteKey("a")).toEqualTypeOf<Promise<boolean>>();
    expectTypeOf(group.clusterKeys()).toEqualTypeOf<Promise<string[]>>();
    group.on("created", (limiter, key) => {
      expectTypeOf(limiter).toExtend<Bottleneck>();
      expectTypeOf(key).toEqualTypeOf<string>();
    });
  });

  it("types the Batcher surface", () => {
    const batcher = new Bottleneck.Batcher<number>({ maxSize: 10 });
    expectTypeOf(batcher.add(1)).toEqualTypeOf<Promise<void>>();
    batcher.on("batch", (batch) => expectTypeOf(batch).toEqualTypeOf<number[]>());
  });

  it("exposes connection clients and channels", () => {
    const limiter = new Bottleneck();
    expectTypeOf(limiter.clients()).toEqualTypeOf<ClientsList>();
    expectTypeOf(limiter.channel()).toEqualTypeOf<string>();
  });

  it("rejects connection options with both Redis and client", () => {
    // Minimal stand-ins: the constructor runs for real (ready() stays pending
    // because these fakes never emit "ready"), the assertions here are
    // compile-time. They must satisfy _setup's synchronous calls.
    const nodeRedisFake = {
      createClient: makeFakeRedisClient,
    };
    const clientFake = makeFakeRedisClient();
    class IORedisFake {
      setMaxListeners() {}
      on() {}
      once() {}
      duplicate() {
        return makeFakeRedisClient();
      }
    }

    // Valid: either branch alone constructs.
    void new Bottleneck.RedisConnection({ Redis: nodeRedisFake });
    void new Bottleneck.RedisConnection({ client: clientFake });
    void new Bottleneck.IORedisConnection({ Redis: IORedisFake });

    // @ts-expect-error Redis and client are mutually exclusive
    void new Bottleneck.RedisConnection({ Redis: nodeRedisFake, client: clientFake });
    // @ts-expect-error ditto for ioredis
    void new Bottleneck.IORedisConnection({ Redis: IORedisFake, client: clientFake });

    expect(typeof Bottleneck.RedisConnection).toBe("function");
    expect(typeof Bottleneck.IORedisConnection).toBe("function");
  });

  it("keeps option types structural", () => {
    expectTypeOf<JobOptions>().toExtend<Record<string, unknown>>();
    expectTypeOf<BatcherOptions>().toExtend<Record<string, unknown>>();
  });
});
