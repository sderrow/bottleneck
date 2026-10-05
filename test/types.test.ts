import type { Cluster, Redis as IORedis } from "ioredis";
import type { createClient } from "redis";
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
import Bottleneck, {
  Batcher,
  Bottleneck as BottleneckNamed,
  BottleneckError,
  Group,
  IORedisConnection,
  RedisConnection,
} from "../src/index";

type NodeRedis = ReturnType<typeof createClient>;

// Never called: the connection type assertions below are compile-time.
const connectIORedis = (client: IORedis) => new IORedisConnection({ client });
const connectCluster = (client: Cluster) => new IORedisConnection({ client });
const connectNodeRedis = (client: NodeRedis) => new RedisConnection({ client });

/*
 * Type-level contract test for the published surface. The generated dts
 * (dist/index.d.mts) is produced from these same source types, so asserting
 * on them here guards the published type contract.
 */

describe("Bottleneck type contract", () => {
  it("exposes the strategy constants as literal types", () => {
    expectTypeOf(Bottleneck.strategy.LEAK).toEqualTypeOf<1>();
    expectTypeOf(Bottleneck.strategy.OVERFLOW).toEqualTypeOf<2>();
    expectTypeOf(Bottleneck.strategy.BLOCK).toEqualTypeOf<3>();
    expectTypeOf(Bottleneck.strategy.OVERFLOW_PRIORITY).toEqualTypeOf<4>();
  });

  it("exposes helper classes as named exports matching the statics", () => {
    expect(BottleneckError).toBe(Bottleneck.BottleneckError);
    expect(Group).toBe(Bottleneck.Group);
    expect(Batcher).toBe(Bottleneck.Batcher);
    expect(RedisConnection).toBe(Bottleneck.RedisConnection);
    expect(IORedisConnection).toBe(Bottleneck.IORedisConnection);
  });

  it("exposes Bottleneck as both a named and a default export", () => {
    // IDE auto-import for `new Bottleneck` resolves the named export; the
    // default export preserves `import Bottleneck from`.
    expect(BottleneckNamed).toBe(Bottleneck);
  });

  it("keeps CJS interop out of source (applied by the build footer instead)", () => {
    // `.default` / `.Bottleneck` self-references live in the tsdown footer,
    // not on the class. Dist-level assertions: test/smoke/lib.test.ts.
    expect("default" in Bottleneck).toBe(false);
    expect("Bottleneck" in Bottleneck).toBe(false);
  });

  it("accepts BottleneckOptions", () => {
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

  it("infers a connection's client type from the client passed in", () => {
    expectTypeOf<ReturnType<typeof connectIORedis>["client"]>().toEqualTypeOf<IORedis>();
    expectTypeOf<ReturnType<typeof connectIORedis>["subscriber"]>().toEqualTypeOf<IORedis>();
    expectTypeOf<ReturnType<typeof connectCluster>["client"]>().toEqualTypeOf<Cluster>();
    expectTypeOf<ReturnType<typeof connectNodeRedis>["client"]>().toEqualTypeOf<NodeRedis>();

    // @ts-expect-error a node-redis client isn't an ioredis client
    void ((client: NodeRedis) => new IORedisConnection({ client }));
    // @ts-expect-error connections take a client, not the library
    void ((Redis: unknown) => new RedisConnection({ Redis }));
  });

  it("keeps option types structural", () => {
    expectTypeOf<JobOptions>().toExtend<Record<string, unknown>>();
    expectTypeOf<BatcherOptions>().toExtend<Record<string, unknown>>();
  });
});
