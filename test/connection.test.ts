import { describe, test, expect, vi } from "vitest";
import type BottleneckBase from "../src/Bottleneck";
import type { IORedisClient, NodeRedisClient } from "../src/cluster/redis-types";
import type { IORedisConnectionOptions, RedisConnectionOptions } from "../src/types";
import IORedisConnection from "../src/cluster/IORedisConnection";
import RedisConnection from "../src/cluster/RedisConnection";
import * as Scripts from "../src/cluster/Scripts";
import { wrongType } from "./helpers/wrong-type";

// These are datastore-independent unit tests: both connection classes are
// driven with fake clients, so the file is deterministic in every test
// project (local + redis) without touching the shared Redis container.

type Handler = (...args: unknown[]) => void;
type Overrides = Record<string, unknown>;

const fakes = new WeakSet<object>();

type FakeNodeClient = ReturnType<typeof makeNodeClient>;
type FakeIOClient = ReturnType<typeof makeIOClient>;

/** Retype a client the connection created from one of the fake factories. */
function fake(client: NodeRedisClient): FakeNodeClient;
function fake(client: IORedisClient): FakeIOClient;
function fake(client: object): FakeNodeClient | FakeIOClient {
  if (!fakes.has(client)) throw new Error("Expected a fake client");
  return wrongType(client);
}

const makeEmitter = () => {
  const handlers: Record<string, Handler[]> = {};
  return {
    on(name: string, cb: Handler) {
      (handlers[name] ??= []).push(cb);
    },
    emit(name: string, ...args: unknown[]) {
      for (const cb of handlers[name] ?? []) cb(...args);
    },
    removeAllListeners(name?: string) {
      if (name != null) delete handlers[name];
    },
  };
};

const makeNodeClient = (overrides: Overrides = {}, dupOverrides: Overrides = {}) => {
  const subscriptions: Record<string, Handler> = {};
  const client = {
    ...makeEmitter(),
    _subscriptions: subscriptions,
    isOpen: true,
    scriptLoad: vi.fn<(payload: string) => Promise<string>>(
      async (payload) => `sha-${payload.length}`,
    ),
    evalSha: vi.fn<
      (sha: string, options: { keys: string[]; arguments: string[] }) => Promise<number>
    >(async () => 1),
    sendCommand: vi.fn<() => Promise<string>>(async () => "OK"),
    subscribe: vi.fn<(channel: string, cb: Handler) => Promise<void>>(async (channel, cb) => {
      subscriptions[channel] = cb;
    }),
    unsubscribe: vi.fn<() => Promise<void>>(async () => {}),
    close: vi.fn<() => Promise<void>>(async () => {}),
    quit: vi.fn<() => Promise<void>>(async () => {}),
    destroy: vi.fn<() => Promise<void>>(async () => {}),
    disconnect: vi.fn<() => void>(),
    duplicate: (): NodeRedisClient => makeNodeClient(dupOverrides),
  };
  Object.assign(client, overrides);
  fakes.add(client);
  // The fake implements only what RedisConnection calls.
  return wrongType<NodeRedisClient & typeof client>(client);
};

const makeIOClient = (overrides: Overrides = {}) => {
  const client = {
    ...makeEmitter(),
    status: "connect",
    once(name: string, cb: Handler) {
      client.on(name, cb);
    },
    setMaxListeners() {},
    defineCommand: vi.fn<(...args: unknown[]) => void>(),
    subscribe: vi.fn<(...args: unknown[]) => Promise<number>>(async () => 1),
    unsubscribe: vi.fn<(...args: unknown[]) => Promise<number>>(async () => 1),
  };
  Object.assign(client, overrides);
  fakes.add(client);
  // The fake implements only what IORedisConnection calls.
  return wrongType<IORedisClient & typeof client>(client);
};

const makeFakeRedis = (mainOverrides: Overrides = {}, subOverrides: Overrides = {}) => {
  // Plain functions, not vi.fn(): `new FakeRedis(...)` must return the fake
  // client object the factory builds.
  const calls: { nodes: unknown; options: unknown }[] = [];
  function Cluster(nodes: unknown, options: unknown) {
    calls.push({ nodes, options });
    return makeIOClient({ status: "ready" });
  }
  function FakeRedis() {
    return makeIOClient({ ...mainOverrides, duplicate: () => makeIOClient(subOverrides) });
  }
  return Object.assign(FakeRedis, { Cluster: Object.assign(Cluster, { calls }) });
};

/** Just the limiter surface the connections touch when (un)registering. */
const makeLimiterInstance = () => {
  const instance = {
    channel: () => "ch-one",
    channel_client: () => "ch-two",
    _store: { onMessage: vi.fn<(...args: unknown[]) => void>() },
  };
  return { instance, limiter: wrongType<BottleneckBase>(instance) };
};

describe("RedisConnection (node-redis)", () => {
  test("Should refuse to build without a Redis reference or a pre-built client", () => {
    // {} is not a valid RedisConnectionOptions; the runtime rejection is the contract
    expect(() => new RedisConnection({} as unknown as RedisConnectionOptions)).toThrow(
      /requires a `Redis` library reference or a pre-built `client`/,
    );
  });

  test("Should connect a client that is not open", async () => {
    const connect = vi.fn<() => Promise<void>>(async () => {});
    const client = makeNodeClient({ isOpen: false, connect });
    const conn = new RedisConnection({ client });

    await conn.ready;
    expect(connect).toHaveBeenCalledTimes(1);
  });

  test("Should reload and retry a script after NOSCRIPT", async () => {
    const client = makeNodeClient();
    client.evalSha
      .mockRejectedValueOnce(new Error("NOSCRIPT Invalid script hash."))
      .mockResolvedValueOnce(7);

    const conn = new RedisConnection({ client });
    await conn.ready;

    await expect(conn.__runScript__("register", "id", [null, 5])).resolves.toBe(7);
    expect(client.scriptLoad).toHaveBeenCalledTimes(Scripts.names.length + 1);
    expect(client.evalSha.mock.calls[1]?.[1].arguments[0]).toBe("");
  });

  test("Should reject ready when script loading fails", async () => {
    const client = makeNodeClient({
      scriptLoad: vi.fn<() => Promise<string>>(async () => {
        throw new Error("script load failed");
      }),
    });
    const conn = new RedisConnection({ client });

    await expect(conn.ready).rejects.toThrow("script load failed");
  });

  test("Should swallow script loading failures after termination", async () => {
    const client = makeNodeClient({
      scriptLoad: vi.fn<() => Promise<string>>(async () => {
        throw new Error("script load failed");
      }),
    });
    const conn = new RedisConnection({ client });
    conn.terminated = true;

    await expect(conn.ready).resolves.toBeDefined();
  });

  test("Should trigger error events only while not terminated", async () => {
    const client = makeNodeClient();
    const conn = new RedisConnection({ client });
    await conn.ready;

    const errors: unknown[] = [];
    conn.on("error", (e: unknown) => errors.push(e));

    client.emit("error", new Error("boom"));
    fake(conn.subscriber).emit("error", new Error("boom"));
    expect(errors.length).toBe(2);

    conn.terminated = true;
    client.emit("error", new Error("ignored"));
    fake(conn.subscriber).emit("error", new Error("ignored"));
    expect(errors.length).toBe(2);
  });

  test("Should route subscribed messages to the limiter's store", async () => {
    const client = makeNodeClient();
    const conn = new RedisConnection({ client });
    await conn.ready;

    const { instance, limiter } = makeLimiterInstance();
    await conn.__addLimiter__(limiter);
    expect(conn.subscriber.subscribe).toHaveBeenCalledTimes(2);

    const onMessage = fake(conn.subscriber)._subscriptions["ch-one"];
    onMessage?.("hello");
    expect(instance._store.onMessage).toHaveBeenCalledWith("ch-one", "hello");

    await conn.__removeLimiter__(limiter);
    expect(conn.subscriber.unsubscribe).toHaveBeenCalledTimes(2);
    expect(conn.limiters["ch-one"]).toBeUndefined();
  });

  test("Should close clients on flush and destroy them otherwise", async () => {
    const client1 = makeNodeClient({}, { close: undefined });
    const conn1 = new RedisConnection({ client: client1 });
    await conn1.ready;
    await conn1.disconnect(true);
    expect(client1.close).toHaveBeenCalledTimes(1);
    expect(client1).not.toHaveProperty("subscriber");
    expect(fake(conn1.subscriber).quit).toHaveBeenCalledTimes(1);
    expect(conn1.subscriber.close).toBeUndefined();

    // The no-op error handlers installed by disconnect() must absorb late
    // client error events.
    fake(conn1.client).emit("error", new Error("late"));
    fake(conn1.subscriber).emit("error", new Error("late"));

    const client2 = makeNodeClient({}, { close: undefined, destroy: undefined });
    const conn2 = new RedisConnection({ client: client2 });
    await conn2.ready;
    await conn2.disconnect(false);
    expect(client2.destroy).toHaveBeenCalledTimes(1);
    expect(fake(conn2.subscriber).disconnect).toHaveBeenCalledTimes(1);
  });
});

describe("IORedisConnection", () => {
  test("Should refuse to build without a Redis reference or a pre-built client", () => {
    expect(() => new IORedisConnection({} as unknown as IORedisConnectionOptions)).toThrow(
      /requires a `Redis` library reference or a pre-built `client`/,
    );
  });

  test("Should resolve ready immediately for already-ready clients", async () => {
    const Redis = makeFakeRedis({ status: "ready" }, { status: "ready" });
    const conn = new IORedisConnection({ Redis, clientOptions: {} });

    const { client, subscriber } = await conn.ready;
    expect(client).toBe(conn.client);
    expect(subscriber).toBe(conn.subscriber);
    expect(client.defineCommand).toHaveBeenCalledTimes(Scripts.names.length);
  });

  test("Should wait for the ready event on a connecting client", async () => {
    const Redis = makeFakeRedis({ status: "ready" }, { status: "connect" });
    const conn = new IORedisConnection({ Redis, clientOptions: {} });

    let resolved = false;
    conn.ready.then(() => {
      resolved = true;
      return resolved;
    });
    await Promise.resolve();
    expect(resolved).toBe(false);

    fake(conn.subscriber).emit("ready");
    await conn.ready;
    expect(resolved).toBe(true);
  });

  test("Should build a Cluster subscriber when the client cannot duplicate", async () => {
    const Redis = makeFakeRedis();
    const client = makeIOClient({
      status: "ready",
      startupNodes: ["node-1"],
      options: { keyPrefix: "x" },
      duplicate: undefined,
    });

    // The options type forbids `Redis` alongside `client`, but the Cluster
    // fallback for a non-duplicating client reads it.
    const conn = new IORedisConnection(wrongType({ Redis, client }));

    await conn.ready;
    expect(Redis.Cluster.calls).toEqual([{ nodes: client.startupNodes, options: client.options }]);
  });

  test("Should route subscribed messages and clean up on removal", async () => {
    const Redis = makeFakeRedis({ status: "ready" }, { status: "ready" });
    const conn = new IORedisConnection({ Redis, clientOptions: {} });
    await conn.ready;

    const { instance, limiter } = makeLimiterInstance();
    await conn.__addLimiter__(limiter);
    expect(conn.subscriber.subscribe).toHaveBeenCalledTimes(2);

    fake(conn.subscriber).emit("message", "ch-one", "hello");
    expect(instance._store.onMessage).toHaveBeenCalledWith("ch-one", "hello");

    await conn.__removeLimiter__(limiter);
    expect(conn.subscriber.unsubscribe).toHaveBeenCalledTimes(2);
    expect(conn.limiters["ch-one"]).toBeUndefined();
  });

  test("Should trigger error events only while not terminated", async () => {
    const Redis = makeFakeRedis({ status: "ready" }, { status: "ready" });
    const conn = new IORedisConnection({ Redis, clientOptions: {} });
    await conn.ready;

    const errors = [];
    conn.on("error", (e: unknown) => errors.push(e));

    fake(conn.client).emit("error", new Error("boom"));
    fake(conn.subscriber).emit("error", new Error("boom"));
    expect(errors.length).toBe(2);

    conn.terminated = true;
    fake(conn.client).emit("error", new Error("ignored"));
    expect(errors.length).toBe(2);
  });

  test("Should quit or disconnect clients on disconnect", async () => {
    const Redis1 = makeFakeRedis({ status: "ready" }, { status: "ready" });
    const conn1 = new IORedisConnection({ Redis: Redis1, clientOptions: {} });
    await conn1.ready;
    conn1.client.quit = vi.fn<() => Promise<string>>(async () => "OK");
    conn1.subscriber.quit = vi.fn<() => Promise<string>>(async () => "OK");
    await conn1.disconnect(true);
    expect(conn1.client.quit).toHaveBeenCalledTimes(1);
    expect(conn1.subscriber.quit).toHaveBeenCalledTimes(1);

    // Late errors are absorbed by the no-op handlers disconnect() installs.
    fake(conn1.client).emit("error", new Error("late"));
    fake(conn1.subscriber).emit("error", new Error("late"));

    const Redis2 = makeFakeRedis({ status: "ready" }, { status: "ready" });
    const conn2 = new IORedisConnection({ Redis: Redis2, clientOptions: {} });
    await conn2.ready;
    conn2.client.disconnect = vi.fn<() => void>();
    conn2.subscriber.disconnect = vi.fn<() => void>();
    await conn2.disconnect(false);
    expect(conn2.client.disconnect).toHaveBeenCalledTimes(1);
    expect(conn2.subscriber.disconnect).toHaveBeenCalledTimes(1);
  });
});
