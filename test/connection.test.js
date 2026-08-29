import { describe, test, expect, vi } from "vitest";
const Scripts = require("../src/cluster/Scripts.js");
const RedisConnection = require("../src/cluster/RedisConnection.js");
const IORedisConnection = require("../src/cluster/IORedisConnection.js");

// These are datastore-independent unit tests: both connection classes are
// driven with fake clients, so the file is deterministic in every test
// project (local + redis) without touching the shared Redis container.

const makeNodeClient = (overrides = {}, dupOverrides = {}) => {
  const handlers = {};
  const client = {
    isOpen: true,
    on(name, cb) {
      (handlers[name] ??= []).push(cb);
    },
    emit(name, ...args) {
      for (const cb of handlers[name] ?? []) cb(...args);
    },
    removeAllListeners(name) {
      if (name != null) delete handlers[name];
    },
    scriptLoad: vi.fn(async (payload) => `sha-${payload.length}`),
    evalSha: vi.fn(async () => 1),
    sendCommand: vi.fn(async () => "OK"),
    subscribe: vi.fn(async (channel, cb) => {
      (handlers.__subscriptions ??= {})[channel] = cb;
    }),
    unsubscribe: vi.fn(async () => {}),
    close: vi.fn(async () => {}),
    quit: vi.fn(async () => {}),
    destroy: vi.fn(async () => {}),
    disconnect: vi.fn(),
    ...overrides,
  };
  client._handlers = handlers;
  client.duplicate = () => makeNodeClient(dupOverrides);
  return client;
};

const makeIOClient = (overrides = {}) => {
  const handlers = {};
  const client = {
    status: "connect",
    on(name, cb) {
      (handlers[name] ??= []).push(cb);
    },
    once(name, cb) {
      (handlers[name] ??= []).push(cb);
    },
    emit(name, ...args) {
      for (const cb of handlers[name] ?? []) cb(...args);
    },
    setMaxListeners() {},
    defineCommand: vi.fn(),
    subscribe: vi.fn(async () => 1),
    unsubscribe: vi.fn(async () => 1),
    ...overrides,
  };
  client._handlers = handlers;
  return client;
};

const makeFakeRedis = (mainOverrides = {}, subOverrides = {}) => {
  // Plain functions, not vi.fn(): `new FakeRedis(...)` must return the fake
  // client object the factory builds.
  const FakeRedis = function () {
    return makeIOClient({ ...mainOverrides, duplicate: () => makeIOClient(subOverrides) });
  };
  FakeRedis.Cluster = function (nodes, options) {
    FakeRedis.Cluster.calls.push({ nodes, options });
    return makeIOClient({ status: "ready" });
  };
  FakeRedis.Cluster.calls = [];
  return FakeRedis;
};

const makeLimiterInstance = () => ({
  channel: () => "ch-one",
  channel_client: () => "ch-two",
  _store: { onMessage: vi.fn() },
});

describe("RedisConnection (node-redis)", () => {
  test("Should refuse to build without a Redis reference or a pre-built client", () => {
    expect(() => new RedisConnection({})).toThrow(
      /requires a `Redis` library reference or a pre-built `client`/,
    );
  });

  test("Should connect a client that is not open", async () => {
    const connect = vi.fn(async () => {});
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
    expect(client.evalSha.mock.calls[1][1].arguments[0]).toBe("");
  });

  test("Should reject ready when script loading fails", async () => {
    const client = makeNodeClient({
      scriptLoad: vi.fn(async () => {
        throw new Error("script load failed");
      }),
    });
    const conn = new RedisConnection({ client });

    await expect(conn.ready).rejects.toThrow("script load failed");
  });

  test("Should swallow script loading failures after termination", async () => {
    const client = makeNodeClient({
      scriptLoad: vi.fn(async () => {
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

    const errors = [];
    conn.on("error", (e) => errors.push(e));

    client.emit("error", new Error("boom"));
    conn.subscriber.emit("error", new Error("boom"));
    expect(errors.length).toBe(2);

    conn.terminated = true;
    client.emit("error", new Error("ignored"));
    conn.subscriber.emit("error", new Error("ignored"));
    expect(errors.length).toBe(2);
  });

  test("Should route subscribed messages to the limiter's store", async () => {
    const client = makeNodeClient();
    const conn = new RedisConnection({ client });
    await conn.ready;

    const instance = makeLimiterInstance();
    await conn.__addLimiter__(instance);
    expect(conn.subscriber.subscribe).toHaveBeenCalledTimes(2);

    const onMessage = conn.subscriber._handlers.__subscriptions["ch-one"];
    onMessage("hello");
    expect(instance._store.onMessage).toHaveBeenCalledWith("ch-one", "hello");

    await conn.__removeLimiter__(instance);
    expect(conn.subscriber.unsubscribe).toHaveBeenCalledTimes(2);
    expect(conn.limiters["ch-one"]).toBeUndefined();
  });

  test("Should close clients on flush and destroy them otherwise", async () => {
    const client1 = makeNodeClient({}, { close: undefined });
    const conn1 = new RedisConnection({ client: client1 });
    await conn1.ready;
    await conn1.disconnect(true);
    expect(client1.close).toHaveBeenCalledTimes(1);
    expect(client1.subscriber).toBeUndefined();
    expect(conn1.subscriber.quit).toHaveBeenCalledTimes(1);
    expect(conn1.subscriber.close).toBeUndefined();

    // The no-op error handlers installed by disconnect() must absorb late
    // client error events.
    conn1.client.emit("error", new Error("late"));
    conn1.subscriber.emit("error", new Error("late"));

    const client2 = makeNodeClient({}, { close: undefined, destroy: undefined });
    const conn2 = new RedisConnection({ client: client2 });
    await conn2.ready;
    await conn2.disconnect(false);
    expect(client2.destroy).toHaveBeenCalledTimes(1);
    expect(conn2.subscriber.disconnect).toHaveBeenCalledTimes(1);
  });
});

describe("IORedisConnection", () => {
  test("Should refuse to build without a Redis reference or a pre-built client", () => {
    expect(() => new IORedisConnection({})).toThrow(
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

    conn.subscriber.emit("ready");
    await conn.ready;
    expect(resolved).toBe(true);
  });

  test("Should build a Cluster subscriber when the client cannot duplicate", async () => {
    const Redis = makeFakeRedis();
    const client = makeIOClient({ status: "ready" });
    client.startupNodes = ["node-1"];
    client.options = { keyPrefix: "x" };
    delete client.duplicate;

    const conn = new IORedisConnection({ Redis, client });

    await conn.ready;
    expect(Redis.Cluster.calls).toEqual([{ nodes: client.startupNodes, options: client.options }]);
  });

  test("Should route subscribed messages and clean up on removal", async () => {
    const Redis = makeFakeRedis({ status: "ready" }, { status: "ready" });
    const conn = new IORedisConnection({ Redis, clientOptions: {} });
    await conn.ready;

    const instance = makeLimiterInstance();
    await conn.__addLimiter__(instance);
    expect(conn.subscriber.subscribe).toHaveBeenCalledTimes(2);

    conn.subscriber.emit("message", "ch-one", "hello");
    expect(instance._store.onMessage).toHaveBeenCalledWith("ch-one", "hello");

    await conn.__removeLimiter__(instance);
    expect(conn.subscriber.unsubscribe).toHaveBeenCalledTimes(2);
    expect(conn.limiters["ch-one"]).toBeUndefined();
  });

  test("Should trigger error events only while not terminated", async () => {
    const Redis = makeFakeRedis({ status: "ready" }, { status: "ready" });
    const conn = new IORedisConnection({ Redis, clientOptions: {} });
    await conn.ready;

    const errors = [];
    conn.on("error", (e) => errors.push(e));

    conn.client.emit("error", new Error("boom"));
    conn.subscriber.emit("error", new Error("boom"));
    expect(errors.length).toBe(2);

    conn.terminated = true;
    conn.client.emit("error", new Error("ignored"));
    expect(errors.length).toBe(2);
  });

  test("Should quit or disconnect clients on disconnect", async () => {
    const Redis1 = makeFakeRedis({ status: "ready" }, { status: "ready" });
    const conn1 = new IORedisConnection({ Redis: Redis1, clientOptions: {} });
    await conn1.ready;
    conn1.client.quit = vi.fn(async () => "OK");
    conn1.subscriber.quit = vi.fn(async () => "OK");
    await conn1.disconnect(true);
    expect(conn1.client.quit).toHaveBeenCalledTimes(1);
    expect(conn1.subscriber.quit).toHaveBeenCalledTimes(1);

    // Late errors are absorbed by the no-op handlers disconnect() installs.
    conn1.client.emit("error", new Error("late"));
    conn1.subscriber.emit("error", new Error("late"));

    const Redis2 = makeFakeRedis({ status: "ready" }, { status: "ready" });
    const conn2 = new IORedisConnection({ Redis: Redis2, clientOptions: {} });
    await conn2.ready;
    conn2.client.disconnect = vi.fn();
    conn2.subscriber.disconnect = vi.fn();
    await conn2.disconnect(false);
    expect(conn2.client.disconnect).toHaveBeenCalledTimes(1);
    expect(conn2.subscriber.disconnect).toHaveBeenCalledTimes(1);
  });
});
