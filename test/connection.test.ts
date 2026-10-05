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

const makeEmitter = () => {
  const handlers: Record<string, Handler[]> = {};
  return {
    on(name: string, cb: Handler) {
      (handlers[name] ??= []).push(cb);
    },
    removeListener(name: string, cb: Handler) {
      handlers[name] = (handlers[name] ?? []).filter((h) => h !== cb);
    },
    listenerCount(name: string) {
      return handlers[name]?.length ?? 0;
    },
    emit(name: string, ...args: unknown[]) {
      const listeners = handlers[name] ?? [];
      // Like EventEmitter: an "error" nobody listens to throws.
      if (name === "error" && listeners.length === 0) throw args[0];
      for (const cb of listeners) cb(...args);
    },
  };
};

const makeNodeClient = (overrides: Overrides = {}, dupOverrides: Overrides = {}) => {
  const subscriptions: Record<string, Handler> = {};
  const client = {
    ...makeEmitter(),
    _subscriptions: subscriptions,
    isOpen: true,
    connect: vi.fn<() => Promise<void>>(async () => {
      client.isOpen = true;
    }),
    setMaxListeners: vi.fn<(n: number) => void>(),
    evalSha: vi.fn<
      (sha: string, options: { keys: string[]; arguments: string[] }) => Promise<unknown>
    >(async () => 1),
    eval: vi.fn<
      (script: string, options: { keys: string[]; arguments: string[] }) => Promise<unknown>
    >(async () => 1),
    sendCommand: vi.fn<() => Promise<string>>(async () => "OK"),
    publish: vi.fn<() => Promise<number>>(async () => 0),
    subscribe: vi.fn<(channel: string, cb: Handler) => Promise<void>>(async (channel, cb) => {
      subscriptions[channel] = cb;
    }),
    unsubscribe: vi.fn<() => Promise<void>>(async () => {}),
    close: vi.fn<() => Promise<void>>(async () => {}),
    quit: vi.fn<() => Promise<void>>(async () => {}),
    destroy: vi.fn<() => Promise<void>>(async () => {}),
    disconnect: vi.fn<() => void>(),
    // node-redis duplicates start disconnected.
    duplicate: (): NodeRedisClient => makeNodeClient({ isOpen: false, ...dupOverrides }),
  };
  Object.assign(client, overrides);
  // The fake implements only what RedisConnection calls.
  return wrongType<NodeRedisClient & typeof client>(client);
};

const makeIOClient = (overrides: Overrides = {}, dupOverrides: Overrides = {}) => {
  const client = {
    ...makeEmitter(),
    status: "ready",
    once(name: string, cb: Handler) {
      client.on(name, cb);
    },
    setMaxListeners: vi.fn<(n: number) => void>(),
    evalsha: vi.fn<(sha: string, numKeys: number, ...args: string[]) => Promise<unknown>>(
      async () => 1,
    ),
    eval: vi.fn<(script: string, numKeys: number, ...args: string[]) => Promise<unknown>>(
      async () => 1,
    ),
    publish: vi.fn<() => Promise<number>>(async () => 0),
    subscribe: vi.fn<(channel: string) => Promise<number>>(async () => 1),
    unsubscribe: vi.fn<(channel: string) => Promise<number>>(async () => 1),
    quit: vi.fn<() => Promise<string>>(async () => "OK"),
    disconnect: vi.fn<() => void>(),
    duplicate: (): IORedisClient => makeIOClient(dupOverrides),
  };
  Object.assign(client, overrides);
  // The fake implements only what IORedisConnection calls.
  return wrongType<IORedisClient & typeof client>(client);
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

const noscript = () => new Error("NOSCRIPT No matching script. Please use EVAL.");

describe("RedisConnection (node-redis)", () => {
  test("Should refuse to build without a client", () => {
    // {} is not a valid RedisConnectionOptions; the runtime rejection is the contract
    expect(() => new RedisConnection({} as unknown as RedisConnectionOptions)).toThrow(
      /requires a node-redis `client`/,
    );
  });

  test("Should refuse a client that hasn't been connected", () => {
    const client = makeNodeClient({ isOpen: false });
    expect(() => new RedisConnection({ client })).toThrow(/call `client.connect\(\)` first/);
    expect(client.connect).not.toHaveBeenCalled();
  });

  test("Should connect, listen to, and close the subscriber it duplicates", async () => {
    const conn = new RedisConnection({ client: makeNodeClient() });
    await conn.ready;
    expect(conn.subscriber.connect).toHaveBeenCalledTimes(1);
    expect(conn.subscriber.setMaxListeners).toHaveBeenCalledWith(0);
    expect(conn.subscriber.listenerCount("error")).toBe(1);

    await conn.disconnect(true);
    expect(conn.subscriber.close).toHaveBeenCalledTimes(1);
    // A no-op handler absorbs late socket errors from the closing subscriber.
    expect(() => conn.subscriber.emit("error", new Error("late"))).not.toThrow();
  });

  test("Should only send commands on the consumer's client", async () => {
    const client = makeNodeClient();
    const keys = Object.keys(client);
    const conn = new RedisConnection({ client });
    await conn.ready;

    const { limiter } = makeLimiterInstance();
    await conn.__addLimiter__(limiter);
    await conn.__runScript__("register", "id", []);
    await conn.__runCommand__(["get", "key"]);
    await conn.__removeLimiter__(limiter);
    await conn.disconnect(true);

    expect(Object.keys(client)).toEqual(keys);
    expect(client.listenerCount("error")).toBe(0);
    for (const method of [
      client.connect,
      client.setMaxListeners,
      client.close,
      client.quit,
      client.destroy,
      client.disconnect,
    ]) {
      expect(method).not.toHaveBeenCalled();
    }
  });

  test("Should run scripts by SHA and fall back to EVAL on NOSCRIPT", async () => {
    const client = makeNodeClient();
    client.evalSha.mockRejectedValueOnce(noscript());
    client.eval.mockResolvedValueOnce(7);
    const conn = new RedisConnection({ client });

    await expect(conn.__runScript__("register", "id", [null, 5])).resolves.toBe(7);
    const options = { keys: Scripts.keys("register", "id"), arguments: ["", "5"] };
    expect(client.evalSha).toHaveBeenCalledWith(Scripts.sha("register"), options);
    expect(client.eval).toHaveBeenCalledWith(Scripts.payload("register"), options);
  });

  test("Should rethrow script errors other than NOSCRIPT", async () => {
    const client = makeNodeClient();
    client.evalSha.mockRejectedValueOnce(new Error("ERR boom"));
    const conn = new RedisConnection({ client });

    await expect(conn.__runScript__("register", "id", [])).rejects.toThrow("ERR boom");
    expect(client.eval).not.toHaveBeenCalled();
  });

  test("Should trigger error events from its own subscriber only while not terminated", async () => {
    const conn = new RedisConnection({ client: makeNodeClient() });
    await conn.ready;

    const errors: unknown[] = [];
    conn.on("error", (e: unknown) => errors.push(e));

    conn.subscriber.emit("error", new Error("boom"));
    expect(errors.length).toBe(1);

    conn.terminated = true;
    conn.subscriber.emit("error", new Error("ignored"));
    expect(errors.length).toBe(1);
  });

  test("Should route subscribed messages to the limiter's store", async () => {
    const conn = new RedisConnection({ client: makeNodeClient() });
    await conn.ready;

    const { instance, limiter } = makeLimiterInstance();
    await conn.__addLimiter__(limiter);
    expect(conn.subscriber.subscribe).toHaveBeenCalledTimes(2);

    const onMessage = conn.subscriber._subscriptions["ch-one"];
    onMessage?.("hello");
    expect(instance._store.onMessage).toHaveBeenCalledWith("ch-one", "hello");

    await conn.__removeLimiter__(limiter);
    expect(conn.subscriber.unsubscribe).toHaveBeenCalledTimes(2);
    expect(conn.limiters["ch-one"]).toBeUndefined();
  });

  test("Should fall back to quit and disconnect on node-redis v4 subscribers", async () => {
    const conn1 = new RedisConnection({ client: makeNodeClient({}, { close: undefined }) });
    await conn1.ready;
    await conn1.disconnect(true);
    expect(conn1.subscriber.quit).toHaveBeenCalledTimes(1);

    const conn2 = new RedisConnection({ client: makeNodeClient({}, { destroy: undefined }) });
    await conn2.ready;
    await conn2.disconnect(false);
    expect(conn2.subscriber.disconnect).toHaveBeenCalledTimes(1);
  });

  test("Should leave a consumer-supplied subscriber open and unlistened", async () => {
    const subscriber = makeNodeClient();
    const conn = new RedisConnection({ client: makeNodeClient(), subscriber });
    await conn.ready;
    expect(conn.subscriber).toBe(subscriber);

    await conn.disconnect(true);
    expect(subscriber.listenerCount("error")).toBe(0);
    expect(subscriber.setMaxListeners).not.toHaveBeenCalled();
    expect(subscriber.close).not.toHaveBeenCalled();
  });
});

describe("IORedisConnection", () => {
  test("Should refuse to build without a client", () => {
    expect(() => new IORedisConnection({} as unknown as IORedisConnectionOptions)).toThrow(
      /requires an ioredis `client`/,
    );
  });

  test("Should resolve ready once the subscriber it duplicates is ready", async () => {
    // The consumer's client still connecting doesn't hold up `ready`: its
    // commands queue until it connects.
    const conn = new IORedisConnection({
      client: makeIOClient({ status: "connect" }, { status: "connect" }),
    });

    let resolved = false;
    void conn.ready.then(() => {
      resolved = true;
      return resolved;
    });
    await Promise.resolve();
    expect(resolved).toBe(false);

    conn.subscriber.emit("ready");
    await conn.ready;
    expect(resolved).toBe(true);
  });

  test("Should listen to and close the subscriber it duplicates", async () => {
    const conn = new IORedisConnection({ client: makeIOClient() });
    await conn.ready;
    expect(conn.subscriber.setMaxListeners).toHaveBeenCalledWith(0);
    expect(conn.subscriber.listenerCount("error")).toBe(1);

    await conn.disconnect(true);
    expect(conn.subscriber.quit).toHaveBeenCalledTimes(1);
    expect(conn.subscriber.listenerCount("message")).toBe(0);
    expect(() => conn.subscriber.emit("error", new Error("late"))).not.toThrow();

    const conn2 = new IORedisConnection({ client: makeIOClient() });
    await conn2.ready;
    await conn2.disconnect(false);
    expect(conn2.subscriber.disconnect).toHaveBeenCalledTimes(1);
  });

  test("Should only send commands on the consumer's client", async () => {
    const client = makeIOClient();
    const keys = Object.keys(client);
    const conn = new IORedisConnection({ client });
    await conn.ready;

    const { limiter } = makeLimiterInstance();
    await conn.__addLimiter__(limiter);
    await conn.__runScript__("register", "id", []);
    await conn.__removeLimiter__(limiter);
    await conn.disconnect(true);

    // No script methods (or anything else) defined on it.
    expect(Object.keys(client)).toEqual(keys);
    expect(client.listenerCount("error")).toBe(0);
    expect(client.listenerCount("ready")).toBe(0);
    expect(client.setMaxListeners).not.toHaveBeenCalled();
    expect(client.quit).not.toHaveBeenCalled();
    expect(client.disconnect).not.toHaveBeenCalled();
  });

  test("Should run scripts by SHA and fall back to EVAL on NOSCRIPT", async () => {
    const client = makeIOClient();
    client.evalsha.mockRejectedValueOnce(noscript());
    client.eval.mockResolvedValueOnce(7);
    const conn = new IORedisConnection({ client });

    await expect(conn.__runScript__("register", "id", [null, 5])).resolves.toBe(7);
    const keys = Scripts.keys("register", "id");
    const args = [keys.length, ...keys, "", "5"];
    expect(client.evalsha).toHaveBeenCalledWith(Scripts.sha("register"), ...args);
    expect(client.eval).toHaveBeenCalledWith(Scripts.payload("register"), ...args);
  });

  test("Should route subscribed messages and clean up on removal", async () => {
    const conn = new IORedisConnection({ client: makeIOClient() });
    await conn.ready;

    const { instance, limiter } = makeLimiterInstance();
    await conn.__addLimiter__(limiter);
    expect(conn.subscriber.subscribe).toHaveBeenCalledTimes(2);

    conn.subscriber.emit("message", "ch-one", "hello");
    expect(instance._store.onMessage).toHaveBeenCalledWith("ch-one", "hello");

    await conn.__removeLimiter__(limiter);
    expect(conn.subscriber.unsubscribe).toHaveBeenCalledTimes(2);
    expect(conn.limiters["ch-one"]).toBeUndefined();
  });

  test("Should trigger error events from its own subscriber only while not terminated", async () => {
    const conn = new IORedisConnection({ client: makeIOClient() });
    await conn.ready;

    const errors: unknown[] = [];
    conn.on("error", (e: unknown) => errors.push(e));

    conn.subscriber.emit("error", new Error("boom"));
    expect(errors.length).toBe(1);

    conn.terminated = true;
    conn.subscriber.emit("error", new Error("ignored"));
    expect(errors.length).toBe(1);
  });

  test("Should leave a consumer-supplied subscriber open, removing only its message listener", async () => {
    const subscriber = makeIOClient();
    const conn = new IORedisConnection({ client: makeIOClient(), subscriber });
    await conn.ready;
    expect(conn.subscriber).toBe(subscriber);
    expect(subscriber.listenerCount("message")).toBe(1);

    await conn.disconnect(true);
    expect(subscriber.listenerCount("message")).toBe(0);
    expect(subscriber.listenerCount("error")).toBe(0);
    expect(subscriber.setMaxListeners).not.toHaveBeenCalled();
    expect(subscriber.quit).not.toHaveBeenCalled();
  });
});
