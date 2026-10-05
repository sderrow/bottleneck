import * as Redis from "redis";
import { describe, expect, onTestFinished, vi } from "vitest";
import { test } from "./helpers/test-api";
import buildClientOptions from "./redis-client-options";

describe("node_redis-only", () => {
  test("Should accept node_redis lib override", ({ makeLimiter }) => {
    const limiter = makeLimiter({
      maxConcurrent: 2,
      Redis,
    });

    expect(limiter.datastore).toStrictEqual("redis");
  });

  test("Should accept existing connections", async ({
    harness: h,
    makeLimiter,
    makeConnection,
  }) => {
    const connection = makeConnection();
    const limiter = makeLimiter({
      minTime: 50,
      connection,
    });

    const p1 = limiter.schedule(h.promise, null, 1);
    const p2 = limiter.schedule(h.promise, null, 2);

    await h.flushLimiter(limiter);
    expect(h.log).toHaveCallOrder([[1], [2]]);
    expect(h).toHaveFinalCallAt(50);
    expect(limiter.connection).toBe(connection);
    expect(limiter.datastore).toStrictEqual("redis");

    await limiter.disconnect();
    expect(limiter.clients().client).toHaveProperty("isReady", true);
    await Promise.all([expect(p1).resolves.toEqual([1]), expect(p2).resolves.toEqual([2])]);
  });

  test("Should accept existing redis clients", async ({
    harness: h,
    makeLimiter,
    makeConnection,
  }) => {
    const client = Redis.createClient(buildClientOptions("redis"));
    onTestFinished(() => client.disconnect());
    await client.connect();

    const connection = makeConnection({ client });
    const limiter = makeLimiter({
      minTime: 50,
      connection,
    });

    const p1 = limiter.schedule(h.promise, null, 1);
    const p2 = limiter.schedule(h.promise, null, 2);

    await h.flushLimiter(limiter);
    expect(h.log).toHaveCallOrder([[1], [2]]);
    expect(h).toHaveFinalCallAt(50);
    expect(limiter.clients().client).toBe(client);
    expect(limiter.connection).toBe(connection);
    expect(limiter.datastore).toStrictEqual("redis");

    await limiter.disconnect();
    expect(limiter.clients().client).toHaveProperty("isReady", true);
    await Promise.all([expect(p1).resolves.toEqual([1]), expect(p2).resolves.toEqual([2])]);
  });

  test("Should trigger error events on the shared connection", ({
    makeLimiter,
    makeClient,
    makeConnection,
  }) => {
    expect.hasAssertions();
    return new Promise<void>((resolve, reject) => {
      const connection = makeConnection({
        client: makeClient(
          { socket: { port: 1, reconnectStrategy: () => false } },
          { expectErrors: true },
        ),
      });
      connection.ready.catch(() => {});
      let fired = false;
      const limiter = makeLimiter({ connection }, { expectErrors: true });
      connection.on("error", (_err: unknown) => {
        if (fired) return;
        fired = true;
        expect(limiter.datastore).toStrictEqual("redis");
        connection.disconnect();
        resolve();
      });

      limiter.on("error", (err) => {
        if (fired) return;
        reject(err);
      });
    });
  });
});

// The client belongs to the consumer: the connection only sends commands on
// it, never connecting it, adding listeners, or closing it.
describe("node_redis passed-in client", () => {
  test("Should require a connected client and only send commands on it", async ({
    makeLimiter,
    makeConnection,
  }) => {
    const client = Redis.createClient(buildClientOptions("redis"));
    onTestFinished(() => (client.isOpen ? client.disconnect() : undefined));
    const consumerListener = vi.fn<() => void>();
    client.on("error", consumerListener);
    expect(() => makeConnection({ client })).toThrow(/call `client.connect\(\)` first/);

    await client.connect();
    const maxListeners = client.getMaxListeners();
    const connection = makeConnection({ client });
    const limiter = makeLimiter({ connection });
    await expect(limiter.schedule(() => Promise.resolve("ran"))).resolves.toBe("ran");
    expect(client.listeners("error")).toEqual([consumerListener]);
    expect(client.getMaxListeners()).toBe(maxListeners);

    await limiter.disconnect();
    await connection.disconnect(true);
    expect(client.listeners("error")).toEqual([consumerListener]);
    expect(client.isOpen).toBe(true);
  });

  test("Should leave a passed-in client open on disconnect(false)", async ({ makeConnection }) => {
    const client = Redis.createClient(buildClientOptions("redis"));
    onTestFinished(() => (client.isOpen ? client.disconnect() : undefined));
    await client.connect();
    const connection = makeConnection({ client });
    await connection.ready;

    await connection.disconnect(false);

    await expect(client.ping()).resolves.toBe("PONG");
  });
});
