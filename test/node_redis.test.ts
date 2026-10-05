import * as Redis from "redis";
import { describe, expect, vi } from "vitest";
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
    const connection = makeConnection({
      Redis,
      clientOptions: buildClientOptions("redis"),
    });
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
    makeConnection,
  }) => {
    expect.hasAssertions();
    return new Promise<void>((resolve, reject) => {
      const connection = makeConnection({
        Redis,
        clientOptions: {
          socket: {
            port: 1,
            reconnectStrategy: () => false,
          },
        },
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

// Pins what a connection does to a client passed in to it. v5 stops all of
// this (the client belongs to the consumer); these tests change with it.
describe("node_redis passed-in client side effects", () => {
  test("Should connect the client and install listeners, then strip and close it", async ({
    makeConnection,
  }) => {
    const client = Redis.createClient(buildClientOptions("redis"));
    const consumerListener = vi.fn<() => void>();
    client.on("error", consumerListener);
    expect(client.isOpen).toBe(false);

    const connection = makeConnection({ client });
    await connection.ready;
    expect(client.isOpen).toBe(true);
    expect(client.listenerCount("error")).toBe(2);
    expect(client.getMaxListeners()).toBe(0);

    await connection.disconnect(true);
    expect(client.listeners("error")).not.toContain(consumerListener);
    expect(client.listenerCount("error")).toBe(1);
    expect(client.isOpen).toBe(false);
  });

  test("Should destroy a passed-in client on disconnect(false)", async ({ makeConnection }) => {
    const client = Redis.createClient(buildClientOptions("redis"));
    await client.connect();
    const connection = makeConnection({ client });
    await connection.ready;

    await connection.disconnect(false);

    expect(client.isOpen).toBe(false);
  });
});
