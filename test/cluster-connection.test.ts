import { describe, expect } from "vitest";
import { defined } from "./helpers/defined";
import { runCommand } from "./helpers/store";
import { test, waitForState } from "./helpers/test-api";

// Characterization tests for the Redis connection layer: how limiters and
// Groups use, share, and tear down connections. Tests under "Ownership" pin
// behavior that v5 intentionally changes (who closes what); the rest must keep
// passing unchanged.

// node-redis v4/v5 shape fails fast; ioredis stays flat.
const failingClientOptions = () =>
  process.env.DATASTORE === "redis"
    ? { socket: { port: 1, reconnectStrategy: () => false } }
    : { port: 1, retryStrategy: () => null };

/** Whether a raw ioredis / node-redis client still has an open socket. */
function isOpen(client: unknown): boolean {
  if (typeof client !== "object" || client == null) throw new Error("Not a client");
  if ("status" in client) return client.status !== "end"; // ioredis
  if ("isOpen" in client) return client.isOpen === true; // node-redis
  throw new Error("Unknown client");
}

describe("Cluster connection", () => {
  test("Should keep running scripts after SCRIPT FLUSH", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    await limiter.ready();
    await expect(limiter.schedule(() => Promise.resolve(1))).resolves.toBe(1);

    await runCommand(limiter, "script", ["flush"]);

    await expect(limiter.schedule(() => Promise.resolve(2))).resolves.toBe(2);
    expect(await limiter.running()).toBe(0);
  });

  test("Should stop delivering to a disconnected limiter while its connection's other limiters keep working", async ({
    makeLimiter,
    makeConnection,
  }) => {
    const connection = makeConnection();
    const a = makeLimiter({ id: "shared-a", connection });
    const b = makeLimiter({ id: "shared-b", connection });
    // Both publishers share one connection and publish in order, and a and b
    // share one subscriber, so Redis delivers in publish order: once b has its
    // message, a would already have had its own.
    const publishers = makeConnection();
    const pubA = makeLimiter({ id: "shared-a", connection: publishers });
    const pubB = makeLimiter({ id: "shared-b", connection: publishers });
    await Promise.all([a.ready(), b.ready(), pubA.ready(), pubB.ready()]);

    const received: string[] = [];
    a.on("message", (message) => received.push(`a:${message}`));
    b.on("message", (message) => received.push(`b:${message}`));

    await a.disconnect();
    await pubA.publish("1");
    await pubB.publish("2");

    await waitForState(() => expect(received).toContain("b:2"));
    expect(received).toEqual(["b:2"]);
    await expect(b.schedule(() => Promise.resolve("still works"))).resolves.toBe("still works");
  });

  describe("Ownership", () => {
    test("Should close a connection the limiter built when it disconnects", async ({
      makeLimiter,
    }) => {
      const limiter = makeLimiter();
      await limiter.ready();
      const { client, subscriber } = defined(limiter.connection);
      expect([isOpen(client), isOpen(subscriber)]).toEqual([true, true]);

      await limiter.disconnect();

      await waitForState(() =>
        expect([isOpen(client), isOpen(subscriber)]).toEqual([false, false]),
      );
    });

    test("Should close a connection the Group built when it disconnects", async ({ makeGroup }) => {
      const group = makeGroup({ datastore: process.env.DATASTORE });
      const connection = defined(group.connection);
      await connection.ready;
      const { client, subscriber } = connection;

      await group.disconnect();

      await waitForState(() =>
        expect([isOpen(client), isOpen(subscriber)]).toEqual([false, false]),
      );
    });

    test("Should leave a shared connection open when a Group disconnects", async ({
      makeGroup,
      makeLimiter,
      makeConnection,
    }) => {
      const connection = makeConnection();
      const group = makeGroup({ connection });
      const sibling = makeLimiter({ connection });
      await Promise.all([group.key("a").ready(), sibling.ready()]);

      await group.disconnect();

      expect([isOpen(connection.client), isOpen(connection.subscriber)]).toEqual([true, true]);
      await expect(sibling.schedule(() => Promise.resolve("ok"))).resolves.toBe("ok");
    });

    test("Should send errors from a connection the Group built to the Group", async ({
      makeGroup,
    }) => {
      const group = makeGroup({
        datastore: process.env.DATASTORE,
        clientOptions: failingClientOptions(),
      });
      defined(group.connection).ready.catch(() => {});

      await expect(
        new Promise((resolve) => {
          group.on("error", resolve);
        }),
      ).resolves.toBeTruthy();
    });
  });
});
