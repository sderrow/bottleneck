import { describe, expect } from "vitest";
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

/** Resolves with the first "error" event the emitter triggers. */
const firstError = (emitter: { on(event: "error", cb: (e: unknown) => void): unknown }) =>
  new Promise((resolve) => {
    emitter.on("error", resolve);
  });

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
    test("Should leave its connection open when a limiter disconnects", async ({
      makeLimiter,
      makeConnection,
    }) => {
      const connection = makeConnection();
      const limiter = makeLimiter({ connection });
      await limiter.ready();

      await limiter.disconnect();

      expect([isOpen(connection.client), isOpen(connection.subscriber)]).toEqual([true, true]);
      const next = makeLimiter({ connection });
      await expect(next.schedule(() => Promise.resolve("ok"))).resolves.toBe("ok");
    });

    test("Should disconnect a Group's limiters but leave its connection open", async ({
      makeGroup,
      makeConnection,
    }) => {
      const connection = makeConnection();
      const group = makeGroup({ connection });
      await Promise.all([group.key("a").ready(), group.key("b").ready()]);
      expect(Object.keys(connection.limiters)).toHaveLength(4);

      await group.disconnect();

      expect(connection.limiters).toEqual({});
      expect([isOpen(connection.client), isOpen(connection.subscriber)]).toEqual([true, true]);
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

    test("Should send its subscriber's errors to the connection and every limiter on it", async ({
      makeClient,
      makeConnection,
      makeLimiter,
    }) => {
      const connection = makeConnection({
        client: makeClient(failingClientOptions(), { expectErrors: true }),
      });
      const limiters = [1, 2].map(() => makeLimiter({ connection }, { expectErrors: true }));

      await expect(
        Promise.all([connection, ...limiters].map((emitter) => firstError(emitter))),
      ).resolves.toHaveLength(3);
    });
  });
});
