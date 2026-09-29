import { AsyncLocalStorage } from "node:async_hooks";
import { describe, expect } from "vitest";
import { useFakeClock } from "./helpers/clock";
import { deferred, enqueued, test } from "./helpers/test-api";

useFakeClock();

function innerTask(): never {
  throw new Error("boom-inner");
}

describe("Async context", () => {
  test("preserves AsyncLocalStorage for jobs that wait in the queue", async ({ makeLimiter }) => {
    const als = new AsyncLocalStorage<string>();
    const limiter = makeLimiter({ maxConcurrent: 1 });

    // Hold the first job so the second job can only run after release() —
    // by which time the drain executes from a foreign async context.
    const hold = deferred();
    const first = limiter.schedule(() => hold.signal);
    await enqueued(limiter);

    const second = als.run("expected-store", () => limiter.schedule(() => als.getStore()));
    await enqueued(limiter);

    hold.release();
    await expect(second).resolves.toBe("expected-store");
    await first;
  });

  test("preserves AsyncLocalStorage through chained limiters", async ({ makeLimiter }) => {
    const als = new AsyncLocalStorage<string>();
    // The inner limiter uses a local datastore (like the chained limiters in
    // cluster-coordination.test.ts): two Redis-backed limiters sharing one
    // maxConcurrent slot would deadlock by design, independent of context.
    const inner = makeLimiter({ id: "chain-inner", maxConcurrent: 1, datastore: "local" });
    const outer = makeLimiter({ maxConcurrent: 1 });
    outer.chain(inner);

    const hold = deferred();
    const first = outer.schedule(() => hold.signal);
    await enqueued(outer);

    const second = als.run("expected-store", () => outer.schedule(() => als.getStore()));
    await enqueued(outer);

    hold.release();
    await expect(second).resolves.toBe("expected-store");
    await first;
  });

  test("preserves AsyncLocalStorage on the chained limiter's schedule path", async ({
    makeLimiter,
  }) => {
    const als = new AsyncLocalStorage<string>();
    // Local inner datastore, as in the test above: two Redis-backed limiters
    // sharing one maxConcurrent slot would deadlock by design.
    const inner = makeLimiter({ id: "chain-received-inner", maxConcurrent: 1, datastore: "local" });
    const outer = makeLimiter({ maxConcurrent: 1 });
    outer.chain(inner);

    const received: (string | undefined)[] = [];
    inner.on("received", () => {
      received.push(als.getStore());
    });

    const hold = deferred();
    const first = outer.schedule(() => hold.signal);
    await enqueued(outer);

    const second = als.run("expected-store", () => outer.schedule(() => als.getStore()));
    await enqueued(outer);

    hold.release();
    await expect(second).resolves.toBe("expected-store");
    await first;
    // Each outer execution schedules exactly one inner job: the first outside
    // any ALS context, the second inside "expected-store".
    expect(received).toEqual([undefined, "expected-store"]);
  });

  test.runIf(process.env.DATASTORE != null)(
    "runs the heartbeat outside the async context that constructed the limiter",
    async ({ makeLimiter }) => {
      const als = new AsyncLocalStorage<string>();
      // A heartbeat that inherited this store would keep reporting into the
      // constructing caller (e.g. its trace) for the limiter's whole lifetime.
      const limiter = als.run("constructor-store", () => makeLimiter({ heartbeatInterval: 20 }));

      const heartbeatStore = new Promise((resolve) => {
        limiter.on("debug", (message: string) => {
          if (message.includes("heartbeat.lua")) resolve(als.getStore());
        });
      });

      await expect(heartbeatStore).resolves.toBeUndefined();
    },
  );

  test("rejection stack includes both the task and the schedule call site", async ({
    makeLimiter,
  }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });

    function outerScheduleTask() {
      return limiter.schedule(() => innerTask());
    }

    const error = await outerScheduleTask().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(Error);
    const stack = (error as Error).stack ?? "";
    expect(stack).toContain("boom-inner");
    expect(stack).toContain("innerTask");
    expect(stack).toContain("outerScheduleTask");
    expect(stack).toContain("From previous Bottleneck.schedule location:");
  });

  test("retrying the same error instance augments the stack only once", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    const failure = new Error("boom-retry");
    let attempts = 0;
    let failures = 0;
    limiter.on("failed", () => {
      failures++;
      if (attempts < 2) {
        return 10;
      }
      return undefined;
    });

    await expect(
      limiter.schedule(() => {
        attempts++;
        throw failure;
      }),
    ).rejects.toBe(failure);
    expect(attempts).toBe(2);
    expect(failures).toBe(2);
    const occurrences = (failure.stack ?? "").split("From previous Bottleneck.schedule location:");
    expect(occurrences).toHaveLength(2);
  });
});
