import { AsyncLocalStorage } from "node:async_hooks";
import { describe, expect, vi } from "vitest";
import { runDetached, setDetachedTimeout } from "../src/async-context";
import { useFakeClock } from "./helpers/clock";
import { deferred, enqueued, test } from "./helpers/test-api";

useFakeClock();

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

  test("runDetached and setDetachedTimeout run outside the caller's context", async () => {
    const als = new AsyncLocalStorage<string>();

    const direct = als.run("caller-store", () => runDetached(() => als.getStore()));
    expect(direct).toBeUndefined();

    const pending = als.run(
      "caller-store",
      () =>
        new Promise((resolve) => {
          setDetachedTimeout(() => resolve(als.getStore()), 10);
        }),
    );
    // Fake timers (local project) or real timers (redis projects): both work
    // because setTimeout is looked up when setDetachedTimeout is called.
    // Advance outside the ALS scope: fake-timer callbacks execute in the
    // advancing context, so advancing inside `als.run` would leak the store
    // back in regardless of the timer's creation context.
    if (vi.isFakeTimers()) {
      await vi.advanceTimersByTime(10);
    }
    expect(await pending).toBeUndefined();
  });
});
