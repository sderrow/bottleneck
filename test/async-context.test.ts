import { AsyncLocalStorage } from "node:async_hooks";
import { describe, expect, vi } from "vitest";
import { runDetached, setDetachedTimeout } from "../src/async-context";
import Bottleneck from "../src/Bottleneck";
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

  test("runDetached and setDetachedTimeout run outside the caller's context", async () => {
    const als = new AsyncLocalStorage<string>();

    const direct = als.run("caller-store", () => runDetached(() => als.getStore()));
    expect(direct).toBeUndefined();

    let pending: Promise<unknown>;
    als.run("caller-store", () => {
      pending = new Promise((resolve) => {
        setDetachedTimeout(() => resolve(als.getStore()), 10);
      });
    });
    // Fake timers (local project) or real timers (redis projects): both work
    // because setTimeout is looked up when setDetachedTimeout is called.
    // Advance outside the ALS scope: fake-timer callbacks execute in the
    // advancing context, so advancing inside `als.run` would leak the store
    // back in regardless of the timer's creation context.
    if (vi.isFakeTimers()) {
      await vi.advanceTimersByTime(10);
    }
    expect(await pending!).toBeUndefined();
  });

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
    expect(stack).toContain("From previous Bottleneck.schedule location (");
    // Cut at the outermost schedule(): no Bottleneck.schedule frame remains.
    const section = stack.split("From previous Bottleneck.schedule location")[1] ?? "";
    expect(section).not.toContain("at Bottleneck.schedule");
    expect(section).toContain("outerScheduleTask");
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
    const occurrences = (failure.stack ?? "").split("From previous Bottleneck.schedule location");
    expect(occurrences).toHaveLength(2);
  });

  test("schedule() does not format stacks until a task fails", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 5 });
    let formats = 0;
    const original = Error.prepareStackTrace;
    Error.prepareStackTrace = ((...args: unknown[]) => {
      formats++;
      return (original as (...a: unknown[]) => unknown)?.(...args);
    }) as typeof Error.prepareStackTrace;
    try {
      await Promise.all(
        Array.from({ length: 20 }, (_, i) => limiter.schedule(() => Promise.resolve(i))),
      );
    } finally {
      Error.prepareStackTrace = original;
    }
    expect(formats).toBe(0);
  });

  test("chained limiters each append their schedule location with the limiter id", async ({
    makeLimiter,
  }) => {
    const inner = makeLimiter({ id: "chain-inner-stack", maxConcurrent: 1, datastore: "local" });
    const outer = makeLimiter({ id: "chain-outer-stack", maxConcurrent: 1 });
    outer.chain(inner);

    const error = await outer
      .schedule(() => {
        throw new Error("boom-chain");
      })
      .catch((e: unknown) => e);
    const stack = (error as Error).stack ?? "";
    expect(stack).toContain("boom-chain");
    // Redis projects prefix limiter ids per test; match the stable suffix.
    expect(stack).toContain("chain-inner-stack):");
    expect(stack).toContain("chain-outer-stack):");
  });

  test("chained schedule locations are capped", async ({ makeLimiter }) => {
    const limiters = Array.from(
      { length: 5 },
      (_, i) =>
        makeLimiter({
          id: `chain-cap-${i}`,
          datastore: "local",
        }) as Bottleneck,
    );
    for (let i = 0; i < limiters.length - 1; i++) {
      limiters[i]!.chain(limiters[i + 1]!);
    }
    const error = await limiters[0]!
      .schedule(() => {
        throw new Error("boom-deep-chain");
      })
      .catch((e: unknown) => e);
    const sections = ((error as Error).stack ?? "").split(
      "From previous Bottleneck.schedule location",
    );
    expect(sections.length - 1).toBe(3);
  });

  test("captureScheduleStack:false skips the schedule location", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ captureScheduleStack: false });
    const error = await limiter
      .schedule(() => {
        throw new Error("boom-no-stack");
      })
      .catch((e: unknown) => e);
    expect((error as Error).stack ?? "").not.toContain(
      "From previous Bottleneck.schedule location",
    );
  });

  test("an instance-patched schedule wrapper is cut like a subclass override", async ({
    makeLimiter,
  }) => {
    const limiter = makeLimiter({ id: "chain-wrapper-stack" });
    const originalSchedule = limiter.schedule.bind(limiter);
    (limiter as { schedule: unknown }).schedule = function (...args: unknown[]) {
      return (originalSchedule as (...a: never[]) => unknown)(...(args as never[]));
    };

    function callerScheduleTask(): Promise<unknown> {
      const schedule = limiter.schedule as (...a: any[]) => Promise<unknown>;
      return schedule(() => {
        throw new Error("boom-wrapper");
      });
    }

    const error = await callerScheduleTask().catch((e: unknown) => e);
    const stack = (error as Error).stack ?? "";
    expect(stack).toContain("callerScheduleTask");
    const section = stack.split("From previous Bottleneck.schedule location")[1] ?? "";
    expect(section).not.toContain("at Bottleneck.schedule");
  });

  test("a schedule nested in a task body appends both locations", async ({ makeLimiter }) => {
    const inner = makeLimiter({ id: "task-body-inner", datastore: "local" });
    const outer = makeLimiter({ id: "task-body-outer", datastore: "local" });

    function scheduleFromTaskBody(): Promise<never> {
      return outer.schedule(() =>
        inner.schedule(() => {
          throw new Error("boom-task-body");
        }),
      ) as Promise<never>;
    }

    const error = await scheduleFromTaskBody().catch((e: unknown) => e);
    const stack = (error as Error).stack ?? "";
    expect(stack).toContain("boom-task-body");
    // Redis projects prefix limiter ids per test; match the stable suffix.
    expect(stack).toContain("task-body-inner):");
    expect(stack).toContain("task-body-outer):");
    expect(stack).toContain("scheduleFromTaskBody");
  });

  test("a cutoff that is not on the stack falls back to a Job-cut section", async ({
    makeLimiter,
  }) => {
    const limiter = makeLimiter({ id: "fallback-stack" });
    const originalSchedule = limiter.schedule.bind(limiter);
    (limiter as { schedule: unknown }).schedule = function (...args: unknown[]) {
      return (originalSchedule as (...a: never[]) => unknown)(...(args as never[]));
    };

    function bypassedScheduleCall(): Promise<unknown> {
      // Bypass the instance patch: the cutoff (the patch) is not on this
      // stack, so the primary capture comes back empty and the fallback
      // still produces a section.
      const schedule = Bottleneck.prototype.schedule as (...a: any[]) => Promise<unknown>;
      return schedule.call(limiter, () => {
        throw new Error("boom-fallback");
      });
    }

    const error = await bypassedScheduleCall().catch((e: unknown) => e);
    const stack = (error as Error).stack ?? "";
    expect(stack).toContain("boom-fallback");
    expect(stack).toContain("From previous Bottleneck.schedule location");
    expect(stack).toContain("bypassedScheduleCall");
  });

  test("group child markers use the group id, never the key", async ({ makeGroup }) => {
    const group = makeGroup({ id: "tm-create-update" });
    const key = "acme-email-jane@acme.com";
    const limiter = group.key(key);

    const error = await limiter
      .schedule(() => {
        throw new Error("boom-group-key");
      })
      .catch((e: unknown) => e);
    const stack = (error as Error).stack ?? "";
    expect(stack).toContain("boom-group-key");
    // Redis projects prefix the group id per test; match the stable suffix.
    expect(stack).toContain("tm-create-update):");
    expect(stack).not.toContain("jane@acme.com");
    expect(stack).not.toContain(key);
  });
});
