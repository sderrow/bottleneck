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

const MARKER = "From previous Bottleneck.schedule location";

/** Schedule-location sections appended to an error's stack, marker included. */
function scheduleSections(error: unknown): string[] {
  return ((error as Error).stack ?? "")
    .split(MARKER)
    .slice(1)
    .map((section) => MARKER + section);
}

/** Frames the stack cutoff must remove: schedule(), wrap(), chain forwarding. */
function expectNoLibraryFrames(section: string | undefined): void {
  expect(section).toBeDefined();
  expect(section).not.toMatch(/src[/\\](Bottleneck|Job)\.ts/);
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
    const [section] = scheduleSections(error);
    expect(section).toContain("outerScheduleTask");
    expectNoLibraryFrames(section);
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
    expect(scheduleSections(failure)).toHaveLength(1);
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

  test("a chain renders as one section listing every chained limiter", async ({ makeLimiter }) => {
    // The head limiter uses the project's datastore; the chained ones are local.
    const limiters = Array.from(
      { length: 5 },
      (_, i) =>
        makeLimiter(
          i === 0 ? { id: "chain-0" } : { id: `chain-${i}`, datastore: "local" },
        ) as Bottleneck,
    );
    for (let i = 0; i < limiters.length - 1; i++) {
      limiters[i]!.chain(limiters[i + 1]!);
    }

    function scheduleOnChain(): Promise<unknown> {
      return limiters[0]!.schedule(() => {
        throw new Error("boom-chain");
      });
    }

    const error = await scheduleOnChain().catch((e: unknown) => e);
    expect((error as Error).stack).toContain("boom-chain");
    const sections = scheduleSections(error);
    expect(sections).toHaveLength(1);
    // Redis projects prefix limiter ids per test; match the stable suffixes.
    expect(sections[0]).toMatch(/chain-0, via \S*chain-1, \S*chain-2, \S*chain-3, \S*chain-4\):/);
    expect(sections[0]).toContain("scheduleOnChain");
    expectNoLibraryFrames(sections[0]);
  });

  test("nested schedule locations keep the innermost two and the outermost", async ({
    makeLimiter,
  }) => {
    const limiters = Array.from(
      { length: 5 },
      (_, i) =>
        makeLimiter({
          id: `nest-cap-${i}`,
          datastore: "local",
        }) as Bottleneck,
    );

    function scheduleAt(i: number): Promise<unknown> {
      return limiters[i]!.schedule(() => {
        if (i === limiters.length - 1) {
          throw new Error("boom-deep-nest");
        }
        return scheduleAt(i + 1);
      });
    }
    function startNestedSchedules(): Promise<unknown> {
      return scheduleAt(0);
    }

    const error = await startNestedSchedules().catch((e: unknown) => e);
    const stack = (error as Error).stack ?? "";
    expect(stack).toContain("boom-deep-nest");
    const sections = scheduleSections(error);
    // Redis projects prefix limiter ids per test; match the stable suffixes.
    expect(sections.map((section) => /nest-cap-\d/.exec(section)?.[0])).toEqual([
      "nest-cap-4",
      "nest-cap-3",
      "nest-cap-0",
    ]);
    expect(stack).toContain("... 2 nested schedule locations omitted\nFrom previous");
    // Only the outermost section reaches the app's original call site.
    expect(sections[2]).toContain("startNestedSchedules");
    expect(sections[0]).not.toContain("startNestedSchedules");
  });

  test("wrap() and withOptions() capture the wrapped function's caller", async ({
    makeLimiter,
  }) => {
    const limiter = makeLimiter({ id: "wrap-stack" });
    const wrapped = limiter.wrap((): never => {
      throw new Error("boom-wrap");
    });

    function callWrapped(): Promise<unknown> {
      return wrapped();
    }
    function callWithOptions(): Promise<unknown> {
      return wrapped.withOptions({});
    }

    for (const [caller, call] of [
      ["callWrapped", callWrapped],
      ["callWithOptions", callWithOptions],
    ] as const) {
      const sections = scheduleSections(await call().catch((e: unknown) => e));
      expect(sections).toHaveLength(1);
      expect(sections[0]).toContain(caller);
      expectNoLibraryFrames(sections[0]);
    }
  });

  test("captureScheduleStack:false skips the schedule location", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ captureScheduleStack: false });
    const error = await limiter
      .schedule(() => {
        throw new Error("boom-no-stack");
      })
      .catch((e: unknown) => e);
    expect((error as Error).stack).toContain("boom-no-stack");
    expect(scheduleSections(error)).toHaveLength(0);
  });

  test("schedule overrides cut their own wrapper frames", async ({ track }) => {
    class SubclassLimiter extends Bottleneck {
      override schedule(...args: unknown[]): Promise<unknown> {
        return this.scheduleWithOpts(args);
      }
      scheduleWithOpts(args: unknown[]): Promise<unknown> {
        return (super.schedule as (...a: unknown[]) => Promise<unknown>)(...args);
      }
    }
    const subclassed = track(new SubclassLimiter({ id: "override-subclass", datastore: "local" }));

    const arrowPatched = track(new Bottleneck({ id: "override-arrow", datastore: "local" }));
    const original = Bottleneck.prototype.schedule as (...a: unknown[]) => Promise<unknown>;
    function scheduleWithOpts(args: unknown[]): Promise<unknown> {
      return original.apply(arrowPatched, args);
    }
    (arrowPatched as { schedule: unknown }).schedule = (...args: unknown[]) =>
      scheduleWithOpts(args);

    for (const limiter of [subclassed, arrowPatched]) {
      function callOverride(): Promise<unknown> {
        return (limiter.schedule as (...a: unknown[]) => Promise<unknown>)(() => {
          throw new Error("boom-override");
        });
      }
      const [section] = scheduleSections(await callOverride().catch((e: unknown) => e));
      expect(section).toContain("callOverride");
      expect(section).not.toContain("scheduleWithOpts");
      expectNoLibraryFrames(section);
    }
  });

  test("bound overrides and bypassed overrides still capture the caller", async ({ track }) => {
    // V8 ignores bound functions as cutoffs, so their wrapper frames remain;
    // calling the base schedule() past an override uses the fallback capture.
    const bound = track(new Bottleneck({ id: "override-bound", datastore: "local" }));
    const original = Bottleneck.prototype.schedule as (...a: unknown[]) => Promise<unknown>;
    (bound as { schedule: unknown }).schedule = function scheduleWithOpts(
      this: Bottleneck,
      ...args: unknown[]
    ) {
      return original.apply(this, args);
    }.bind(bound);

    function callBound(): Promise<unknown> {
      return (bound.schedule as (...a: unknown[]) => Promise<unknown>)(() => {
        throw new Error("boom-bound");
      });
    }
    function callBypass(): Promise<unknown> {
      return original.call(bound, () => {
        throw new Error("boom-bypass");
      });
    }

    for (const [caller, call] of [
      ["callBound", callBound],
      ["callBypass", callBypass],
    ] as const) {
      const [section] = scheduleSections(await call().catch((e: unknown) => e));
      expect(section).toContain(caller);
      expectNoLibraryFrames(section);
    }
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

  test("an explicit scheduleStackLabel is used for the marker", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ id: "pii-check-123", scheduleStackLabel: "check" });
    const error = await limiter
      .schedule(() => {
        throw new Error("boom-label");
      })
      .catch((e: unknown) => e);
    const stack = (error as Error).stack ?? "";
    expect(stack).toContain("From previous Bottleneck.schedule location (check):");
    expect(stack).not.toContain("pii-check-123");
  });

  test("a group-level scheduleStackLabel labels child markers", async ({ makeGroup }) => {
    const group = makeGroup({ id: "tm-create-update", scheduleStackLabel: "payroll" });
    const limiter = group.key("acme-email-jane@acme.com");

    const error = await limiter
      .schedule(() => {
        throw new Error("boom-group-label");
      })
      .catch((e: unknown) => e);
    const stack = (error as Error).stack ?? "";
    expect(stack).toContain("From previous Bottleneck.schedule location (payroll):");
    expect(stack).not.toContain("jane@acme.com");
  });
});
