import { describe, expect } from "vitest";
import Bottleneck from "../src/Bottleneck";
import { useFakeClock } from "./helpers/clock";
import { defined } from "./helpers/defined";
import { test } from "./helpers/test-api";

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

/** Frames that ran the task a job was scheduled from, cut at its marker. */
function expectNoTaskRunnerFrames(section: string | undefined): void {
  expect(section).toBeDefined();
  expect(section).not.toMatch(/__bottleneckRunTask|runInAsyncScope|bound \[as task\]/);
}

describe("Schedule stacks", () => {
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
    const head = makeLimiter({ id: "chain-0" }) as Bottleneck;
    const tail = Array.from(
      { length: 4 },
      (_, i) => makeLimiter({ id: `chain-${i + 1}`, datastore: "local" }) as Bottleneck,
    );
    tail.reduce((limiter, next) => {
      limiter.chain(next);
      return next;
    }, head);

    function scheduleOnChain(): Promise<unknown> {
      return head.schedule(() => {
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
      return defined(limiters[i]).schedule(() => {
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
    expectNoTaskRunnerFrames(sections[0]);
    expectNoTaskRunnerFrames(sections[1]);
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
    // Async overrides run synchronously until their first await, so the
    // override is still on the stack when the base schedule() captures.
    class AsyncSubclassLimiter extends Bottleneck {
      override async schedule(...args: unknown[]): Promise<unknown> {
        return this.scheduleWithOpts(args);
      }
      scheduleWithOpts(args: unknown[]): Promise<unknown> {
        return (super.schedule as (...a: unknown[]) => Promise<unknown>)(...args);
      }
    }
    const arrowPatched = track(new Bottleneck({ id: "override-arrow" }));
    const original = Bottleneck.prototype.schedule as (...a: unknown[]) => Promise<unknown>;
    function scheduleWithOpts(args: unknown[]): Promise<unknown> {
      return original.apply(arrowPatched, args);
    }
    (arrowPatched as { schedule: unknown }).schedule = (...args: unknown[]) =>
      scheduleWithOpts(args);

    const limiters = [
      track(new SubclassLimiter({ id: "override-subclass" })),
      track(new AsyncSubclassLimiter({ id: "override-async" })),
      arrowPatched,
    ];
    for (const limiter of limiters) {
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

    // Bypassing the override: its cutoff isn't on the stack, so the first
    // capture is empty and the base-cut fallback supplies the section.
    function callBypass(): Promise<unknown> {
      return original.call(arrowPatched, () => {
        throw new Error("boom-bypass");
      });
    }
    const [section] = scheduleSections(await callBypass().catch((e: unknown) => e));
    expect(section).toContain("callBypass");
    expectNoLibraryFrames(section);
  });

  test("a bound schedule override keeps its wrapper frames", async ({ track }) => {
    // V8 can't cut at a bound function, so the base cutoff is used instead.
    const bound = track(new Bottleneck({ id: "override-bound" }));
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

    const [section] = scheduleSections(await callBound().catch((e: unknown) => e));
    expect(section).toContain("callBound");
    expect(section).toContain("scheduleWithOpts");
    expectNoLibraryFrames(section);
  });

  test("a schedule nested in a task body appends both locations", async ({ makeLimiter }) => {
    const inner = makeLimiter({ id: "task-body-inner", datastore: "local" });
    const outer = makeLimiter({ id: "task-body-outer", datastore: "local" });

    const outerTaskBody = () =>
      inner.schedule(() => {
        throw new Error("boom-task-body");
      });
    function scheduleFromTaskBody(): Promise<never> {
      return outer.schedule(outerTaskBody) as Promise<never>;
    }

    const error = await scheduleFromTaskBody().catch((e: unknown) => e);
    const stack = (error as Error).stack ?? "";
    expect(stack).toContain("boom-task-body");
    // Redis projects prefix limiter ids per test; match the stable suffix.
    expect(stack).toContain("task-body-inner):");
    expect(stack).toContain("task-body-outer):");
    expect(stack).toContain("scheduleFromTaskBody");
    const [innerSection] = scheduleSections(error);
    expect(innerSection).toContain("outerTaskBody");
    expectNoTaskRunnerFrames(innerSection);
  });

  test("a schedule after an await in a task body keeps the task frame", async ({ makeLimiter }) => {
    const inner = makeLimiter({ id: "task-await-inner", datastore: "local" });
    const outer = makeLimiter({ id: "task-await-outer", datastore: "local" });

    const outerTaskAwait = async () => {
      await Promise.resolve();
      return inner.schedule(() => {
        throw new Error("boom-task-await");
      });
    };
    const error = await outer.schedule(outerTaskAwait).catch((e: unknown) => e);
    const [innerSection] = scheduleSections(error);
    expect(innerSection).toContain("outerTaskAwait");
    // The marker isn't on the stack after the await; V8 leaves just one
    // `at async Job.doExecute` frame below the task.
    expectNoTaskRunnerFrames(innerSection);
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
