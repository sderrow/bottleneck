import { describe, expect, vi } from "vitest";
import { deferred, enqueued, test } from "./helpers/test-api";

/*
 * `signal` cancels jobs that haven't started. Every case holds state with
 * deferred() and barriers with enqueued() or a transition event, so it is
 * deterministic under the local and redis projects alike. No fake clock:
 * AbortSignal.timeout runs on Node's internal timers either way.
 */

const zeroCounts = { RECEIVED: 0, QUEUED: 0, RUNNING: 0, EXECUTING: 0 };

describe("Abort signal", () => {
  test("an already-aborted signal rejects without running the task", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1, rejectOnDrop: false });
    const controller = new AbortController();
    const reason = new Error("cancelled");
    controller.abort(reason);
    const task = vi.fn<() => string>(() => "ran");

    await expect(limiter.schedule({ signal: controller.signal }, task)).rejects.toBe(reason);
    expect(task).not.toHaveBeenCalled();
    expect(limiter.counts()).toEqual(zeroCounts);
  });

  test("aborting mid-submit rejects and leaves the limiter usable", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    const controller = new AbortController();
    const task = vi.fn<() => string>(() => "ran");

    // schedule() runs synchronously into `await __submit__`: still RECEIVED.
    const victim = limiter.schedule({ signal: controller.signal }, task);
    expect(limiter.counts().RECEIVED).toEqual(1);
    controller.abort();

    await expect(victim).rejects.toBe(controller.signal.reason);
    await enqueued(limiter);
    expect(task).not.toHaveBeenCalled();
    expect(limiter.counts()).toEqual(zeroCounts);
    await expect(limiter.schedule(() => "next")).resolves.toEqual("next");
  });

  test("aborting while waiting on the submit lock rejects", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    const controller = new AbortController();
    const task = vi.fn<() => string>(() => "ran");

    const first = limiter.schedule(() => "first");
    const victim = limiter.schedule({ signal: controller.signal }, task);
    controller.abort();

    await expect(victim).rejects.toBe(controller.signal.reason);
    await expect(first).resolves.toEqual("first");
    await enqueued(limiter);
    expect(task).not.toHaveBeenCalled();
    expect(limiter.counts()).toEqual(zeroCounts);
  });

  test("aborting a queued job removes it and the next job still runs", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    const hold = deferred();
    const controller = new AbortController();
    const task = vi.fn<() => string>(() => "ran");

    const blocker = limiter.schedule(() => hold.signal);
    const victim = limiter.schedule({ id: "victim", signal: controller.signal }, task);
    const next = limiter.schedule(() => "next");
    await enqueued(limiter);
    expect(limiter.queued()).toEqual(2);
    expect(limiter.jobStatus("victim")).toEqual("QUEUED");

    let empties = 0;
    limiter.on("empty", () => empties++);
    controller.abort();

    await expect(victim).rejects.toBe(controller.signal.reason);
    expect(limiter.queued()).toEqual(1);
    expect(limiter.jobStatus("victim")).toBeNull();
    expect(empties).toEqual(0);

    hold.release();
    await expect(blocker).resolves.toBeUndefined();
    await expect(next).resolves.toEqual("next");
    expect(task).not.toHaveBeenCalled();
    expect(empties).toEqual(1);
  });

  test("aborting the last queued job fires empty", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    const hold = deferred();
    const controller = new AbortController();

    const blocker = limiter.schedule(() => hold.signal);
    const victim = limiter.schedule({ signal: controller.signal }, () => "ran");
    await enqueued(limiter);

    let empties = 0;
    limiter.on("empty", () => empties++);
    controller.abort();
    expect(empties).toEqual(1);
    expect(limiter.empty()).toBe(true);

    await expect(victim).rejects.toBe(controller.signal.reason);
    hold.release();
    await expect(blocker).resolves.toBeUndefined();
  });

  test("AbortSignal.timeout rejects with a TimeoutError", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    const hold = deferred();
    const task = vi.fn<() => string>(() => "ran");

    const blocker = limiter.schedule(() => hold.signal);
    const victim = limiter.schedule({ signal: AbortSignal.timeout(1) }, task);

    await expect(victim).rejects.toMatchObject({ name: "TimeoutError" });
    hold.release();
    await expect(blocker).resolves.toBeUndefined();
    expect(task).not.toHaveBeenCalled();
  });

  test("aborting once the job has started does nothing", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    const running = new AbortController();
    const executing = new AbortController();

    // RUNNING: abort inside the transition event, before the task's timer.
    limiter.on("scheduled", (info) => {
      if (info.options.id === "running") running.abort();
    });
    const p1 = limiter.schedule({ id: "running", signal: running.signal }, () => "ran 1");
    const p2 = limiter.schedule({ id: "executing", signal: executing.signal }, () => {
      executing.abort();
      return "ran 2";
    });

    await expect(p1).resolves.toEqual("ran 1");
    await expect(p2).resolves.toEqual("ran 2");
    expect(running.signal.aborted).toBe(true);
  });

  test("a shared signal holds one listener, removed once its jobs leave the queue", async ({
    makeLimiter,
  }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    const controller = new AbortController();
    const { signal } = controller;
    const add = vi.spyOn(signal, "addEventListener");
    const remove = vi.spyOn(signal, "removeEventListener");
    const warnings: Error[] = [];
    const onWarning = (warning: Error) => warnings.push(warning);
    process.on("warning", onWarning);

    try {
      const hold = deferred();
      const jobs = [
        limiter.schedule({ signal }, () => hold.signal),
        ...Array.from({ length: 99 }, (_, i) => limiter.schedule({ signal }, () => i)),
      ];
      await enqueued(limiter);
      expect(limiter.queued()).toEqual(99);
      expect(add).toHaveBeenCalledTimes(1);
      expect(remove).not.toHaveBeenCalled();

      hold.release();
      await Promise.all(jobs);
      expect(add).toHaveBeenCalledTimes(1);
      expect(remove).toHaveBeenCalledTimes(1);
      expect(remove.mock.calls[0]?.[1]).toBe(add.mock.calls[0]?.[1]);
    } finally {
      process.off("warning", onWarning);
    }
    expect(warnings.filter((w) => w.name === "MaxListenersExceededWarning")).toEqual([]);
  });

  test("aborting during register frees the slot", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    const controller = new AbortController();
    const reason = new Error("cancelled");
    const task = vi.fn<() => string>(() => "ran");

    // Debug listeners run synchronously, just before `await __register__`.
    limiter.on("debug", (message) => {
      if (message === "Draining victim") controller.abort(reason);
    });
    const victim = limiter.schedule({ id: "victim", signal: controller.signal }, task);

    await expect(victim).rejects.toBe(reason);
    // The victim's submit holds the lock through its drain, including the free.
    await enqueued(limiter);
    expect(await limiter.running()).toEqual(0);
    expect(limiter.counts()).toEqual(zeroCounts);
    await expect(limiter.schedule(() => "next")).resolves.toEqual("next");
    expect(task).not.toHaveBeenCalled();
  });

  test("aborting in a chained limiter rejects the outer job as a cancellation", async ({
    makeLimiter,
  }) => {
    const outer = makeLimiter({ id: "abort-outer" });
    const inner = makeLimiter({ id: "abort-inner", maxConcurrent: 1 });
    outer.chain(inner);
    const hold = deferred();
    const controller = new AbortController();
    const reason = new Error("cancelled");
    const stack = reason.stack;
    const task = vi.fn<() => string>(() => "ran");
    const failed = vi.fn<() => number>(() => 0);
    outer.on("failed", failed);

    const blocker = inner.schedule(() => hold.signal);
    await enqueued(inner);
    const queuedInInner = new Promise<void>((resolve) => {
      inner.on("queued", () => resolve());
    });
    const victim = outer.schedule({ signal: controller.signal }, task);
    await queuedInInner;
    controller.abort(reason);

    await expect(victim).rejects.toBe(reason);
    expect(failed).not.toHaveBeenCalled();
    expect(reason.stack).toBe(stack);
    expect(inner.queued()).toEqual(0);

    hold.release();
    await expect(blocker).resolves.toBeUndefined();
    expect(task).not.toHaveBeenCalled();
  });

  test("a task that throws its own abort reason is not retried", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    const controller = new AbortController();
    const reason = new Error("cancelled");
    const stack = reason.stack;
    const failed = vi.fn<() => number>(() => 0);
    limiter.on("failed", failed);

    const task = vi.fn<() => never>(() => {
      controller.abort(reason);
      throw controller.signal.reason;
    });

    await expect(limiter.schedule({ signal: controller.signal }, task)).rejects.toBe(reason);
    expect(task).toHaveBeenCalledTimes(1);
    expect(failed).not.toHaveBeenCalled();
    expect(reason.stack).toBe(stack);
  });
});
