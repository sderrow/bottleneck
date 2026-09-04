import { describe, expect } from "vitest";
import Events from "../src/Events";
import Job from "../src/Job";
import States from "../src/States";
import { test } from "./helpers/test-api";

const jobDefaults = { priority: 5, weight: 1, expiration: null, id: "<no-id>" };

const makeJob = (options: object) => {
  const states = new States(["RECEIVED", "QUEUED", "RUNNING", "EXECUTING"]);
  const job = new Job(
    () => Promise.resolve(),
    [],
    options,
    jobDefaults,
    true,
    new Events({}),
    states,
  );
  return { job, states };
};

describe("Job", () => {
  test("Clamps priority to the valid range", () => {
    expect(makeJob({ priority: -1 }).job.options.priority).toBe(0);
    expect(makeJob({ priority: 42 }).job.options.priority).toBe(9);
  });

  test("Coerces a non-integer priority to the default", () => {
    expect(makeJob({ priority: 2.5 }).job.options.priority).toBe(5);
    expect(makeJob({ priority: "high" }).job.options.priority).toBe(5);
  });

  test("doDrop returns false for a job that was never received", () => {
    const { job } = makeJob({ id: "never-received" });
    expect(job.doDrop({ message: "Dropped!" })).toBe(false);
  });

  test("doQueue throws when the job is not RECEIVED", () => {
    const { job } = makeJob({ id: "wrong-status" });
    job.doReceive();
    expect(() => job.doRun()).toThrow(
      "Invalid job status RECEIVED, expected QUEUED. Please open an issue at https://github.com/SGrondin/bottleneck/issues",
    );
  });

  test("A failing task with a rejected clearGlobalState leaves the job unsettled", async () => {
    const { job, states } = makeJob({ id: "unsettled" });
    job.task = () => Promise.reject(new Error("task failed"));
    job.doReceive();
    job.doQueue(false, false);
    job.doRun();

    const execution = job.doExecute(
      null,
      () => false, // the store disconnected before the failure was processed
      () => {
        throw new Error("should not retry");
      },
      async () => {},
    );

    await expect(execution).resolves.toBeUndefined();
    expect(states.jobStatus("unsettled")).toBe("EXECUTING");

    const settled = await Promise.race([job.promise.then(() => true), Promise.resolve(false)]);
    expect(settled).toBe(false);
  });

  // Regression test for an upstream CoffeeScript precedence bug: doExpire's
  // guard compiled to `jobStatus(@options.id == "RUNNING")` (a boolean job
  // id), so a job whose expiration fired while still RUNNING threw
  // "Invalid job status RUNNING, expected EXECUTING" instead of expiring.
  // The window is unreachable through Bottleneck._run's timers (the execute
  // timer always fires before the expiration timer), so only a direct
  // doExpire call can pin the repaired behavior.
  test("doExpire expires a job that is still RUNNING", async () => {
    const { job, states } = makeJob({ id: "running-expiry", expiration: 50 });

    job.doReceive();
    job.doQueue(false, false);
    job.doRun();
    expect(states.jobStatus("running-expiry")).toBe("RUNNING");

    const expired = job.doExpire(
      () => true,
      () => {
        throw new Error("should not retry");
      },
      async () => {},
    );

    await expect(job.promise).rejects.toThrow("This job timed out after 50 ms.");
    await expired;
    expect(states.jobStatus("running-expiry")).toBe(null);
  });

  test("doExpire expires an EXECUTING job", async () => {
    const { job, states } = makeJob({ id: "executing-expiry", expiration: 75 });

    job.doReceive();
    job.doQueue(false, false);
    job.doRun();
    const executing = job.doExecute(
      null,
      () => false, // simulate the expiration timer having claimed the job
      () => {},
      async () => {},
    );
    expect(states.jobStatus("executing-expiry")).toBe("EXECUTING");

    const expired = job.doExpire(
      () => true,
      () => {
        throw new Error("should not retry");
      },
      async () => {},
    );

    await expect(job.promise).rejects.toThrow("This job timed out after 75 ms.");
    await Promise.all([expired, executing]);
    expect(states.jobStatus("executing-expiry")).toBe(null);
  });
});
