import { describe, expect } from "vitest";
import { test } from "./helpers/test-api.js";
const Job = require("../src/Job.js");
const States = require("../src/States.js");
const Events = require("../src/Events.js");

const jobDefaults = { priority: 5, weight: 1, expiration: null, id: "<no-id>" };

const makeJob = (options) => {
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
