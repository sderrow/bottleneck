import { describe, it, expect } from "vitest";
import type { Status } from "../src/types";
import BottleneckError from "../src/BottleneckError";
import States from "../src/States";

const counts = (RECEIVED: number, QUEUED: number, RUNNING: number, EXECUTING: number) => ({
  RECEIVED,
  QUEUED,
  RUNNING,
  EXECUTING,
});

describe("States", () => {
  it("Should be created and be empty", () => {
    expect(new States(false).statusCounts()).toStrictEqual(counts(0, 0, 0, 0));
    expect(new States(true).statusCounts()).toStrictEqual({ ...counts(0, 0, 0, 0), DONE: 0 });
  });

  it("Should start new series", () => {
    const states = new States(false);

    expect(states.start("x")).toBe(0);
    expect(states.start("y")).toBe(1);

    expect(states.statusCounts()).toStrictEqual(counts(2, 0, 0, 0));
  });

  it("Should increment", () => {
    const states = new States(false);

    states.start("x");
    states.start("y");
    states.next("x");
    states.next("y");
    states.next("x");
    expect(states.statusCounts()).toStrictEqual(counts(0, 1, 1, 0));

    states.next("z");
    expect(states.statusCounts()).toStrictEqual(counts(0, 1, 1, 0));

    states.next("x");
    expect(states.statusCounts()).toStrictEqual(counts(0, 1, 0, 1));

    states.next("x");
    expect(states.statusCounts()).toStrictEqual(counts(0, 1, 0, 0));

    states.next("x");
    expect(states.statusCounts()).toStrictEqual(counts(0, 1, 0, 0));

    states.next("y");
    states.next("y");
    states.next("y");
    expect(states.statusCounts()).toStrictEqual(counts(0, 0, 0, 0));
  });

  it("Should keep jobs in DONE when tracking it", () => {
    const states = new States(true);

    states.start("x");
    states.next("x");
    states.next("x");
    states.next("x");
    states.next("x");
    expect(states.statusCounts()).toStrictEqual({ ...counts(0, 0, 0, 0), DONE: 1 });
    expect(states.jobStatus("x")).toBe("DONE");

    states.next("x");
    expect(states.statusCounts()).toStrictEqual({ ...counts(0, 0, 0, 0), DONE: 0 });
    expect(states.jobStatus("x")).toBe(null);
  });

  it("Should remove", () => {
    const states = new States(false);

    states.start("x");
    states.start("y");
    states.next("x");
    states.next("y");
    states.next("x");
    expect(states.statusCounts()).toStrictEqual(counts(0, 1, 1, 0));

    expect(states.remove("x")).toBe(true);
    expect(states.statusCounts()).toStrictEqual(counts(0, 1, 0, 0));

    expect(states.remove("y")).toBe(true);
    expect(states.remove("y")).toBe(false);
    expect(states.statusCounts()).toStrictEqual(counts(0, 0, 0, 0));
  });

  it("Should return current status", () => {
    const states = new States(false);

    states.start("x");
    states.start("y");
    states.next("x");
    states.next("y");
    states.next("x");
    expect(states.statusCounts()).toStrictEqual(counts(0, 1, 1, 0));

    expect(states.jobStatus("x")).toStrictEqual("RUNNING");
    expect(states.jobStatus("y")).toStrictEqual("QUEUED");
    expect(states.jobStatus("z")).toStrictEqual(null);
  });

  it("Should return job ids for a status", () => {
    const states = new States(false);

    states.start("x");
    states.start("y");
    states.start("z");
    states.next("x");
    states.next("y");
    states.next("x");
    states.next("z");
    expect(states.statusCounts()).toStrictEqual(counts(0, 2, 1, 0));

    expect(states.statusJobs().sort()).toStrictEqual(["x", "y", "z"]);
    expect(states.statusJobs("RECEIVED")).toStrictEqual([]);
    expect(states.statusJobs("QUEUED").sort()).toStrictEqual(["y", "z"]);
    expect(states.statusJobs("RUNNING")).toStrictEqual(["x"]);
    expect(() => states.statusJobs("Z" as Status)).toThrow(BottleneckError);
    expect(() => states.statusJobs("DONE")).toThrow(
      "status must be one of RECEIVED, QUEUED, RUNNING, EXECUTING",
    );
    expect(new States(true).statusJobs("DONE")).toStrictEqual([]);
  });
});
