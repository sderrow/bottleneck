import { describe, it, expect } from "vitest";
import BottleneckError from "../src/BottleneckError";
import States from "../src/States";

describe("States", () => {
  it("Should be created and be empty", () => {
    const states = new States(["A", "B", "C"]);
    expect(states.statusCounts()).toStrictEqual({ A: 0, B: 0, C: 0 });
  });

  it("Should start new series", () => {
    const states = new States(["A", "B", "C"]);

    states.start("x");
    states.start("y");

    expect(states.statusCounts()).toStrictEqual({ A: 2, B: 0, C: 0 });
  });

  it("Should increment", () => {
    const states = new States(["A", "B", "C"]);

    states.start("x");
    states.start("y");
    states.next("x");
    states.next("y");
    states.next("x");
    expect(states.statusCounts()).toStrictEqual({ A: 0, B: 1, C: 1 });

    states.next("z");
    expect(states.statusCounts()).toStrictEqual({ A: 0, B: 1, C: 1 });

    states.next("x");
    expect(states.statusCounts()).toStrictEqual({ A: 0, B: 1, C: 0 });

    states.next("x");
    expect(states.statusCounts()).toStrictEqual({ A: 0, B: 1, C: 0 });

    states.next("y");
    states.next("y");
    expect(states.statusCounts()).toStrictEqual({ A: 0, B: 0, C: 0 });
  });

  it("Should remove", () => {
    const states = new States(["A", "B", "C"]);

    states.start("x");
    states.start("y");
    states.next("x");
    states.next("y");
    states.next("x");
    expect(states.statusCounts()).toStrictEqual({ A: 0, B: 1, C: 1 });

    states.remove("x");
    expect(states.statusCounts()).toStrictEqual({ A: 0, B: 1, C: 0 });

    states.remove("y");
    expect(states.statusCounts()).toStrictEqual({ A: 0, B: 0, C: 0 });
  });

  it("Should return current status", () => {
    const states = new States(["A", "B", "C"]);

    states.start("x");
    states.start("y");
    states.next("x");
    states.next("y");
    states.next("x");
    expect(states.statusCounts()).toStrictEqual({ A: 0, B: 1, C: 1 });

    expect(states.jobStatus("x")).toStrictEqual("C");
    expect(states.jobStatus("y")).toStrictEqual("B");
    expect(states.jobStatus("z")).toStrictEqual(null);
  });

  it("Should return job ids for a status", () => {
    const states = new States(["A", "B", "C"]);

    states.start("x");
    states.start("y");
    states.start("z");
    states.next("x");
    states.next("y");
    states.next("x");
    states.next("z");
    expect(states.statusCounts()).toStrictEqual({ A: 0, B: 2, C: 1 });

    expect(states.statusJobs().sort()).toStrictEqual(["x", "y", "z"]);
    expect(states.statusJobs("A")).toStrictEqual([]);
    expect(states.statusJobs("B").sort()).toStrictEqual(["y", "z"]);
    expect(states.statusJobs("C")).toStrictEqual(["x"]);
    expect(() => states.statusJobs("Z")).toThrow(BottleneckError);
  });
});
