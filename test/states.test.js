import { describe, it, expect } from "vitest";
const States = require("../src/States");
const BottleneckError = require("../src/BottleneckError");

describe("States", function () {
  it("Should be created and be empty", function () {
    const states = new States(["A", "B", "C"]);
    expect(states.statusCounts()).toStrictEqual({ A: 0, B: 0, C: 0 });
  });

  it("Should start new series", function () {
    const states = new States(["A", "B", "C"]);

    states.start("x");
    states.start("y");

    expect(states.statusCounts()).toStrictEqual({ A: 2, B: 0, C: 0 });
  });

  it("Should increment", function () {
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

  it("Should remove", function () {
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

  it("Should return current status", function () {
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

  it("Should return job ids for a status", function () {
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
