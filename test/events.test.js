import { describe, test, expect, vi } from "vitest";
const Events = require("../src/Events.js");

const noop = () => {};

describe("Events", () => {
  test("Should refuse an object that already has an emitter", () => {
    for (const prop of ["on", "once", "removeAllListeners"]) {
      expect(() => new Events({ [prop]: () => {} })).toThrow(
        "An Emitter already exists for this object",
      );
    }
  });

  test("removeAllListeners(name) removes one event, removeAllListeners() removes all", () => {
    const target = {};
    const events = new Events(target);
    const cb = noop;
    target.on("a", cb);
    target.on("b", cb);

    target.removeAllListeners("a");
    expect(events.listenerCount("a")).toBe(0);
    expect(events.listenerCount("b")).toBe(1);

    target.removeAllListeners();
    expect(events.listenerCount("b")).toBe(0);
  });

  test("A throwing listener triggers the error event and does not break others", async () => {
    const target = {};
    const events = new Events(target);
    const onError = vi.fn();
    const good = vi.fn(() => "ok");
    target.on("error", onError);
    target.on("boom", () => {
      throw new Error("listener exploded");
    });
    target.on("boom", good);

    expect(await events.trigger("boom", 1, 2)).toBe("ok");
    expect(good).toHaveBeenCalledWith(1, 2);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0][0]).toBeInstanceOf(Error);
  });

  test("A throwing error-event listener does not recurse", async () => {
    const target = {};
    const events = new Events(target);
    target.on("error", () => {
      throw new Error("error handler broke");
    });
    target.on("boom", () => {
      throw new Error("listener exploded");
    });

    expect(await events.trigger("boom")).toBeUndefined();
  });
});
