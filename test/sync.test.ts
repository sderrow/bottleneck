import { describe, test, expect } from "vitest";
import Sync from "../src/Sync";

describe("Sync", () => {
  test("A rejected task rejects its caller and does not block the queue", async () => {
    const sync = new Sync("test");
    const ran = [];

    const failing = sync.schedule(() => Promise.reject(new Error("task failed")));
    const following = sync.schedule(async () => {
      ran.push("following");
      return "done";
    });

    await expect(failing).rejects.toThrow("task failed");
    await expect(following).resolves.toBe("done");
    expect(ran).toEqual(["following"]);
    expect(sync.isEmpty()).toBe(true);
  });

  test("Forwards arguments to the task", async () => {
    const sync = new Sync("test");
    await expect(sync.schedule((a, b) => a + b, 1, 2)).resolves.toBe(3);
  });
});
