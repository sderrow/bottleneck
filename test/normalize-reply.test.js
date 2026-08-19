import { describe, expect, test } from "vitest";
const { normalizeReply } = require("../src/cluster/normalizeReply");

describe("normalizeReply", () => {
  test("hgetall: RESP2 flat array becomes an object", () => {
    expect(normalizeReply(["hgetall", "k"], ["a", "1", "b", "2"])).toStrictEqual({
      a: "1",
      b: "2",
    });
  });

  test("hgetall: RESP3 map object passes through", () => {
    expect(normalizeReply(["hgetall", "k"], { a: "1" })).toStrictEqual({ a: "1" });
  });

  test("withscores: RESP2 flat array passes through", () => {
    const reply = ["member1", "5", "member2", "10"];
    expect(normalizeReply(["zrange", "k", "0", "-1", "withscores"], reply)).toStrictEqual(reply);
  });

  test("withscores: node-redis RESP3 pairs flatten to RESP2 form", () => {
    expect(
      normalizeReply(
        ["zrange", "k", "0", "-1", "withscores"],
        [
          ["member1", 5],
          ["member2", 10],
        ],
      ),
    ).toStrictEqual(["member1", "5", "member2", "10"]);
  });

  test("withscores: ioredis RESP3 map (replyMapping: resp3) flattens to RESP2 form", () => {
    expect(normalizeReply(["zrange", "k", "0", "-1", "withscores"], { member1: 5 })).toStrictEqual([
      "member1",
      "5",
    ]);
  });

  test("withscores matching is case-insensitive", () => {
    expect(normalizeReply(["ZRANGE", "k", "0", "-1", "WITHSCORES"], { m: 1.5 })).toStrictEqual([
      "m",
      "1.5",
    ]);
  });

  test("plain replies are untouched", () => {
    expect(normalizeReply(["del", "k"], 3)).toStrictEqual(3);
    expect(normalizeReply(["scan", 0], ["0", ["b_a_settings"]])).toStrictEqual([
      "0",
      ["b_a_settings"],
    ]);
    expect(normalizeReply(["zrange", "k", "0", "-1"], ["a", "b"])).toStrictEqual(["a", "b"]);
  });
});
