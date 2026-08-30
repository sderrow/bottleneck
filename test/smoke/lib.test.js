// Smoke test for `dist/index.js` (the full bundle).
//
// The point of this file is the `inline-lua` plugin in `tsdown.config.mts`.
// When the package is published, `dist/index.js` is the only file consumers
// load, and `src/cluster/lua/*.lua` is NOT shipped — so any code path that
// reaches `Scripts.js` must find its lua text already inlined into the
// bundle, not via a runtime `fs.readdirSync(__dirname)`. Local-only tests
// against `src/index.js` do not exercise this; the cluster suite runs from
// source too. So this is the only smoke that catches a regression where
// lua inlining drops a header, the script-id shape changes, or the bundle
// keeps a stray fs.read at runtime — all of which fail the moment a
// Redis-backed limiter actually evaluates a script.
//
// Loaded via `test/bottleneck.mjs` with `BOTTLENECK_ENTRY=lib` and
// `DATASTORE=redis` so the wrapper injects the test heartbeat override,
// the per-fork id prefix, and node-redis client options pointing at the
// shared testcontainer Redis (started by the root globalSetup). The
// `lib-smoke` project's globalSetup builds the lib bundle before any
// test file is collected.

import { describe, it, expect } from "vitest";
import Bottleneck from "../bottleneck.mjs";

describe("dist/index full smoke", () => {
  it("loads", () => {
    expect(Bottleneck).toBeDefined();
    expect(typeof Bottleneck).toBe("function");
    expect(typeof Bottleneck.Group).toBe("function");
  });

  it("schedules locally", async () => {
    const limiter = new Bottleneck({ maxConcurrent: 1 });
    const result = await limiter.schedule(() => 42);
    expect(result).toBe(42);
    await limiter.disconnect(false);
  });

  it("evaluates inlined lua against redis (init + submit + done)", async () => {
    // ready() runs init.lua, which is built by Scripts.js by concatenating
    // the `process_tick`, `refresh_expiration`, `validate_keys`,
    // `validate_client`, `refs`, `get_time`, and `conditions_check` headers.
    // schedule() further triggers register/submit/free/done. If any of those
    // lua files were missing from the inlined bundle, ioredis/node-redis
    // would surface a "ERR Error compiling script" or the body would silently
    // contain `undefined` and Redis would reply with a runtime lua error.
    const limiter = new Bottleneck({
      id: "lib-smoke",
      datastore: "redis",
      maxConcurrent: 1,
      clearDatastore: true,
    });
    try {
      const clients = await limiter.ready();
      expect(Object.keys(clients)).toEqual(["client", "subscriber"]);

      const result = await limiter.schedule(() => "ok");
      expect(result).toBe("ok");
    } finally {
      await limiter.disconnect(false);
    }
  });

  it("Bottleneck.Group spins up child limiters against redis", async () => {
    // Group.key() instantiates child limiters lazily; each child runs its
    // own init.lua against the shared connection. This exercises a different
    // wiring path (Scripts loaded through the Group's RedisDatastore) and
    // catches regressions where the inlined lua map is reachable from one
    // entry point but not another.
    const group = new Bottleneck.Group({
      id: "lib-smoke-group",
      datastore: "redis",
      maxConcurrent: 1,
      clearDatastore: true,
    });
    try {
      const a = group.key("A");
      const b = group.key("B");
      const [ra, rb] = await Promise.all([a.schedule(() => "a"), b.schedule(() => "b")]);
      expect(ra).toBe("a");
      expect(rb).toBe("b");
    } finally {
      await group.disconnect(false);
    }
  });
});
