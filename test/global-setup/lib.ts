// Project-level Vitest globalSetup for the `lib-smoke` project.
//
// The lib smoke test loads `dist/index.js` via `test/bottleneck.mjs`
// (BOTTLENECK_ENTRY=lib), so the full bundle must exist on disk before any
// test file is collected. We invoke tsdown programmatically and filter to
// the `lib` config in tsdown.config.mts so the light bundle isn't rebuilt
// unnecessarily.
//
// The lib bundle is the one that actually exercises the `inline-lua` plugin
// at runtime (the light bundle stubs out the cluster modules), so this smoke
// is the only check that the inlined Lua scripts wired through Scripts.js
// match what Redis expects when EVALSHA'd. A local-only smoke would not
// catch a regression where, say, a header is dropped or the script-id
// shape changes — Redis only complains the first time a Cluster-backed
// limiter actually submits a job.

export async function setup(): Promise<void> {
  const { build } = await import("tsdown");
  await build({ filter: "lib" });
}
