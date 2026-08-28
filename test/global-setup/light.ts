// Project-level Vitest globalSetup for the `light-smoke` project.
//
// The light smoke test loads `dist/light.js` via `test/bottleneck.js`
// (BOTTLENECK_ENTRY=light), so the light bundle must exist on disk before
// any test file is collected. We invoke tsdown programmatically and filter
// to the `light` config in tsdown.config.mts so the lib bundle isn't rebuilt
// unnecessarily.
//
// Scoped to this project's globalSetup (not the root globalSetup) so other
// projects — which never read dist/light.js — don't pay the build cost.

export async function setup(): Promise<void> {
  const { build } = await import("tsdown");
  await build({ filter: "light" });
}
