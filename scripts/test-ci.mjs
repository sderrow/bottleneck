#!/usr/bin/env node
// Locally recreates the test matrices from .github/workflows/ci.yaml so they
// can be confirmed before pushing:
//   cluster → "Cluster (image)" jobs:           the ioredis + node-redis suites
//                                               plus the lib smoke test, once per
//                                               supported redis/valkey image
//   clients → "Clients (project@version)" jobs: each old client pinned against
//                                               the oldest supported server
// (The redis-free projects are one `pnpm run test:no-cluster` away; the checks
// job is `pnpm run format:check && pnpm run lint && pnpm tsc && pnpm run build`.)
//
// Legs run sequentially (one Redis testcontainer at a time keeps Docker
// overhead and flakes down) with CI's fail-fast: false semantics: every leg
// runs, results are summarized, and the exit code is non-zero if any failed.
//
// Assumes the same environment as any test run here: pnpm and a running Docker
// daemon. Matrix inputs come from ./ci-matrix.mjs — the same single source of
// truth .github/workflows/ci.yaml consumes.

import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { CLIENT_MATRIX_IMAGE, CLIENT_PINS, CLUSTER_IMAGES } from "./ci-matrix.mjs";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const USAGE = `Locally recreates the CI test matrices from .github/workflows/ci.yaml.

Usage:
  pnpm run test:ci [leg...] [--image=<name,...>] [--client=<client@version,...>] [--dry-run]

Legs (default: all, run in CI order):
  cluster  test:cluster + test:lib per redis/valkey image   ("Cluster (image)" jobs)
  clients  each pinned client against the oldest server     ("Clients (...)" jobs)

Options:
  --image=    subset of cluster images (e.g. --image=redis:6-alpine,valkey/valkey:9-alpine)
  --client=   subset of client pins (ioredis@5, redis@4, redis@5)
  --dry-run   print the resolved plan without running anything
  -h, --help  show this help

Notes:
  The client legs temporarily pin the client devDependency in package.json and
  pnpm-lock.yaml exactly like CI; both files are restored and a final
  \`pnpm install\` re-syncs node_modules, so nothing is left mutated.`;

const tty = process.stdout.isTTY;
const color = (code, text) => (tty ? `\x1b[${code}m${text}\x1b[0m` : text);
const dim = (text) => color(2, text);
const green = (text) => color(32, text);
const red = (text) => color(31, text);
const bold = (text) => color(1, text);

function fatal(message) {
  console.error(red(`error: ${message}`));
  process.exit(2);
}

function formatDuration(ms) {
  if (ms < 1000) return `${ms}ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${String(Math.round(seconds % 60)).padStart(2, "0")}s`;
}

function formatCmd(cmd, env = {}) {
  const prefix = Object.entries(env)
    .map(([key, value]) => `${key}=${value}`)
    .join(" ");
  return prefix ? `${prefix} ${cmd.join(" ")}` : cmd.join(" ");
}

// ---------------------------------------------------------------- arguments

const LEG_NAMES = ["cluster", "clients"];
const args = process.argv.slice(2);
const imageFilter = [];
const clientFilter = [];
const explicitLegs = [];
let dryRun = false;

for (let i = 0; i < args.length; i++) {
  const arg = args[i];
  if (arg === "-h" || arg === "--help") {
    console.log(USAGE);
    process.exit(0);
  } else if (arg === "--dry-run") {
    dryRun = true;
  } else if (arg === "--image") {
    const value = args[++i];
    if (!value) fatal("Missing value for --image");
    imageFilter.push(...value.split(",").filter(Boolean));
  } else if (arg.startsWith("--image=")) {
    imageFilter.push(...arg.slice(8).split(",").filter(Boolean));
  } else if (arg === "--client") {
    const value = args[++i];
    if (!value) fatal("Missing value for --client");
    clientFilter.push(...value.split(",").filter(Boolean));
  } else if (arg.startsWith("--client=")) {
    clientFilter.push(...arg.slice(9).split(",").filter(Boolean));
  } else if (LEG_NAMES.includes(arg)) {
    explicitLegs.push(arg);
  } else {
    fatal(`Unknown argument: ${arg}\n\n${USAGE}`);
  }
}

for (const image of imageFilter) {
  if (!CLUSTER_IMAGES.includes(image)) {
    fatal(`Unknown image "${image}". Supported: ${CLUSTER_IMAGES.join(", ")}`);
  }
}
const pinKeys = CLIENT_PINS.map((pin) => `${pin.client}@${pin.version}`);
for (const pin of clientFilter) {
  if (!pinKeys.includes(pin)) {
    fatal(`Unknown client pin "${pin}". Supported: ${pinKeys.join(", ")}`);
  }
}

const selectedLegs = new Set(explicitLegs.length ? explicitLegs : LEG_NAMES);
if (imageFilter.length) selectedLegs.add("cluster");
if (clientFilter.length) selectedLegs.add("clients");

// -------------------------------------------------------------------- plan

const plan = [];
if (selectedLegs.has("cluster")) {
  for (const image of imageFilter.length ? imageFilter : CLUSTER_IMAGES) {
    plan.push({
      name: `Cluster (${image})`,
      steps: [
        {
          title: "test:cluster",
          cmd: ["pnpm", "run", "test:cluster"],
          env: { REDIS_IMAGE: image },
        },
        { title: "test:lib", cmd: ["pnpm", "run", "test:lib"], env: { REDIS_IMAGE: image } },
      ],
    });
  }
}
if (selectedLegs.has("clients")) {
  for (const pin of CLIENT_PINS.filter(
    (entry) => !clientFilter.length || clientFilter.includes(`${entry.client}@${entry.version}`),
  )) {
    plan.push({
      name: `Clients (${pin.project}@${pin.version})`,
      steps: [
        {
          title: `pnpm add -D ${pin.client}@${pin.version}`,
          cmd: ["pnpm", "add", "-D", `${pin.client}@${pin.version}`],
          pin,
        },
        {
          title: `vitest run --project ${pin.project}`,
          cmd: ["pnpm", "exec", "vitest", "run", "--project", pin.project],
          env: { REDIS_IMAGE: CLIENT_MATRIX_IMAGE },
        },
      ],
    });
  }
}

if (dryRun) {
  for (const leg of plan) {
    console.log(`\n${bold(`▶ ${leg.name}`)}`);
    for (const step of leg.steps) console.log(`  $ ${formatCmd(step.cmd, step.env)}`);
  }
  console.log(dim(`\n${plan.length} leg(s) selected. Re-run without --dry-run to execute.`));
  process.exit(0);
}

// ------------------------------------------------------------------ runner

function exec(cmd, env = {}) {
  console.log(dim(`  $ ${formatCmd(cmd, env)}`));
  const result = spawnSync(cmd[0], cmd.slice(1), {
    cwd: ROOT,
    env: { ...process.env, ...env },
    stdio: "inherit",
  });
  if (result.error) {
    console.error(red(`  ✗ ${result.error.message}`));
    return false;
  }
  if (result.status !== 0) {
    const why = result.signal ? `killed by ${result.signal}` : `exit code ${result.status}`;
    console.error(red(`  ✗ ${formatCmd(cmd)} failed (${why})`));
    return false;
  }
  return true;
}

// The client legs reproduce CI's "Pin client version" step: package.json and
// pnpm-lock.yaml are mutated then restored. spawnSync blocks the event loop,
// so the restore is wired into SIGINT/SIGTERM instead of relying on finally;
// a hard SIGKILL is recoverable with `git checkout package.json pnpm-lock.yaml`.
let savedManifests;
let pinnedThisRun = false;

function saveManifests() {
  savedManifests = new Map(
    ["package.json", "pnpm-lock.yaml"].map((rel) => [rel, readFileSync(path.join(ROOT, rel))]),
  );
}

function restoreManifests() {
  if (!savedManifests) return;
  for (const [rel, contents] of savedManifests) writeFileSync(path.join(ROOT, rel), contents);
  savedManifests = undefined;
}

let finishing = false;
function finish(code) {
  if (finishing) return;
  finishing = true;
  restoreManifests();
  process.exit(code);
}

process.on("SIGINT", () => finish(130));
process.on("SIGTERM", () => finish(143));

function runClientLeg(leg) {
  saveManifests();
  pinnedThisRun = true;
  const [pinStep, testStep] = leg.steps;
  let ok = exec(pinStep.cmd);
  if (ok) ok = exec(testStep.cmd, testStep.env);
  restoreManifests();
  return ok;
}

const results = [];
for (const [index, leg] of plan.entries()) {
  console.log(`\n${bold(`▶ ${leg.name}`)} ${dim(`(${index + 1}/${plan.length})`)}`);
  const startedAt = Date.now();
  let ok;
  if (leg.steps.some((step) => step.pin)) {
    ok = runClientLeg(leg);
  } else {
    ok = true;
    for (const step of leg.steps) {
      if (!exec(step.cmd, step.env)) {
        ok = false;
        break;
      }
    }
  }
  results.push({ leg, ok, ms: Date.now() - startedAt });
}

if (pinnedThisRun) {
  console.log(`\n${bold("▶ Restore dependencies")} ${dim("(undo client version pins)")}`);
  exec(["pnpm", "install"]);
}

// ----------------------------------------------------------------- summary

console.log(`\n${bold("Summary")}`);
const width = Math.max(...results.map((result) => result.leg.name.length));
for (const result of results) {
  const name = result.leg.name.padEnd(width);
  console.log(`  ${result.ok ? green("✓") : red("✗")} ${name}  ${dim(formatDuration(result.ms))}`);
}

const failed = results.filter((result) => !result.ok);
if (failed.length) {
  console.error(red(`\nOne or more test legs failed (${failed.length}/${results.length}).`));
  process.exit(1);
}
console.log(green(`\nAll ${results.length} legs passed.`));
