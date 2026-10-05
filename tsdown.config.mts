import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig, type TsdownPlugin } from "tsdown";

const here = path.dirname(fileURLToPath(import.meta.url));
const pkg = JSON.parse(fs.readFileSync(path.join(here, "package.json"), "utf8")) as {
  version: string;
};

const toPosix = (id: string) => id.replace(/\\/g, "/");
const isClusterModule = (id: string) => toPosix(id).includes("/src/cluster/");
const isLuaIndex = (id: string) => toPosix(id).endsWith("/src/cluster/lua/index.ts");

const cleanFor = (...basenames: string[]) =>
  basenames.flatMap((b) => [`dist/${b}`, `dist/${b}.map`]);

const inlineLua: TsdownPlugin = {
  name: "inline-lua",
  load(id) {
    if (!isLuaIndex(id)) return null;
    const dir = path.dirname(id);
    const luaFiles = fs
      .readdirSync(dir)
      .filter((f) => f.endsWith(".lua"))
      .sort();
    for (const f of luaFiles) {
      this.addWatchFile(path.join(dir, f));
    }
    const lua = Object.fromEntries(
      luaFiles.map((f) => [f, fs.readFileSync(path.join(dir, f), "utf8")]),
    );
    return `module.exports = ${JSON.stringify(lua)};`;
  },
};

const stub = `
class Stub { constructor() { throw new Error("You must import the full version of Bottleneck to use clustering."); } }
module.exports = Stub;
`;

const excludeClustering: TsdownPlugin = {
  name: "exclude-clustering",
  load(id) {
    if (isClusterModule(id) && !isLuaIndex(id)) return stub;
  },
};

// `src/async-context.ts` statically imports `node:async_hooks`, which has no
// browser equivalent. The light build redirects that import to a shim with no
// AsyncResource constructor, so tasks simply run without async context.
const shimAsyncHooks: TsdownPlugin = {
  name: "shim-async-hooks",
  resolveId(id) {
    if (id === "node:async_hooks") return "virtual:async-hooks-shim";
  },
  load(id) {
    if (id === "virtual:async-hooks-shim") {
      return {
        code: "export const AsyncResource = {};",
        moduleType: "js",
      };
    }
  },
};

const inlinePkgVersion: TsdownPlugin = {
  name: "inline-pkg-version",
  load(id) {
    if (toPosix(id).endsWith("/package.json")) {
      return {
        code: `module.exports = ${JSON.stringify({ version: pkg.version })};`,
        moduleType: "js",
      };
    }
    return null;
  },
};

// Keeps the v2 CJS shape (`require() === Bottleneck class`): rolldown emits
// a namespace, so reassign `module.exports` and reattach `.default` (no
// esModuleInterop) and `.Bottleneck` (named-export destructuring). Group /
// Batcher / ... resolve via Bottleneck's own statics (real API, not interop).
// A footer (not a plugin) keeps sourcemaps intact. Must stay JS-only:
// statements are illegal in .d.ts (TS1036).
const cjsFooter = (ctx: { format: string }): string | undefined =>
  ctx.format === "cjs"
    ? "\nmodule.exports = exports.default;\nmodule.exports.default = exports.default;\nmodule.exports.Bottleneck = exports.default;\n"
    : undefined;

const lightBanner = [
  "/**",
  "  * This file contains the Bottleneck library (MIT) without Clustering support.",
  "  * https://github.com/sderrow/bottleneck",
  "  */",
].join("\n");

const libBanner = `/** Bottleneck v${pkg.version} (MIT). https://github.com/sderrow/bottleneck */\n`;

export default defineConfig([
  {
    // ESM build + the single published typings (index.d.mts). One types file
    // for both conditions keeps resolution and auto-import on the same file;
    // dual typings break IDE hints under `module: commonjs` + `bundler`.
    // Accepted casualty: `module: node16` CJS (TS1479); nodenext/bundler fine.
    // No `fixedExtension`: under "type": "module" the ESM output gets .mjs,
    // which is exactly what Node infers from.
    name: "lib-esm",
    entry: ["src/index.ts"],
    format: "esm",
    outDir: "dist",
    platform: "node",
    target: "es2023",
    plugins: [inlineLua, inlinePkgVersion],
    clean: cleanFor("index.mjs", "index.d.mts"),
    sourcemap: true,
    dts: true,
    report: false,
    hash: false,
    banner: { js: libBanner },
  },
  {
    // CJS runtime only, no declarations. The footer above restores the v2 shape.
    name: "lib-cjs",
    entry: ["src/index.ts"],
    format: "cjs",
    outDir: "dist",
    platform: "node",
    target: "es2023",
    plugins: [inlineLua, inlinePkgVersion],
    // Named + default exports: "named" is the intended CJS shape (the footer
    // below then restores `module.exports` for v2 compatibility).
    outputOptions: { exports: "named" },
    footer: cjsFooter,
    clean: cleanFor("index.cjs"),
    sourcemap: true,
    dts: false,
    report: false,
    hash: false,
    banner: { js: libBanner },
  },
  {
    // Browser build: ESM for `<script type="module">` and bundlers.
    // Clustering is stubbed out (it is Node-only) so the bundle stays small.
    name: "light",
    entry: { light: "src/index.ts" },
    format: "esm",
    outDir: "dist",
    platform: "browser",
    target: "es2023",
    plugins: [excludeClustering, shimAsyncHooks, inlinePkgVersion],
    clean: cleanFor("light.js"),
    sourcemap: true,
    dts: true,
    report: false,
    hash: false,
    outputOptions: { entryFileNames: "[name].js" },
    banner: { js: lightBanner },
  },
]);
