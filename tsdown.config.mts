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

const lightBanner = [
  "/**",
  "  * This file contains the Bottleneck library (MIT) without Clustering support.",
  "  * https://github.com/sderrow/bottleneck",
  "  */",
].join("\n");

const libBanner = `/** Bottleneck v${pkg.version} (MIT). https://github.com/sderrow/bottleneck */\n`;

export default defineConfig([
  {
    // Dual-format Node build: ESM (dist/index.mjs) + CJS (dist/index.cjs).
    // No `fixedExtension`: under "type": "module" the ESM output gets .mjs and
    // the CJS output .cjs, which is exactly what Node infers from.
    name: "lib",
    entry: ["src/index.ts"],
    format: ["esm", "cjs"],
    outDir: "dist",
    platform: "node",
    target: "es2023",
    plugins: [inlineLua, inlinePkgVersion],
    clean: cleanFor("index.mjs", "index.cjs"),
    sourcemap: true,
    dts: true,
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
    plugins: [excludeClustering, inlinePkgVersion],
    clean: cleanFor("light.js"),
    sourcemap: true,
    dts: true,
    report: false,
    hash: false,
    outputOptions: { entryFileNames: "[name].js" },
    banner: { js: lightBanner },
  },
]);
