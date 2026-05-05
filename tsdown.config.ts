import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig, type TsdownPlugin } from "tsdown";

const here = path.dirname(fileURLToPath(import.meta.url));
const pkg = JSON.parse(fs.readFileSync(path.join(here, "package.json"), "utf8")) as {
  version: string;
};

const toPosix = (id: string) => id.replace(/\\/g, "/");
const cleanFor = (basename: string) => [`dist/${basename}.js`, `dist/${basename}.js.map`];

const inlineLua: TsdownPlugin = {
  name: "inline-lua",
  load(id) {
    const norm = id.replace(/\\/g, "/");
    if (!norm.endsWith("/src/redis/index.js")) return null;
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
const lightExclude = new Set([
  "RedisDatastore.js",
  "RedisConnection.js",
  "IORedisConnection.js",
  "Scripts.js",
]);

const excludeClustering: TsdownPlugin = {
  name: "exclude-clustering",
  load(id) {
    const file = id.split("/").pop();
    if (file && lightExclude.has(file)) return stub;
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
    name: "lib",
    entry: ["src/index.js"],
    format: "cjs",
    outDir: "dist",
    platform: "node",
    target: "es2023",
    plugins: [inlineLua, inlinePkgVersion],
    clean: cleanFor("index"),
    sourcemap: true,
    dts: false,
    report: false,
    fixedExtension: false,
    hash: false,
    banner: { js: libBanner },
  },
  {
    name: "light",
    entry: { light: "src/index.js" },
    format: "umd",
    globalName: "Bottleneck",
    outDir: "dist",
    platform: "neutral",
    target: "es2023",
    plugins: [excludeClustering, inlinePkgVersion],
    clean: cleanFor("light"),
    sourcemap: true,
    dts: false,
    report: false,
    hash: false,
    outputOptions: { entryFileNames: "[name].js" },
    banner: { js: lightBanner },
  },
]);
