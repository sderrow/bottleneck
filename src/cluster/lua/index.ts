import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const dir = path.dirname(fileURLToPath(import.meta.url));

const lua: Record<string, string> = Object.fromEntries(
  fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".lua"))
    .sort()
    .map((f) => [f, fs.readFileSync(path.join(dir, f), "utf8")]),
);

export default lua;
