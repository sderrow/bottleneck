const fs = require("node:fs");
const path = require("node:path");

module.exports = Object.fromEntries(
  fs
    .readdirSync(__dirname)
    .filter((f) => f.endsWith(".lua"))
    .sort()
    .map((f) => [f, fs.readFileSync(path.join(__dirname, f), "utf8")]),
);
