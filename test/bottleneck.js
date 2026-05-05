if (process.env.BUILD === "light") {
  module.exports = require("../dist/light.js");
} else {
  module.exports = require("../src/index.js");
}
