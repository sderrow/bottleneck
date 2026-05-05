const Bottleneck = (() => {
  switch (process.env.BUILD) {
    case "light":
      return require("../dist/light.js");
    case "full":
      return require("../dist/index.js");
    default:
      return require("../src/index.js");
  }
})();

const usingRedis = process.env.DATASTORE === "redis" || process.env.DATASTORE === "ioredis";

if (!usingRedis) {
  module.exports = Bottleneck;
} else {
  const Redis = process.env.DATASTORE === "redis" ? require("redis") : require("ioredis");

  // Tests construct limiters and Groups directly via `new Bottleneck({ datastore: ... })`
  // without threading `Redis` through every call site. We subclass here to inject it once.
  const withRedis = (options) => {
    if (
      options != null &&
      typeof options === "object" &&
      (options.datastore === "redis" || options.datastore === "ioredis") &&
      options.Redis == null &&
      options.connection == null &&
      options.client == null
    ) {
      return { ...options, Redis };
    }
    return options;
  };

  // `Group.limiters()` returns instances of the real `Bottleneck`, so we override
  // `Symbol.hasInstance` to keep `instanceof` checks in tests behaving as expected.
  class TestBottleneck extends Bottleneck {
    static [Symbol.hasInstance](instance) {
      return instance instanceof Bottleneck;
    }
    constructor(options) {
      super(withRedis(options));
    }
  }

  TestBottleneck.Group = class TestGroup extends Bottleneck.Group {
    static [Symbol.hasInstance](instance) {
      return instance instanceof Bottleneck.Group;
    }
    constructor(options) {
      super(withRedis(options));
    }
  };

  module.exports = TestBottleneck;
}
