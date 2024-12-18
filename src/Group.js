/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS103: Rewrite code to no longer use __guard__, or convert again using --optional-chaining
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS206: Consider reworking classes to avoid initClass
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/main/docs/suggestions.md
 */
const parser = require("./parser");
const Events = require("./Events");
const Bottleneck = require("./Bottleneck");
const RedisConnection = require("./RedisConnection");
const IORedisConnection = require("./IORedisConnection");
const Scripts = require("./Scripts");

class Group {
  defaults = {
    timeout: 1000 * 60 * 5,
    connection: null,
    id: "group-key",
  };

  constructor(limiterOptions) {
    this.limiterOptions = limiterOptions ?? {};
    parser.load(this.limiterOptions, this.defaults, this);
    this.Events = new Events(this);
    this.instances = {};
    this._startAutoCleanup();
    this.sharedConnection = this.connection != null;

    if (this.connection == null) {
      if (this.limiterOptions.datastore === "redis") {
        this.connection = new RedisConnection(
          Object.assign({}, this.limiterOptions, { Events: this.Events }),
        );
      } else if (this.limiterOptions.datastore === "ioredis") {
        this.connection = new IORedisConnection(
          Object.assign({}, this.limiterOptions, { Events: this.Events }),
        );
      }
    }
  }

  key(key) {
    key ??= "";

    let limiter = this.instances[key];
    if (!limiter) {
      limiter = new Bottleneck(
        Object.assign(this.limiterOptions, {
          id: `${this.id}-${key}`,
          timeout: this.timeout,
          connection: this.connection,
        }),
      );
      this.Events.trigger("created", limiter, key);
      this.instances[key] = limiter;
    }
    return limiter;
  }

  async deleteKey(key) {
    let deleted;
    key ??= "";

    const instance = this.instances[key];
    if (this.connection) {
      deleted = await this.connection.__runCommand__([
        "del",
        ...Scripts.allKeys(`${this.id}-${key}`),
      ]);
    }
    if (instance != null) {
      delete this.instances[key];
      await instance.disconnect();
    }
    return instance != null || deleted > 0;
  }

  limiters() {
    return Object.entries(this.instances).map(([key, limiter]) => ({ key, limiter }));
  }

  keys() {
    return Object.keys(this.instances);
  }

  async clusterKeys() {
    if (this.connection == null) {
      return Promise.resolve(this.keys());
    }
    const keys = [];
    let cursor = null;
    const start = `b_${this.id}-`.length;
    const end = "_settings".length;
    while (cursor !== 0) {
      const [next, found] = Array.from(
        await this.connection.__runCommand__([
          "scan",
          cursor ?? 0,
          "match",
          `b_${this.id}-*_settings`,
          "count",
          10000,
        ]),
      );
      cursor = ~~next;
      for (const k of found) {
        keys.push(k.slice(start, -end));
      }
    }
    return keys;
  }

  _startAutoCleanup() {
    clearInterval(this.interval);

    this.interval = setInterval(async () => {
      const time = Date.now();
      for (const [k, v] of Object.entries(this.instances)) {
        try {
          if (await v._store.__groupCheck__(time)) {
            this.deleteKey(k);
          }
        } catch (e) {
          v.Events.trigger("error", e);
        }
      }
    }, this.timeout / 2).unref();
  }

  updateSettings(options) {
    options ??= {};
    parser.overwrite(options, this.defaults, this);
    parser.overwrite(options, options, this.limiterOptions);
    if (options.timeout != null) {
      return this._startAutoCleanup();
    }
  }

  disconnect(flush) {
    flush ??= true;

    if (!this.sharedConnection) {
      return this.connection?.disconnect(flush);
    }
  }
}

module.exports = Group;
