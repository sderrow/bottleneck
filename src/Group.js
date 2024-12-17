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
const RedisConnection = require("./RedisConnection");
const IORedisConnection = require("./IORedisConnection");
const Scripts = require("./Scripts");

class Group {
  static initClass() {
    this.prototype.defaults = {
      timeout: 1000 * 60 * 5,
      connection: null,
      Promise,
      id: "group-key"
    };
  }

  constructor(limiterOptions) {
    this.deleteKey = this.deleteKey.bind(this);
    if (limiterOptions == null) { limiterOptions = {}; }
    this.limiterOptions = limiterOptions;
    parser.load(this.limiterOptions, this.defaults, this);
    this.Events = new Events(this);
    this.instances = {};
    this.Bottleneck = require("./Bottleneck");
    this._startAutoCleanup();
    this.sharedConnection = (this.connection != null);

    if ((this.connection == null)) {
      if (this.limiterOptions.datastore === "redis") {
        this.connection = new RedisConnection(Object.assign({}, this.limiterOptions, { Events: this.Events }));
      } else if (this.limiterOptions.datastore === "ioredis") {
        this.connection = new IORedisConnection(Object.assign({}, this.limiterOptions, { Events: this.Events }));
      }
    }
  }

  key(key) { if (key == null) { key = ""; } return this.instances[key] != null ? this.instances[key] : (() => {
    const limiter = (this.instances[key] = new this.Bottleneck(Object.assign(this.limiterOptions, {
      id: `${this.id}-${key}`,
      timeout: this.timeout,
      connection: this.connection
    })));
    this.Events.trigger("created", limiter, key);
    return limiter;
  })(); }

  deleteKey(key) {
    let deleted;
    if (key == null) { key = ""; }
    const instance = this.instances[key];
    if (this.connection) {
      deleted = await(this.connection.__runCommand__(['del', ...Array.from(Scripts.allKeys(`${this.id}-${key}`))]));
    }
    if (instance != null) {
      delete this.instances[key];
      await(instance.disconnect());
    }
    return (instance != null) || (deleted > 0);
  }

  limiters() { return (() => {
    const result = [];
    for (var k in this.instances) {
      var v = this.instances[k];
      result.push({ key: k, limiter: v });
    }
    return result;
  })(); }

  keys() { return Object.keys(this.instances); }

  clusterKeys() {
    if ((this.connection == null)) { return this.Promise.resolve(this.keys()); }
    const keys = [];
    let cursor = null;
    const start = `b_${this.id}-`.length;
    const end = "_settings".length;
    while (cursor !== 0) {
      var [next, found] = Array.from(await(this.connection.__runCommand__(["scan", (cursor != null ? cursor : 0), "match", `b_${this.id}-*_settings`, "count", 10000])));
      cursor = ~~next;
      for (var k of Array.from(found)) { keys.push(k.slice(start, -end)); }
    }
    return keys;
  }

  _startAutoCleanup() {
    clearInterval(this.interval);
    return __guardMethod__((this.interval = setInterval(() => {
      const time = Date.now();
      return (() => {
        const result = [];
        for (var k in this.instances) {
          var v = this.instances[k];
          try { if (await(v._store.__groupCheck__(time))) { result.push(this.deleteKey(k)); } else {
            result.push(undefined);
          } }
          catch (e) { result.push(v.Events.trigger("error", e)); }
        }
        return result;
      })();
    }
    , (this.timeout / 2))), 'unref', o => o.unref());
  }

  updateSettings(options) {
    if (options == null) { options = {}; }
    parser.overwrite(options, this.defaults, this);
    parser.overwrite(options, options, this.limiterOptions);
    if (options.timeout != null) { return this._startAutoCleanup(); }
  }

  disconnect(flush) {
    if (flush == null) { flush = true; }
    if (!this.sharedConnection) {
      return (this.connection != null ? this.connection.disconnect(flush) : undefined);
    }
  }
}
Group.initClass();

module.exports = Group;

function __guardMethod__(obj, methodName, transform) {
  if (typeof obj !== 'undefined' && obj !== null && typeof obj[methodName] === 'function') {
    return transform(obj, methodName);
  } else {
    return undefined;
  }
}