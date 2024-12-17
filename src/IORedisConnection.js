/* eslint-disable
    no-undef,
    no-unused-vars,
*/
// TODO: This file was created by bulk-decaffeinate.
// Fix any style issues and re-enable lint.
/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS201: Simplify complex destructure assignments
 * DS206: Consider reworking classes to avoid initClass
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/main/docs/suggestions.md
 */
const parser = require("./parser");
const Events = require("./Events");
const Scripts = require("./Scripts");

class IORedisConnection {
  static initClass() {
    this.prototype.datastore = "ioredis";
    this.prototype.defaults = {
      Redis: null,
      clientOptions: {},
      clusterNodes: null,
      client: null,
      Promise,
      Events: null
    };
  }

  constructor(options) {
    if (options == null) { options = {}; }
    parser.load(options, this.defaults, this);
    if (this.Redis == null) { this.Redis = eval("require")("ioredis"); } // Obfuscated or else Webpack/Angular will try to inline the optional ioredis module. To override this behavior: pass the ioredis module to Bottleneck as the 'Redis' option.
    if (this.Events == null) { this.Events = new Events(this); }
    this.terminated = false;

    if (this.clusterNodes != null) {
      this.client = new this.Redis.Cluster(this.clusterNodes, this.clientOptions);
      this.subscriber = new this.Redis.Cluster(this.clusterNodes, this.clientOptions);
    } else if ((this.client != null) && (this.client.duplicate == null)) {
      this.subscriber = new this.Redis.Cluster(this.client.startupNodes, this.client.options);
    } else {
      if (this.client == null) { this.client = new this.Redis(this.clientOptions); }
      this.subscriber = this.client.duplicate();
    }
    this.limiters = {};

    this.ready = this.Promise.all([this._setup(this.client, false), this._setup(this.subscriber, true)])
    .then(() => {
      this._loadScripts();
      return { client: this.client, subscriber: this.subscriber };
  });
  }

  _setup(client, sub) {
    client.setMaxListeners(0);
    return new this.Promise((resolve, reject) => {
      client.on("error", e => this.Events.trigger("error", e));
      if (sub) {
        client.on("message", (channel, message) => {
          return (this.limiters[channel] != null ? this.limiters[channel]._store.onMessage(channel, message) : undefined);
        });
      }
      if (client.status === "ready") { return resolve();
      } else { return client.once("ready", resolve); }
    });
  }

  _loadScripts() { return Scripts.names.forEach(name => this.client.defineCommand(name, { lua: Scripts.payload(name) })); }

  __runCommand__(cmd) {
    await(this.ready);
    const array = await(this.client.pipeline([cmd]).exec()), [_, deleted] = Array.from(array[0]);
    return deleted;
  }

  __addLimiter__(instance) {
    return this.Promise.all([instance.channel(), instance.channel_client()].map(channel => {
      return new this.Promise((resolve, reject) => {
        return this.subscriber.subscribe(channel, () => {
          this.limiters[channel] = instance;
          return resolve();
        });
      });
    })
    );
  }

  __removeLimiter__(instance) {
    return [instance.channel(), instance.channel_client()].forEach(channel => {
      if (!this.terminated) { await(this.subscriber.unsubscribe(channel)); }
      return delete this.limiters[channel];
  });
  }

  __scriptArgs__(name, id, args, cb) {
    const keys = Scripts.keys(name, id);
    return [keys.length].concat(keys, args, cb);
  }

  __scriptFn__(name) {
    return this.client[name].bind(this.client);
  }

  disconnect(flush) {
    if (flush == null) { flush = true; }
    for (var k of Array.from(Object.keys(this.limiters))) { clearInterval(this.limiters[k]._store.heartbeat); }
    this.limiters = {};
    this.terminated = true;

    if (flush) {
      return this.Promise.all([this.client.quit(), this.subscriber.quit()]);
    } else {
      this.client.disconnect();
      this.subscriber.disconnect();
      return this.Promise.resolve();
    }
  }
}
IORedisConnection.initClass();

module.exports = IORedisConnection;
