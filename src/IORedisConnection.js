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
  datastore = "ioredis";
  defaults = {
    Redis: null,
    clientOptions: {},
    clusterNodes: null,
    client: null,
    Events: null,
  };

  constructor(options) {
    options ??= {};
    parser.load(options, this.defaults, this);

    // Obfuscated or else Webpack/Angular will try to inline the optional ioredis module. To override this behavior: pass the ioredis module to Bottleneck as the 'Redis' option.
    this.Redis ??= eval("require")("ioredis");
    this.Events ??= new Events(this);
    this.terminated = false;

    if (this.clusterNodes != null) {
      this.client = new this.Redis.Cluster(this.clusterNodes, this.clientOptions);
      this.subscriber = new this.Redis.Cluster(this.clusterNodes, this.clientOptions);
    } else if (this.client != null && this.client.duplicate == null) {
      this.subscriber = new this.Redis.Cluster(this.client.startupNodes, this.client.options);
    } else {
      this.client ??= new this.Redis(this.clientOptions);
      this.subscriber = this.client.duplicate();
    }
    this.limiters = {};

    this.ready = Promise.all([
      this._setup(this.client, false),
      this._setup(this.subscriber, true),
    ]).then(() => {
      this._loadScripts();
      return { client: this.client, subscriber: this.subscriber };
    });
  }

  _setup(client, sub) {
    client.setMaxListeners(0);
    return new Promise((resolve) => {
      client.on("error", (e) => this.Events.trigger("error", e));
      if (sub) {
        client.on("message", (channel, message) => {
          this.limiters[channel]?._store.onMessage(channel, message);
        });
      }
      if (client.status === "ready") {
        resolve();
      } else {
        client.once("ready", resolve);
      }
    });
  }

  _loadScripts() {
    return Scripts.names.forEach((name) =>
      this.client.defineCommand(name, { lua: Scripts.payload(name) }),
    );
  }

  async __runCommand__(cmd) {
    await this.ready;
    const [[, deleted]] = await this.client.pipeline([cmd]).exec();
    return deleted;
  }

  async __addLimiter__(instance) {
    await Promise.all(
      [instance.channel(), instance.channel_client()].map((channel) => {
        return new Promise((resolve) => {
          this.subscriber.subscribe(channel, () => {
            this.limiters[channel] = instance;
            resolve();
          });
        });
      }),
    );
  }

  async __removeLimiter__(instance) {
    await Promise.all(
      [instance.channel(), instance.channel_client()].map(async (channel) => {
        if (!this.terminated) {
          await this.subscriber.unsubscribe(channel);
        }
        delete this.limiters[channel];
      }),
    );
  }

  __scriptArgs__(name, id, args, cb) {
    const keys = Scripts.keys(name, id);
    return [keys.length].concat(keys, args, cb);
  }

  __scriptFn__(name) {
    return this.client[name].bind(this.client);
  }

  async disconnect(flush = true) {
    for (const v of Object.values(this.limiters)) {
      clearInterval(v._store.heartbeat);
    }
    this.limiters = {};
    this.terminated = true;

    if (flush) {
      await Promise.all([this.client.quit(), this.subscriber.quit()]);
    } else {
      this.client.disconnect();
      this.subscriber.disconnect();
    }
  }
}

module.exports = IORedisConnection;
