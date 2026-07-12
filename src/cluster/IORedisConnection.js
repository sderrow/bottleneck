const parser = require("../parser");
const Events = require("../Events");
const BottleneckError = require("../BottleneckError");
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

    if (this.Redis == null && this.client == null) {
      throw new BottleneckError(
        "Bottleneck cluster mode requires a `Redis` library reference or a pre-built `client`. " +
          "Pass it explicitly: `new Bottleneck({ datastore: 'ioredis', Redis: require('ioredis'), clientOptions })`.",
      );
    }

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

    this.ready = this._initReady();
  }

  async _initReady() {
    await Promise.all([this._setup(this.client, false), this._setup(this.subscriber, true)]);
    this._loadScripts();
    return { client: this.client, subscriber: this.subscriber };
  }

  _setup(client, sub) {
    client.setMaxListeners(0);
    return new Promise((resolve) => {
      client.on("error", (e) => {
        if (!this.terminated) {
          this.Events.trigger("error", e);
        }
      });
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
    const [[, value]] = await this.client.pipeline([cmd]).exec();
    return value;
  }

  __runScript__(name, id, args) {
    const keys = Scripts.keys(name, id);
    return this.client[name](keys.length, ...keys, ...args);
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

  async disconnect(flush = true) {
    for (const v of Object.values(this.limiters)) {
      clearInterval(v._store.heartbeat);
    }
    this.limiters = {};
    this.terminated = true;

    this.client.removeAllListeners?.("error");
    this.client.on?.("error", () => {});
    this.subscriber.removeAllListeners?.("error");
    this.subscriber.on?.("error", () => {});

    if (flush) {
      await Promise.all([this.client.quit(), this.subscriber.quit()]);
    } else {
      this.client.disconnect();
      this.subscriber.disconnect();
    }
  }
}

module.exports = IORedisConnection;
