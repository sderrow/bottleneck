const parser = require("../parser");
const Events = require("../Events");
const BottleneckError = require("../BottleneckError");
const Scripts = require("./Scripts");

const closeClient = (c) => (typeof c.close === "function" ? c.close() : c.quit());
const destroyClient = (c) => (typeof c.destroy === "function" ? c.destroy() : c.disconnect());
const safe = (run) =>
  Promise.resolve()
    .then(run)
    .catch(() => undefined);

const connectIfNeeded = async (c) => {
  if (typeof c.connect === "function" && c.isOpen === false) {
    await c.connect();
  }
};

const stringifyArgs = (args) =>
  args.map((a) => (a == null ? "" : typeof a === "string" ? a : String(a)));

class RedisConnection {
  defaults = {
    Redis: null,
    clientOptions: {},
    client: null,
    Events: null,
  };
  datastore = "redis";

  constructor(options) {
    options ??= {};
    parser.load(options, this.defaults, this);

    if (this.Redis == null && this.client == null) {
      throw new BottleneckError(
        "Bottleneck cluster mode requires a `Redis` library reference or a pre-built `client`. " +
          "Pass it explicitly: `new Bottleneck({ datastore: 'redis', Redis: require('redis'), clientOptions })`.",
      );
    }

    this.Events ??= new Events(this);
    this.terminated = false;
    this.shas = {};

    this.client ??= this.Redis.createClient(this.clientOptions);
    this.subscriber = this.client.duplicate();
    this.limiters = {};

    this.ready = Promise.all([this._setup(this.client, false), this._setup(this.subscriber, true)])
      .then(() => this._loadScripts())
      .then(() => ({ client: this.client, subscriber: this.subscriber }));
  }

  async _setup(client, _sub) {
    client.setMaxListeners?.(0);
    client.on("error", (e) => this.Events.trigger("error", e));
    await connectIfNeeded(client);
  }

  async _loadScript(name) {
    this.shas[name] = await this.client.scriptLoad(Scripts.payload(name));
    return this.shas[name];
  }

  _loadScripts() {
    return Promise.all(Scripts.names.map((k) => this._loadScript(k)));
  }

  async __runCommand__(cmd) {
    await this.ready;
    const reply = await this.client.sendCommand(stringifyArgs(cmd));
    const name = String(cmd[0]).toLowerCase();
    if (name === "hgetall" && Array.isArray(reply)) {
      const obj = {};
      for (let i = 0; i < reply.length; i += 2) {
        obj[reply[i]] = reply[i + 1];
      }
      return obj;
    }
    return reply;
  }

  async __runScript__(name, id, args) {
    const keys = Scripts.keys(name, id);
    const stringArgs = stringifyArgs(args);
    try {
      return await this.client.evalSha(this.shas[name], { keys, arguments: stringArgs });
    } catch (e) {
      if (typeof e?.message === "string" && e.message.startsWith("NOSCRIPT")) {
        await this._loadScript(name);
        return this.client.evalSha(this.shas[name], { keys, arguments: stringArgs });
      }
      throw e;
    }
  }

  async __addLimiter__(instance) {
    await Promise.all(
      [instance.channel(), instance.channel_client()].map(async (channel) => {
        await this.subscriber.subscribe(channel, (message) => {
          this.limiters[channel]?._store.onMessage(channel, message);
        });
        this.limiters[channel] = instance;
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
    if (this.terminated) return;
    this.terminated = true;

    this.client.removeAllListeners?.("error");
    this.client.on?.("error", () => {});
    this.subscriber.removeAllListeners?.("error");
    this.subscriber.on?.("error", () => {});

    if (flush) {
      await Promise.all([
        safe(() => closeClient(this.client)),
        safe(() => closeClient(this.subscriber)),
      ]);
    } else {
      await Promise.all([
        safe(() => destroyClient(this.client)),
        safe(() => destroyClient(this.subscriber)),
      ]);
    }
  }
}

module.exports = RedisConnection;
