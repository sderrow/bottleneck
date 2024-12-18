/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS206: Consider reworking classes to avoid initClass
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/main/docs/suggestions.md
 */
const parser = require("./parser");
const Events = require("./Events");
const Scripts = require("./Scripts");

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
    // Obfuscated or else Webpack/Angular will try to inline the optional redis module. To override this behavior: pass the redis module to Bottleneck as the 'Redis' option.
    this.Redis ??= eval("require")("redis");
    this.Events ??= new Events(this);
    this.terminated = false;

    this.client ??= this.Redis.createClient(this.clientOptions);
    this.subscriber = this.client.duplicate();
    this.limiters = {};
    this.shas = {};

    this.ready = Promise.all([this._setup(this.client, false), this._setup(this.subscriber, true)])
      .then(() => this._loadScripts())
      .then(() => ({ client: this.client, subscriber: this.subscriber }));
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
      if (client.ready) {
        resolve();
      } else {
        client.once("ready", resolve);
      }
    });
  }

  _loadScript(name) {
    return new Promise((resolve, reject) => {
      const payload = Scripts.payload(name);
      this.client.multi([["script", "load", payload]]).exec((err, replies) => {
        if (err != null) {
          reject(err);
        }
        this.shas[name] = replies[0];
        resolve(replies[0]);
      });
    });
  }

  _loadScripts() {
    return Promise.all(Scripts.names.map((k) => this._loadScript(k)));
  }

  async __runCommand__(cmd) {
    await this.ready;
    return new Promise((resolve, reject) => {
      this.client.multi([cmd]).exec_atomic(function (err, replies) {
        if (err != null) {
          reject(err);
        } else {
          resolve(replies[0]);
        }
      });
    });
  }

  async __addLimiter__(instance) {
    await Promise.all(
      [instance.channel(), instance.channel_client()].map((channel) => {
        return new Promise((resolve) => {
          var handler = (chan) => {
            if (chan === channel) {
              this.subscriber.removeListener("subscribe", handler);
              this.limiters[channel] = instance;
              resolve();
            }
          };
          this.subscriber.on("subscribe", handler);
          this.subscriber.subscribe(channel);
        });
      }),
    );
  }

  async __removeLimiter__(instance) {
    await Promise.all(
      [instance.channel(), instance.channel_client()].map(async (channel) => {
        if (!this.terminated) {
          await new Promise((resolve, reject) => {
            return this.subscriber.unsubscribe(channel, function (err, chan) {
              if (err != null) {
                return reject(err);
              }
              if (chan === channel) {
                return resolve();
              }
            });
          });
        }
        delete this.limiters[channel];
      }),
    );
  }

  __scriptArgs__(name, id, args, cb) {
    const keys = Scripts.keys(name, id);
    return [this.shas[name], keys.length].concat(keys, args, cb);
  }

  __scriptFn__() {
    return this.client.evalsha.bind(this.client);
  }

  async disconnect(flush = true) {
    for (const v of Object.values(this.limiters)) {
      clearInterval(v._store.heartbeat);
    }
    this.limiters = {};
    this.terminated = true;

    this.client.end(flush);
    this.subscriber.end(flush);
  }
}

module.exports = RedisConnection;
