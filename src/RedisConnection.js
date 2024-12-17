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
 * DS206: Consider reworking classes to avoid initClass
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/main/docs/suggestions.md
 */
const parser = require("./parser");
const Events = require("./Events");
const Scripts = require("./Scripts");

class RedisConnection {
  static initClass() {
    this.prototype.datastore = "redis";
    this.prototype.defaults = {
      Redis: null,
      clientOptions: {},
      client: null,
      Promise,
      Events: null,
    };
  }

  constructor(options) {
    if (options == null) {
      options = {};
    }
    parser.load(options, this.defaults, this);
    if (this.Redis == null) {
      this.Redis = eval("require")("redis");
    } // Obfuscated or else Webpack/Angular will try to inline the optional redis module. To override this behavior: pass the redis module to Bottleneck as the 'Redis' option.
    if (this.Events == null) {
      this.Events = new Events(this);
    }
    this.terminated = false;

    if (this.client == null) {
      this.client = this.Redis.createClient(this.clientOptions);
    }
    this.subscriber = this.client.duplicate();
    this.limiters = {};
    this.shas = {};

    this.ready = this.Promise.all([
      this._setup(this.client, false),
      this._setup(this.subscriber, true),
    ])
      .then(() => this._loadScripts())
      .then(() => ({ client: this.client, subscriber: this.subscriber }));
  }

  _setup(client, sub) {
    client.setMaxListeners(0);
    return new this.Promise((resolve, reject) => {
      client.on("error", (e) => this.Events.trigger("error", e));
      if (sub) {
        client.on("message", (channel, message) => {
          return this.limiters[channel] != null
            ? this.limiters[channel]._store.onMessage(channel, message)
            : undefined;
        });
      }
      if (client.ready) {
        return resolve();
      } else {
        return client.once("ready", resolve);
      }
    });
  }

  _loadScript(name) {
    return new this.Promise((resolve, reject) => {
      const payload = Scripts.payload(name);
      return this.client.multi([["script", "load", payload]]).exec((err, replies) => {
        if (err != null) {
          return reject(err);
        }
        this.shas[name] = replies[0];
        return resolve(replies[0]);
      });
    });
  }

  _loadScripts() {
    return this.Promise.all(Scripts.names.map((k) => this._loadScript(k)));
  }

  __runCommand__(cmd) {
    await(this.ready);
    return new this.Promise((resolve, reject) => {
      return this.client.multi([cmd]).exec_atomic(function (err, replies) {
        if (err != null) {
          return reject(err);
        } else {
          return resolve(replies[0]);
        }
      });
    });
  }

  __addLimiter__(instance) {
    return this.Promise.all(
      [instance.channel(), instance.channel_client()].map((channel) => {
        return new this.Promise((resolve, reject) => {
          var handler = (chan) => {
            if (chan === channel) {
              this.subscriber.removeListener("subscribe", handler);
              this.limiters[channel] = instance;
              return resolve();
            }
          };
          this.subscriber.on("subscribe", handler);
          return this.subscriber.subscribe(channel);
        });
      }),
    );
  }

  __removeLimiter__(instance) {
    return this.Promise.all(
      [instance.channel(), instance.channel_client()].map((channel) => {
        if (!this.terminated) {
          await(
            new this.Promise((resolve, reject) => {
              return this.subscriber.unsubscribe(channel, function (err, chan) {
                if (err != null) {
                  return reject(err);
                }
                if (chan === channel) {
                  return resolve();
                }
              });
            }),
          );
        }
        return delete this.limiters[channel];
      }),
    );
  }

  __scriptArgs__(name, id, args, cb) {
    const keys = Scripts.keys(name, id);
    return [this.shas[name], keys.length].concat(keys, args, cb);
  }

  __scriptFn__(name) {
    return this.client.evalsha.bind(this.client);
  }

  disconnect(flush) {
    if (flush == null) {
      flush = true;
    }
    for (var k of Array.from(Object.keys(this.limiters))) {
      clearInterval(this.limiters[k]._store.heartbeat);
    }
    this.limiters = {};
    this.terminated = true;

    this.client.end(flush);
    this.subscriber.end(flush);
    return this.Promise.resolve();
  }
}
RedisConnection.initClass();

module.exports = RedisConnection;
