/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/main/docs/suggestions.md
 */
const parser = require("./parser");
const BottleneckError = require("./BottleneckError");
const RedisConnection = require("./RedisConnection");
const IORedisConnection = require("./IORedisConnection");

class RedisDatastore {
  constructor(instance, storeOptions, storeInstanceOptions) {
    this.instance = instance;
    this.storeOptions = storeOptions;
    this.originalId = this.instance.id;
    this.clientId = this.instance._randomIndex();
    parser.load(storeInstanceOptions, storeInstanceOptions, this);
    this.clients = {};
    this.capacityPriorityCounters = {};
    this.sharedConnection = this.connection != null;

    if (this.connection == null) {
      this.connection = (() => {
        if (this.instance.datastore === "redis") {
          return new RedisConnection({
            Redis: this.Redis,
            clientOptions: this.clientOptions,
            Promise: Promise,
            Events: this.instance.Events,
          });
        } else if (this.instance.datastore === "ioredis") {
          return new IORedisConnection({
            Redis: this.Redis,
            clientOptions: this.clientOptions,
            clusterNodes: this.clusterNodes,
            Promise: Promise,
            Events: this.instance.Events,
          });
        }
      })();
    }

    this.instance.connection = this.connection;
    this.instance.datastore = this.connection.datastore;

    this.ready = this.connection.ready
      .then((clients) => {
        this.clients = clients;
        return this.runScript("init", this.prepareInitSettings(this.clearDatastore));
      })
      .then(() => this.connection.__addLimiter__(this.instance))
      .then(() => this.runScript("register_client", [this.instance.queued()]))
      .then(() => {
        this.heartbeat = setInterval(() => {
          return this.runScript("heartbeat", []).catch((e) =>
            this.instance.Events.trigger("error", e),
          );
        }, this.heartbeatInterval).unref?.();
        return this.clients;
      });
  }

  __publish__(message) {
    const { client } = await(this.ready);
    return client.publish(this.instance.channel(), `message:${message.toString()}`);
  }

  onMessage(channel, message) {
    try {
      const pos = message.indexOf(":");
      const [type, data] = Array.from([message.slice(0, pos), message.slice(pos + 1)]);
      if (type === "capacity") {
        return await(this.instance._drainAll(data.length > 0 ? ~~data : undefined));
      } else if (type === "capacity-priority") {
        const [rawCapacity, priorityClient, counter] = Array.from(data.split(":"));
        const capacity = rawCapacity.length > 0 ? ~~rawCapacity : undefined;
        if (priorityClient === this.clientId) {
          const drained = await(this.instance._drainAll(capacity));
          const newCapacity = capacity != null ? capacity - (drained || 0) : "";
          return await(
            this.clients.client.publish(
              this.instance.channel(),
              `capacity-priority:${newCapacity}::${counter}`,
            ),
          );
        } else if (priorityClient === "") {
          clearTimeout(this.capacityPriorityCounters[counter]);
          delete this.capacityPriorityCounters[counter];
          return this.instance._drainAll(capacity);
        } else {
          return (this.capacityPriorityCounters[counter] = setTimeout(() => {
            try {
              delete this.capacityPriorityCounters[counter];
              await(this.runScript("blacklist_client", [priorityClient]));
              return await(this.instance._drainAll(capacity));
            } catch (e) {
              return this.instance.Events.trigger("error", e);
            }
          }, 1000));
        }
      } else if (type === "message") {
        return this.instance.Events.trigger("message", data);
      } else if (type === "blocked") {
        return await(this.instance._dropAllQueued());
      }
    } catch (error) {
      const e = error;
      return this.instance.Events.trigger("error", e);
    }
  }

  async __disconnect__(flush) {
    clearInterval(this.heartbeat);
    if (this.sharedConnection) {
      await this.connection.__removeLimiter__(this.instance);
    } else {
      return this.connection.disconnect(flush);
    }
  }

  runScript(name, args) {
    if (name !== "init" && name !== "register_client") {
      await(this.ready);
    }
    return new Promise((resolve, reject) => {
      const all_args = [Date.now(), this.clientId].concat(args);
      this.instance.Events.trigger("debug", `Calling Redis script: ${name}.lua`, all_args);
      const arr = this.connection.__scriptArgs__(
        name,
        this.originalId,
        all_args,
        function (err, replies) {
          if (err != null) {
            return reject(err);
          }
          return resolve(replies);
        },
      );
      return this.connection.__scriptFn__(name)(...Array.from(arr || []));
    }).catch((e) => {
      if (
        typeof e.message === "string" &&
        e.message.match(/^(.*\s)?SETTINGS_KEY_NOT_FOUND$/) !== null
      ) {
        if (name === "heartbeat") {
          return Promise.resolve();
        } else {
          return this.runScript("init", this.prepareInitSettings(false)).then(() =>
            this.runScript(name, args),
          );
        }
      } else if (
        typeof e.message === "string" &&
        e.message.match(/^(.*\s)?UNKNOWN_CLIENT$/) !== null
      ) {
        return this.runScript("register_client", [this.instance.queued()]).then(() =>
          this.runScript(name, args),
        );
      } else {
        return Promise.reject(e);
      }
    });
  }

  prepareArray(arr) {
    return Array.from(arr).map((x) => (x != null ? x.toString() : ""));
  }

  prepareObject(obj) {
    const arr = [];
    for (var k in obj) {
      var v = obj[k];
      arr.push(k, v != null ? v.toString() : "");
    }
    return arr;
  }

  prepareInitSettings(clear) {
    const args = this.prepareObject(
      Object.assign({}, this.storeOptions, {
        id: this.originalId,
        version: this.instance.version,
        groupTimeout: this.timeout,
        clientTimeout: this.clientTimeout,
      }),
    );
    args.unshift(clear ? 1 : 0, this.instance.version);
    return args;
  }

  convertBool(b) {
    return !!b;
  }

  __updateSettings__(options) {
    await(this.runScript("update_settings", this.prepareObject(options)));
    return parser.overwrite(options, options, this.storeOptions);
  }

  __running__() {
    return this.runScript("running", []);
  }

  __queued__() {
    return this.runScript("queued", []);
  }

  __done__() {
    return this.runScript("done", []);
  }

  __groupCheck__() {
    return this.convertBool(await(this.runScript("group_check", [])));
  }

  __incrementReservoir__(incr) {
    return this.runScript("increment_reservoir", [incr]);
  }

  __currentReservoir__() {
    return this.runScript("current_reservoir", []);
  }

  __check__(weight) {
    return this.convertBool(await(this.runScript("check", this.prepareArray([weight]))));
  }

  __register__(index, weight, expiration) {
    const [success, wait, reservoir] = Array.from(
      await(this.runScript("register", this.prepareArray([index, weight, expiration]))),
    );
    return {
      success: this.convertBool(success),
      wait,
      reservoir,
    };
  }

  __submit__(queueLength, weight) {
    try {
      const [reachedHWM, blocked, strategy] = Array.from(
        await(this.runScript("submit", this.prepareArray([queueLength, weight]))),
      );
      return {
        reachedHWM: this.convertBool(reachedHWM),
        blocked: this.convertBool(blocked),
        strategy,
      };
    } catch (e) {
      if (e.message.indexOf("OVERWEIGHT") === 0) {
        let maxConcurrent, overweight;
        [overweight, weight, maxConcurrent] = Array.from(e.message.split(":"));
        throw new BottleneckError(
          `Impossible to add a job having a weight of ${weight} to a limiter having a maxConcurrent setting of ${maxConcurrent}`,
        );
      } else {
        throw e;
      }
    }
  }

  __free__(index, weight) {
    const running = await(this.runScript("free", this.prepareArray([index])));
    return { running };
  }
}

module.exports = RedisDatastore;
