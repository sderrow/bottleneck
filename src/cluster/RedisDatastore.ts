const parser = require("../parser");
const BottleneckError = require("../BottleneckError");
const RedisConnection = require("./RedisConnection");
const IORedisConnection = require("./IORedisConnection");

class RedisDatastore {
  _disconnecting = false;

  constructor(instance, storeOptions, storeInstanceOptions) {
    this.instance = instance;
    this.storeOptions = storeOptions;
    this.originalId = this.instance.id;
    this.clientId = this.instance._randomIndex();
    parser.load(storeInstanceOptions, storeInstanceOptions, this);
    this.clients = {};
    this.capacityPriorityCounters = {};
    this.sharedConnection = this.connection != null;

    if (!this.connection) {
      if (this.instance.datastore === "redis") {
        this.connection = new RedisConnection({
          Redis: this.Redis,
          clientOptions: this.clientOptions,
          Promise: Promise,
          Events: this.instance.Events,
        });
      } else if (this.instance.datastore === "ioredis") {
        this.connection = new IORedisConnection({
          Redis: this.Redis,
          clientOptions: this.clientOptions,
          clusterNodes: this.clusterNodes,
          Promise: Promise,
          Events: this.instance.Events,
        });
      }
    }

    this.instance.connection = this.connection;
    this.instance.datastore = this.connection.datastore;

    this.ready = this._initReady();
    // Stored init promise: consumers await `ready` lazily, so suppress the
    // unhandled-rejection that would fire before anyone attaches a handler.
    this.ready.catch(() => {});
  }

  async _initReady() {
    this.clients = await this.connection.ready;
    await this.runScript("init", this.prepareInitSettings(this.clearDatastore));
    await this.connection.__addLimiter__(this.instance);
    await this.runScript("register_client", [this.instance.queued()]);
    if (!this._disconnecting) {
      this.heartbeat = setInterval(async () => {
        try {
          await this.runScript("heartbeat", []);
        } catch (e) {
          if (!this._disconnecting) {
            this.instance.Events.trigger("error", e);
          }
        }
      }, this.heartbeatInterval).unref?.();
    }
    return this.clients;
  }

  async __publish__(message) {
    const { client } = await this.ready;
    return client.publish(this.instance.channel(), `message:${message.toString()}`);
  }

  async onMessage(channel, message) {
    try {
      const pos = message.indexOf(":");
      const [type, data] = [message.slice(0, pos), message.slice(pos + 1)];
      if (type === "capacity") {
        return await this.instance._drainAll(data.length > 0 ? ~~data : undefined);
      } else if (type === "capacity-priority") {
        const [rawCapacity, priorityClient, counter] = data.split(":");
        const capacity = rawCapacity.length > 0 ? ~~rawCapacity : undefined;
        if (priorityClient === this.clientId) {
          this.instance.Events.trigger("capacity-priority", capacity);
          const drained = await this.instance._drainAll(capacity);
          const newCapacity = capacity != null ? capacity - (drained || 0) : "";
          return await this.clients.client.publish(
            this.instance.channel(),
            `capacity-priority:${newCapacity}::${counter}`,
          );
        } else if (priorityClient === "") {
          clearTimeout(this.capacityPriorityCounters[counter]);
          delete this.capacityPriorityCounters[counter];
          return this.instance._drainAll(capacity);
        } else {
          return (this.capacityPriorityCounters[counter] = setTimeout(async () => {
            try {
              delete this.capacityPriorityCounters[counter];
              await this.runScript("blacklist_client", [priorityClient]);
              return await this.instance._drainAll(capacity);
            } catch (e) {
              if (!this._disconnecting) {
                return this.instance.Events.trigger("error", e);
              }
            }
          }, 1000));
        }
      } else if (type === "message") {
        return this.instance.Events.trigger("message", data);
      } else if (type === "blocked") {
        return await this.instance._dropAllQueued();
      }
    } catch (error) {
      const e = error;
      if (!this._disconnecting) {
        return this.instance.Events.trigger("error", e);
      }
    }
  }

  async __disconnect__(flush) {
    this._disconnecting = true;
    clearInterval(this.heartbeat);
    if (this.sharedConnection) {
      await this.connection.__removeLimiter__(this.instance);
    } else {
      return this.connection.disconnect(flush);
    }
  }

  async runScript(name, args) {
    if (name !== "init" && name !== "register_client") {
      await this.ready;
    }
    const all_args = [Date.now(), this.clientId].concat(args);
    this.instance.Events.trigger("debug", `Calling Redis script: ${name}.lua`, all_args);
    try {
      return await this.connection.__runScript__(name, this.originalId, all_args);
    } catch (e) {
      if (
        typeof e.message === "string" &&
        e.message.match(/^(.*\s)?SETTINGS_KEY_NOT_FOUND$/) !== null
      ) {
        if (name === "heartbeat") {
          return undefined;
        }
        await this.runScript("init", this.prepareInitSettings(false));
        return this.runScript(name, args);
      } else if (
        typeof e.message === "string" &&
        e.message.match(/^(.*\s)?UNKNOWN_CLIENT$/) !== null
      ) {
        await this.runScript("register_client", [this.instance.queued()]);
        return this.runScript(name, args);
      } else {
        throw e;
      }
    }
  }

  prepareArray(arr) {
    return arr.map((x) => (x != null ? x.toString() : ""));
  }

  prepareObject(obj) {
    const arr = [];
    for (const [k, v] of Object.entries(obj)) {
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

  async __updateSettings__(options) {
    await this.runScript("update_settings", this.prepareObject(options));
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

  async __groupCheck__() {
    return this.convertBool(await this.runScript("group_check", []));
  }

  __incrementReservoir__(incr) {
    return this.runScript("increment_reservoir", [incr]);
  }

  __currentReservoir__() {
    return this.runScript("current_reservoir", []);
  }

  async __check__(weight) {
    return this.convertBool(await this.runScript("check", this.prepareArray([weight])));
  }

  async __register__(index, weight, expiration) {
    const [success, wait, reservoir] = await this.runScript(
      "register",
      this.prepareArray([index, weight, expiration]),
    );

    return {
      success: this.convertBool(success),
      wait,
      reservoir,
    };
  }

  async __submit__(queueLength, weight) {
    try {
      const [reachedHWM, blocked, strategy] = Array.from(
        await this.runScript("submit", this.prepareArray([queueLength, weight])),
      );
      return {
        reachedHWM: this.convertBool(reachedHWM),
        blocked: this.convertBool(blocked),
        strategy,
      };
    } catch (e) {
      if (/^(ERR )?OVERWEIGHT/.test(e.message)) {
        let maxConcurrent;
        [, weight, maxConcurrent] = e.message.split(":");
        throw new BottleneckError(
          `Impossible to add a job having a weight of ${weight} to a limiter having a maxConcurrent setting of ${maxConcurrent}`,
        );
      } else {
        throw e;
      }
    }
  }

  async __free__(index, _weight) {
    const running = await this.runScript("free", this.prepareArray([index]));
    return { running };
  }
}

module.exports = RedisDatastore;
