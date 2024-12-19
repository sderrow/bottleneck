const NUM_PRIORITIES = 10;
const DEFAULT_PRIORITY = 5;

const parser = require("./parser");
const Queues = require("./Queues");
const Job = require("./Job");
const LocalDatastore = require("./LocalDatastore");
const RedisDatastore = require("./RedisDatastore");
const Events = require("./Events");
const States = require("./States");
const Sync = require("./Sync");
const BottleneckError = require("./BottleneckError");
const Group = require("./Group");
const RedisConnection = require("./RedisConnection");
const IORedisConnection = require("./IORedisConnection");
const Batcher = require("./Batcher");
const versionJson = require("./version.json");
class Bottleneck {
  static BottleneckError = BottleneckError;
  static Group = Group;
  static RedisConnection = RedisConnection;
  static IORedisConnection = IORedisConnection;
  static Batcher = Batcher;
  static Events = Events;
  static strategy = {
    LEAK: 1,
    OVERFLOW: 2,
    OVERFLOW_PRIORITY: 4,
    BLOCK: 3,
  };

  version = versionJson.version;
  jobDefaults = {
    priority: DEFAULT_PRIORITY,
    weight: 1,
    expiration: null,
    id: "<no-id>",
  };
  storeDefaults = {
    maxConcurrent: null,
    minTime: 0,
    highWater: null,
    strategy: Bottleneck.prototype.strategy.LEAK,
    penalty: null,
    reservoir: null,
    reservoirRefreshInterval: null,
    reservoirRefreshAmount: null,
    reservoirIncreaseInterval: null,
    reservoirIncreaseAmount: null,
    reservoirIncreaseMaximum: null,
  };
  localStoreDefaults = {
    Promise,
    timeout: null,
    heartbeatInterval: 250,
  };
  redisStoreDefaults = {
    Promise,
    timeout: null,
    heartbeatInterval: 5000,
    clientTimeout: 10000,
    Redis: null,
    clientOptions: {},
    clusterNodes: null,
    clearDatastore: false,
    connection: null,
  };
  instanceDefaults = {
    datastore: "local",
    connection: null,
    id: "<no-id>",
    rejectOnDrop: true,
    trackDoneStatus: false,
    Promise,
  };
  stopDefaults = {
    enqueueErrorMessage: "This limiter has been stopped and cannot accept new jobs.",
    dropWaitingJobs: true,
    dropErrorMessage: "This limiter has been stopped.",
  };

  constructor(options, ...invalid) {
    options ??= {};
    this._validateOptions(options, invalid);
    parser.load(options, this.instanceDefaults, this);
    this._queues = new Queues(NUM_PRIORITIES);
    this._scheduled = {};
    this._states = new States(
      ["RECEIVED", "QUEUED", "RUNNING", "EXECUTING"].concat(this.trackDoneStatus ? ["DONE"] : []),
    );
    this._limiter = null;
    this.Events = new Events(this);
    this._submitLock = new Sync("submit");
    this._registerLock = new Sync("register");
    const storeOptions = parser.load(options, this.storeDefaults, {});

    if (this.datastore === "redis" || this.datastore === "ioredis" || this.connection != null) {
      const opts = parser.load(options, this.redisStoreDefaults, {});
      this._store = new RedisDatastore(this, storeOptions, opts);
    } else if (this.datastore === "local") {
      const opts = parser.load(options, this.localStoreDefaults, {});
      this._store = new LocalDatastore(this, storeOptions, opts);
    } else {
      throw new BottleneckError(`Invalid datastore type: ${this.datastore}`);
    }

    this._queues.on("leftzero", () => this._store.heartbeat?.ref?.());
    this._queues.on("zero", () => this._store.heartbeat?.unref?.());
  }

  _validateOptions(options, invalid) {
    if (options == null || typeof options !== "object" || invalid.length !== 0) {
      throw new BottleneckError(
        "Bottleneck v2 takes a single object argument. Refer to https://github.com/SGrondin/bottleneck#upgrading-to-v2 if you're upgrading from Bottleneck v1.",
      );
    }
  }

  ready() {
    return this._store.ready;
  }

  clients() {
    return this._store.clients;
  }

  channel() {
    return `b_${this.id}`;
  }

  channel_client() {
    return `b_${this.id}_${this._store.clientId}`;
  }

  publish(message) {
    return this._store.__publish__(message);
  }

  async disconnect(flush = true) {
    await this._store.__disconnect__(flush);
  }

  chain(_limiter) {
    this._limiter = _limiter;
    return this;
  }

  queued(priority) {
    return this._queues.queued(priority);
  }

  clusterQueued() {
    return this._store.__queued__();
  }

  empty() {
    return this.queued() === 0 && this._submitLock.isEmpty();
  }

  running() {
    return this._store.__running__();
  }

  done() {
    return this._store.__done__();
  }

  jobStatus(id) {
    return this._states.jobStatus(id);
  }

  jobs(status) {
    return this._states.statusJobs(status);
  }

  counts() {
    return this._states.statusCounts();
  }

  _randomIndex() {
    return Math.random().toString(36).slice(2);
  }

  check(weight = 1) {
    return this._store.__check__(weight);
  }

  _clearGlobalState(index) {
    if (this._scheduled[index] != null) {
      clearTimeout(this._scheduled[index].expiration);
      delete this._scheduled[index];
      return true;
    } else {
      return false;
    }
  }

  async _free(index, job, options, eventInfo) {
    try {
      const { running } = await this._store.__free__(index, options.weight);
      this.Events.trigger("debug", `Freed ${options.id}`, eventInfo);
      if (running === 0 && this.empty()) {
        return this.Events.trigger("idle");
      }
    } catch (e) {
      return this.Events.trigger("error", e);
    }
  }

  _run(index, job, wait) {
    job.doRun();
    const clearGlobalState = this._clearGlobalState.bind(this, index);
    const run = this._run.bind(this, index, job);
    const free = this._free.bind(this, index, job);

    return (this._scheduled[index] = {
      timeout: setTimeout(() => {
        return job.doExecute(this._limiter, clearGlobalState, run, free);
      }, wait),
      expiration:
        job.options.expiration != null
          ? setTimeout(
              () => job.doExpire(clearGlobalState, run, free),
              wait + job.options.expiration,
            )
          : undefined,
      job,
    });
  }

  async _drainOne(capacity) {
    return this._registerLock.schedule(async () => {
      let next;
      if (this.queued() === 0) {
        return null;
      }
      const queue = this._queues.getFirst();
      const { options, args } = (next = queue.first());
      if (capacity != null && options.weight > capacity) {
        return null;
      }
      this.Events.trigger("debug", `Draining ${options.id}`, { args, options });
      const index = this._randomIndex();

      const { success, wait, reservoir } = await this._store.__register__(
        index,
        options.weight,
        options.expiration,
      );

      this.Events.trigger("debug", `Drained ${options.id}`, { success, args, options });

      if (success) {
        queue.shift();
        const empty = this.empty();
        if (empty) {
          this.Events.trigger("empty");
        }
        if (reservoir === 0) {
          this.Events.trigger("depleted", empty);
        }
        this._run(index, next, wait);
        return options.weight;
      } else {
        return null;
      }
    });
  }

  async _drainAll(capacity, total = 0) {
    try {
      const drained = await this._drainOne(capacity);
      if (drained != null) {
        const newCapacity = capacity != null ? capacity - drained : capacity;
        return this._drainAll(newCapacity, total + drained);
      } else {
        return total;
      }
    } catch (e) {
      this.Events.trigger("error", e);
    }
  }

  _dropAllQueued(message) {
    return this._queues.shiftAll((job) => job.doDrop({ message }));
  }

  async stop(options) {
    options ??= {};
    options = parser.load(options, this.stopDefaults);

    const waitForExecuting = (at) => {
      const finished = () => {
        const { counts } = this._states;
        return counts[0] + counts[1] + counts[2] + counts[3] === at;
      };
      return new Promise((resolve) => {
        if (finished()) {
          resolve();
        } else {
          this.on("done", () => {
            if (finished()) {
              this.removeAllListeners("done");
              resolve();
            }
          });
        }
      });
    };

    this._receive = (job) => job._reject(new BottleneckError(options.enqueueErrorMessage));
    this.stop = () => Promise.reject(new BottleneckError("stop() has already been called"));

    if (options.dropWaitingJobs) {
      this._run = (index, next) => next.doDrop({ message: options.dropErrorMessage });
      this._drainOne = () => Promise.resolve(null);
      await this._registerLock.schedule(() =>
        this._submitLock.schedule(async () => {
          for (const v of Object.values(this._scheduled)) {
            if (this.jobStatus(v.job.options.id) === "RUNNING") {
              clearTimeout(v.timeout);
              clearTimeout(v.expiration);
              v.job.doDrop({ message: options.dropErrorMessage });
            }
          }
          this._dropAllQueued(options.dropErrorMessage);
          await waitForExecuting(0);
        }),
      );
    } else {
      await this.schedule({ priority: NUM_PRIORITIES - 1, weight: 0 }, () => waitForExecuting(1));
    }
  }

  async _addToQueue(job) {
    let blocked, reachedHWM, strategy;
    const { args, options } = job;
    try {
      ({ reachedHWM, blocked, strategy } = await this._store.__submit__(
        this.queued(),
        options.weight,
      ));
    } catch (error) {
      this.Events.trigger("debug", `Could not queue ${options.id}`, { args, options, error });
      job.doDrop({ error });
      return false;
    }

    if (blocked) {
      job.doDrop();
      return true;
    } else if (reachedHWM) {
      let shifted;
      if (strategy === Bottleneck.strategy.LEAK) {
        shifted = this._queues.shiftLastFrom(options.priority);
      } else if (strategy === Bottleneck.strategy.OVERFLOW_PRIORITY) {
        shifted = this._queues.shiftLastFrom(options.priority + 1);
      } else if (strategy === Bottleneck.strategy.OVERFLOW) {
        shifted = job;
      }
      if (shifted != null) {
        shifted.doDrop();
      }
      if (shifted == null || strategy === Bottleneck.strategy.OVERFLOW) {
        if (shifted == null) {
          job.doDrop();
        }
        return reachedHWM;
      }
    }

    job.doQueue(reachedHWM, blocked);
    this._queues.push(job);
    await this._drainAll();
    return reachedHWM;
  }

  _receive(job) {
    if (this._states.jobStatus(job.options.id) != null) {
      job._reject(
        new BottleneckError(`A job with the same id already exists (id=${job.options.id})`),
      );
      return false;
    } else {
      job.doReceive();
      return this._submitLock.schedule(this._addToQueue, job);
    }
  }

  submit(...args) {
    let cb, fn, options;
    if (typeof args[0] === "function") {
      cb = args.pop();
      [fn, ...args] = args;
      options = parser.load({}, this.jobDefaults);
    } else {
      cb = args.pop();
      [options, fn, ...args] = args;
      options = parser.load(options, this.jobDefaults);
    }

    const task = (...args) => {
      return new Promise((resolve, reject) =>
        fn(...args, (...args) => (args[0] != null ? reject : resolve)(args)),
      );
    };

    const job = new Job(
      task,
      args,
      options,
      this.jobDefaults,
      this.rejectOnDrop,
      this.Events,
      this._states,
    );
    job.promise
      .then((args) => (typeof cb === "function" ? cb(...(args || [])) : undefined))
      .catch(function (args) {
        if (Array.isArray(args)) {
          return typeof cb === "function" ? cb(...args) : undefined;
        } else {
          return typeof cb === "function" ? cb(args) : undefined;
        }
      });
    return this._receive(job);
  }

  schedule(...args) {
    let options, task;
    if (typeof args[0] === "function") {
      [task, ...args] = args;
      options = {};
    } else {
      [options, task, ...args] = args;
    }
    const job = new Job(
      task,
      args,
      options,
      this.jobDefaults,
      this.rejectOnDrop,
      this.Events,
      this._states,
    );
    this._receive(job);
    return job.promise;
  }

  wrap(fn) {
    const schedule = this.schedule.bind(this);
    const wrapped = function (...args) {
      return schedule(fn.bind(this), ...args);
    };
    wrapped.withOptions = (options, ...args) => schedule(options, fn, ...args);
    return wrapped;
  }

  async updateSettings(options) {
    options ??= {};
    await this._store.__updateSettings__(parser.overwrite(options, this.storeDefaults));
    parser.overwrite(options, this.instanceDefaults, this);
    return this;
  }

  currentReservoir() {
    return this._store.__currentReservoir__();
  }

  incrementReservoir(incr = 0) {
    return this._store.__incrementReservoir__(incr);
  }
}

module.exports = Bottleneck;
module.exports.default = Bottleneck;
