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
 * DS103: Rewrite code to no longer use __guard__, or convert again using --optional-chaining
 * DS201: Simplify complex destructure assignments
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS206: Consider reworking classes to avoid initClass
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/main/docs/suggestions.md
 */
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

class Bottleneck {
  static initClass() {
    Bottleneck.default = Bottleneck;
    Bottleneck.Events = Events;
    Bottleneck.version = (Bottleneck.prototype.version = require("./version.json").version);
    Bottleneck.strategy = (Bottleneck.prototype.strategy = { LEAK:1, OVERFLOW:2, OVERFLOW_PRIORITY:4, BLOCK:3 });
    Bottleneck.BottleneckError = (Bottleneck.prototype.BottleneckError = require("./BottleneckError"));
    Bottleneck.Group = (Bottleneck.prototype.Group = require("./Group"));
    Bottleneck.RedisConnection = (Bottleneck.prototype.RedisConnection = require("./RedisConnection"));
    Bottleneck.IORedisConnection = (Bottleneck.prototype.IORedisConnection = require("./IORedisConnection"));
    Bottleneck.Batcher = (Bottleneck.prototype.Batcher = require("./Batcher"));
    this.prototype.jobDefaults = {
      priority: DEFAULT_PRIORITY,
      weight: 1,
      expiration: null,
      id: "<no-id>"
    };
    this.prototype.storeDefaults = {
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
      reservoirIncreaseMaximum: null
    };
    this.prototype.localStoreDefaults = {
      Promise,
      timeout: null,
      heartbeatInterval: 250
    };
    this.prototype.redisStoreDefaults = {
      Promise,
      timeout: null,
      heartbeatInterval: 5000,
      clientTimeout: 10000,
      Redis: null,
      clientOptions: {},
      clusterNodes: null,
      clearDatastore: false,
      connection: null
    };
    this.prototype.instanceDefaults = {
      datastore: "local",
      connection: null,
      id: "<no-id>",
      rejectOnDrop: true,
      trackDoneStatus: false,
      Promise
    };
    this.prototype.stopDefaults = {
      enqueueErrorMessage: "This limiter has been stopped and cannot accept new jobs.",
      dropWaitingJobs: true,
      dropErrorMessage: "This limiter has been stopped."
    };
  }

  constructor(options, ...invalid) {
    this._addToQueue = this._addToQueue.bind(this);
    if (options == null) { options = {}; }
    this._validateOptions(options, invalid);
    parser.load(options, this.instanceDefaults, this);
    this._queues = new Queues(NUM_PRIORITIES);
    this._scheduled = {};
    this._states = new States(["RECEIVED", "QUEUED", "RUNNING", "EXECUTING"].concat(this.trackDoneStatus ? ["DONE"] : []));
    this._limiter = null;
    this.Events = new Events(this);
    this._submitLock = new Sync("submit", this.Promise);
    this._registerLock = new Sync("register", this.Promise);
    const storeOptions = parser.load(options, this.storeDefaults, {});

    this._store = (() => {
      let storeInstanceOptions;
      if ((this.datastore === "redis") || (this.datastore === "ioredis") || (this.connection != null)) {
      storeInstanceOptions = parser.load(options, this.redisStoreDefaults, {});
      return new RedisDatastore(this, storeOptions, storeInstanceOptions);
    } else if (this.datastore === "local") {
      storeInstanceOptions = parser.load(options, this.localStoreDefaults, {});
      return new LocalDatastore(this, storeOptions, storeInstanceOptions);
    } else {
      throw new Bottleneck.prototype.BottleneckError(`Invalid datastore type: ${this.datastore}`);
    }
    })();

    this._queues.on("leftzero", () => __guardMethod__(this._store.heartbeat, 'ref', o => o.ref()));
    this._queues.on("zero", () => __guardMethod__(this._store.heartbeat, 'unref', o => o.unref()));
  }

  _validateOptions(options, invalid) {
    if ((options == null) || (typeof options !== "object") || (invalid.length !== 0)) {
      throw new Bottleneck.prototype.BottleneckError("Bottleneck v2 takes a single object argument. Refer to https://github.com/SGrondin/bottleneck#upgrading-to-v2 if you're upgrading from Bottleneck v1.");
    }
  }

  ready() { return this._store.ready; }

  clients() { return this._store.clients; }

  channel() { return `b_${this.id}`; }

  channel_client() { return `b_${this.id}_${this._store.clientId}`; }

  publish(message) { return this._store.__publish__(message); }

  disconnect(flush) { if (flush == null) { flush = true; } return this._store.__disconnect__(flush); }

  chain(_limiter) { this._limiter = _limiter; return this; }

  queued(priority) { return this._queues.queued(priority); }

  clusterQueued() { return this._store.__queued__(); }

  empty() { return (this.queued() === 0) && this._submitLock.isEmpty(); }

  running() { return this._store.__running__(); }

  done() { return this._store.__done__(); }

  jobStatus(id) { return this._states.jobStatus(id); }

  jobs(status) { return this._states.statusJobs(status); }

  counts() { return this._states.statusCounts(); }

  _randomIndex() { return Math.random().toString(36).slice(2); }

  check(weight) { if (weight == null) { weight = 1; } return this._store.__check__(weight); }

  _clearGlobalState(index) {
    if (this._scheduled[index] != null) {
      clearTimeout(this._scheduled[index].expiration);
      delete this._scheduled[index];
      return true;
    } else { return false; }
  }

  _free(index, job, options, eventInfo) {
    try {
      const { running } = await(this._store.__free__(index, options.weight));
      this.Events.trigger("debug", `Freed ${options.id}`, eventInfo);
      if ((running === 0) && this.empty()) { return this.Events.trigger("idle"); }
    } catch (e) {
      return this.Events.trigger("error", e);
    }
  }

  _run(index, job, wait) {
    job.doRun();
    const clearGlobalState = this._clearGlobalState.bind(this, index);
    const run = this._run.bind(this, index, job);
    const free = this._free.bind(this, index, job);

    return this._scheduled[index] = {
      timeout: setTimeout(() => {
        return job.doExecute(this._limiter, clearGlobalState, run, free);
      }
      , wait),
      expiration: (job.options.expiration != null) ? setTimeout(() => job.doExpire(clearGlobalState, run, free)
      , wait + job.options.expiration) : undefined,
      job
    };
  }

  _drainOne(capacity) {
    return this._registerLock.schedule(() => {
      let next;
      if (this.queued() === 0) { return this.Promise.resolve(null); }
      const queue = this._queues.getFirst();
      const { options, args } = (next = queue.first());
      if ((capacity != null) && (options.weight > capacity)) { return this.Promise.resolve(null); }
      this.Events.trigger("debug", `Draining ${options.id}`, { args, options });
      const index = this._randomIndex();
      return this._store.__register__(index, options.weight, options.expiration)
      .then(({ success, wait, reservoir }) => {
        this.Events.trigger("debug", `Drained ${options.id}`, { success, args, options });
        if (success) {
          queue.shift();
          const empty = this.empty();
          if (empty) { this.Events.trigger("empty"); }
          if (reservoir === 0) { this.Events.trigger("depleted", empty); }
          this._run(index, next, wait);
          return this.Promise.resolve(options.weight);
        } else {
          return this.Promise.resolve(null);
        }
      });
    });
  }

  _drainAll(capacity, total) {
    if (total == null) { total = 0; }
    return this._drainOne(capacity)
    .then(drained => {
      if (drained != null) {
        const newCapacity = (capacity != null) ? capacity - drained : capacity;
        return this._drainAll(newCapacity, total + drained);
      } else { return this.Promise.resolve(total); }
  }).catch(e => this.Events.trigger("error", e));
  }

  _dropAllQueued(message) { return this._queues.shiftAll(job => job.doDrop({ message })); }

  stop(options) {
    if (options == null) { options = {}; }
    options = parser.load(options, this.stopDefaults);
    const waitForExecuting = at => {
      const finished = () => {
        const {
          counts
        } = this._states;
        return (counts[0] + counts[1] + counts[2] + counts[3]) === at;
      };
      return new this.Promise((resolve, reject) => {
        if (finished()) { return resolve();
        } else {
          return this.on("done", () => {
            if (finished()) {
              this.removeAllListeners("done");
              return resolve();
            }
          });
        }
      });
    };
    const done = (() => {
      if (options.dropWaitingJobs) {
      this._run = (index, next) => next.doDrop({ message: options.dropErrorMessage });
      this._drainOne = () => this.Promise.resolve(null);
      return this._registerLock.schedule(() => this._submitLock.schedule(() => {
        for (var k in this._scheduled) {
          var v = this._scheduled[k];
          if (this.jobStatus(v.job.options.id) === "RUNNING") {
            clearTimeout(v.timeout);
            clearTimeout(v.expiration);
            v.job.doDrop({ message: options.dropErrorMessage });
          }
        }
        this._dropAllQueued(options.dropErrorMessage);
        return waitForExecuting(0);
      })
      );
    } else {
      return this.schedule({ priority: NUM_PRIORITIES - 1, weight: 0 }, () => waitForExecuting(1));
    }
    })();
    this._receive = job => job._reject(new Bottleneck.prototype.BottleneckError(options.enqueueErrorMessage));
    this.stop = () => this.Promise.reject(new Bottleneck.prototype.BottleneckError("stop() has already been called"));
    return done;
  }

  _addToQueue(job) {
    let blocked, reachedHWM, strategy;
    const { args, options } = job;
    try {
      ({ reachedHWM, blocked, strategy } = await(this._store.__submit__(this.queued(), options.weight)));
    } catch (error) {
      this.Events.trigger("debug", `Could not queue ${options.id}`, { args, options, error });
      job.doDrop({ error });
      return false;
    }

    if (blocked) {
      job.doDrop();
      return true;
    } else if (reachedHWM) {
      const shifted = (() => {
        if (strategy === Bottleneck.prototype.strategy.LEAK) { return this._queues.shiftLastFrom(options.priority);
      } else if (strategy === Bottleneck.prototype.strategy.OVERFLOW_PRIORITY) { return this._queues.shiftLastFrom(options.priority + 1);
      } else if (strategy === Bottleneck.prototype.strategy.OVERFLOW) { return job; }
      })();
      if (shifted != null) { shifted.doDrop(); }
      if ((shifted == null) || (strategy === Bottleneck.prototype.strategy.OVERFLOW)) {
        if ((shifted == null)) { job.doDrop(); }
        return reachedHWM;
      }
    }

    job.doQueue(reachedHWM, blocked);
    this._queues.push(job);
    await(this._drainAll());
    return reachedHWM;
  }

  _receive(job) {
    if (this._states.jobStatus(job.options.id) != null) {
      job._reject(new Bottleneck.prototype.BottleneckError(`A job with the same id already exists (id=${job.options.id})`));
      return false;
    } else {
      job.doReceive();
      return this._submitLock.schedule(this._addToQueue, job);
    }
  }

  submit(...args) {
    let cb, fn, options;
    if (typeof args[0] === "function") {
      let adjustedLength;
      fn = args[0],
        adjustedLength = Math.max(args.length, 2),
        args = args.slice(1, adjustedLength - 1),
        cb = args[adjustedLength - 1];
      options = parser.load({}, this.jobDefaults);
    } else {
      let adjustedLength1;
      options = args[0],
        fn = args[1],
        adjustedLength1 = Math.max(args.length, 3),
        args = args.slice(2, adjustedLength1 - 1),
        cb = args[adjustedLength1 - 1];
      options = parser.load(options, this.jobDefaults);
    }

    const task = (...args) => {
      return new this.Promise((resolve, reject) => fn(...Array.from(args), (...args) => ((args[0] != null) ? reject : resolve)(args)));
    };

    const job = new Job(task, args, options, this.jobDefaults, this.rejectOnDrop, this.Events, this._states, this.Promise);
    job.promise
    .then(args => typeof cb === 'function' ? cb(...Array.from(args || [])) : undefined)
    .catch(function(args) { if (Array.isArray(args)) { return (typeof cb === 'function' ? cb(...Array.from(args || [])) : undefined); } else { return (typeof cb === 'function' ? cb(args) : undefined); } });
    return this._receive(job);
  }

  schedule(...args) {
    let options, task;
    if (typeof args[0] === "function") {
      [task, ...args] = Array.from(args);
      options = {};
    } else {
      [options, task, ...args] = Array.from(args);
    }
    const job = new Job(task, args, options, this.jobDefaults, this.rejectOnDrop, this.Events, this._states, this.Promise);
    this._receive(job);
    return job.promise;
  }

  wrap(fn) {
    const schedule = this.schedule.bind(this);
    const wrapped = function(...args) { return schedule(fn.bind(this), ...Array.from(args)); };
    wrapped.withOptions = (options, ...args) => schedule(options, fn, ...Array.from(args));
    return wrapped;
  }

  updateSettings(options) {
    if (options == null) { options = {}; }
    await(this._store.__updateSettings__(parser.overwrite(options, this.storeDefaults)));
    parser.overwrite(options, this.instanceDefaults, this);
    return this;
  }

  currentReservoir() { return this._store.__currentReservoir__(); }

  incrementReservoir(incr) { if (incr == null) { incr = 0; } return this._store.__incrementReservoir__(incr); }
}
Bottleneck.initClass();

module.exports = Bottleneck;

function __guardMethod__(obj, methodName, transform) {
  if (typeof obj !== 'undefined' && obj !== null && typeof obj[methodName] === 'function') {
    return transform(obj, methodName);
  } else {
    return undefined;
  }
}