import type {
  BottleneckEvents,
  ClientsList,
  ConstructorOptions,
  Counts,
  EventInfo,
  JobDefaults,
  JobOptions,
  ResolvedJobOptions,
  Status,
  StopOptions,
  StoreOptions,
  StrategyConstants,
} from "./types";
import pkg from "../package.json" with { type: "json" };
import Batcher from "./Batcher";
import BottleneckError from "./BottleneckError";
import IORedisConnection from "./cluster/IORedisConnection";
import RedisConnection from "./cluster/RedisConnection";
import RedisDatastore from "./cluster/RedisDatastore";
import Events from "./Events";
import Group from "./Group";
import Job from "./Job";
import LocalDatastore from "./LocalDatastore";
import { load, overwrite } from "./parser";
import Queues from "./Queues";
import randomIndex from "./random-index";
import States from "./States";
import Sync from "./Sync";

const NUM_PRIORITIES = 10;
const DEFAULT_PRIORITY = 5;

const version = (pkg as { version: string }).version;

// Group <-> Bottleneck ESM cycle: assignment target for `static set Group`.
// Read through the getter; never referenced during module evaluation.
let groupOverride: typeof Group | undefined;

type ScheduledJob = {
  timeout: ReturnType<typeof setTimeout>;
  expiration: ReturnType<typeof setTimeout> | undefined;
  job: Job;
};

class Bottleneck {
  static BottleneckError = BottleneckError;
  // Lazy accessors: Group <-> Bottleneck form an ESM module cycle (Group
  // instantiates Bottleneck at runtime). A static field initializer would
  // evaluate during the cycle and hit the TDZ when Group.mts is imported
  // first; accessors defer resolution until after both modules initialize.
  static get Group() {
    return groupOverride ?? Group;
  }
  static set Group(value) {
    groupOverride = value;
  }
  static RedisConnection = RedisConnection;
  static IORedisConnection = IORedisConnection;
  static Batcher = Batcher;
  static Events = Events;
  static readonly strategy: StrategyConstants = {
    LEAK: 1,
    OVERFLOW: 2,
    OVERFLOW_PRIORITY: 4,
    BLOCK: 3,
  };

  version = version;
  jobDefaults: JobDefaults = {
    priority: DEFAULT_PRIORITY,
    weight: 1,
    expiration: null,
    id: "<no-id>",
  };
  storeDefaults: StoreOptions = {
    maxConcurrent: null,
    minTime: 0,
    highWater: null,
    strategy: Bottleneck.strategy.LEAK,
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
    timeout: null as number | null,
    heartbeatInterval: 250,
  };
  redisStoreDefaults = {
    Promise,
    timeout: null as number | null,
    heartbeatInterval: 5000,
    clientTimeout: 10000,
    Redis: null,
    clientOptions: {} as object,
    clusterNodes: null as unknown,
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

  // Populated from instanceDefaults via parser.load in the constructor.
  datastore: string = "local";
  connection: RedisConnection | IORedisConnection | null = null;
  id: string = "<no-id>";
  rejectOnDrop: boolean = true;
  trackDoneStatus: boolean = false;
  Promise: PromiseConstructor = Promise;

  _addToQueue: (job: Job) => Promise<boolean>;
  _queues: Queues;
  _scheduled: Record<string, ScheduledJob> = {};
  _states: States;
  _limiter: Bottleneck | null = null;
  Events: Events;
  _submitLock: Sync;
  _registerLock: Sync;
  _store: LocalDatastore | RedisDatastore;

  // Installed on the instance by Events (see Events constructor); declared so
  // this class typechecks against its own runtime behavior. The listener map
  // is the public event contract.
  declare on: {
    <E extends keyof BottleneckEvents>(event: E, listener: BottleneckEvents[E]): unknown;
    (event: string, listener: (...args: any[]) => unknown): unknown;
  };
  declare once: {
    <E extends keyof BottleneckEvents>(event: E, listener: BottleneckEvents[E]): unknown;
    (event: string, listener: (...args: any[]) => unknown): unknown;
  };
  declare removeAllListeners: (name?: string | null) => void;

  constructor(options?: ConstructorOptions, ...invalid: unknown[]) {
    this._addToQueue = this._addToQueueImpl.bind(this);
    options ??= {};
    this._validateOptions(options, invalid);
    load(options, this.instanceDefaults, this);
    this._queues = new Queues(NUM_PRIORITIES);
    this._scheduled = {};
    this._states = new States(
      ["RECEIVED", "QUEUED", "RUNNING", "EXECUTING"].concat(this.trackDoneStatus ? ["DONE"] : []),
    );
    this._limiter = null;
    this.Events = new Events(this);
    this._submitLock = new Sync("submit");
    this._registerLock = new Sync("register");
    const storeOptions = load(options, this.storeDefaults, {});

    if (this.datastore === "redis" || this.datastore === "ioredis" || this.connection != null) {
      const opts = load(options, this.redisStoreDefaults, {});
      this._store = new RedisDatastore(this, storeOptions, opts);
    } else if (this.datastore === "local") {
      const opts = load(options, this.localStoreDefaults, {});
      this._store = new LocalDatastore(this, storeOptions, opts);
    } else {
      throw new BottleneckError(`Invalid datastore type: ${this.datastore}`);
    }

    this._queues.on("leftzero", () => this._store.heartbeat?.ref?.());
    this._queues.on("zero", () => this._store.heartbeat?.unref?.());
  }

  _validateOptions(options: object | null | undefined, invalid: unknown[]): void {
    if (options == null || typeof options !== "object" || invalid.length !== 0) {
      throw new BottleneckError(
        "Bottleneck v2 takes a single object argument. Refer to https://github.com/SGrondin/bottleneck#upgrading-to-v2 if you're upgrading from Bottleneck v1.",
      );
    }
  }

  ready(): Promise<unknown> {
    return this._store.ready;
  }

  clients(): ClientsList {
    return this._store.clients as ClientsList;
  }

  channel(): string {
    return `b_${this.id}`;
  }

  channel_client(): string {
    return `b_${this.id}_${this._store.clientId}`;
  }

  publish(message: string): Promise<unknown> {
    return this._store.__publish__(message);
  }

  async disconnect(flush = true): Promise<void> {
    await this._store.__disconnect__(flush);
  }

  chain(limiter?: Bottleneck): this {
    this._limiter = limiter ?? null;
    return this;
  }

  queued(priority?: number): number {
    return this._queues.queued(priority);
  }

  clusterQueued(): Promise<number> {
    return this._store.__queued__() as Promise<number>;
  }

  empty(): boolean {
    return this.queued() === 0 && this._submitLock.isEmpty();
  }

  running(): Promise<number> {
    return this._store.__running__() as Promise<number>;
  }

  done(): Promise<number> {
    return this._store.__done__() as Promise<number>;
  }

  jobStatus(id: string): Status | null {
    return this._states.jobStatus(id) as Status | null;
  }

  jobs(status?: Status): string[] {
    return this._states.statusJobs(status);
  }

  counts(): Counts {
    return this._states.statusCounts() as Counts;
  }

  _randomIndex(): string {
    return randomIndex();
  }

  check(weight = 1): Promise<boolean> {
    return this._store.__check__(weight);
  }

  _clearGlobalState(index: string): boolean {
    const scheduled = this._scheduled[index];
    if (scheduled != null) {
      clearTimeout(scheduled.expiration);
      delete this._scheduled[index];
      return true;
    } else {
      return false;
    }
  }

  async _free(
    index: string,
    _job: Job,
    options: ResolvedJobOptions,
    eventInfo: EventInfo,
  ): Promise<unknown> {
    try {
      const { running } = await this._store.__free__(index, options.weight);
      this.Events.trigger("debug", `Freed ${options.id}`, eventInfo);
      if (running === 0 && this.empty()) {
        return this.Events.trigger("idle");
      }
    } catch (e) {
      if (
        !this._store._disconnecting ||
        (e as Error)?.constructor?.name !== "DisconnectsClientError"
      ) {
        return this.Events.trigger("error", e);
      }
    }
  }

  _run(index: string, job: Job, wait: number): unknown {
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

  async _drainOne(capacity: number | null | undefined): Promise<number | null> {
    return this._registerLock.schedule(async (): Promise<number | null> => {
      if (this.queued() === 0) {
        return null;
      }
      const queue = this._queues.getFirst();
      const next = queue.first() as Job;
      const { options, args } = next;
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
        this._run(index, next, wait as number);
        return options.weight;
      } else {
        return null;
      }
    });
  }

  async _drainAll(capacity?: number | null, total = 0): Promise<number | undefined> {
    try {
      const drained = await this._drainOne(capacity);
      if (drained != null) {
        const newCapacity = capacity != null ? capacity - drained : undefined;
        return this._drainAll(newCapacity, total + drained);
      } else {
        return total;
      }
    } catch (e) {
      if (
        !this._store._disconnecting ||
        (e as Error)?.constructor?.name !== "DisconnectsClientError"
      ) {
        this.Events.trigger("error", e);
      }
    }
  }

  _dropAllQueued(message?: string): void {
    this._queues.shiftAll((job) => job.doDrop({ message }));
  }

  stop(options: StopOptions = {}): Promise<void> {
    options = load(options ?? {}, this.stopDefaults);

    const waitForExecuting = (at: number): Promise<void> => {
      const finished = (): boolean => {
        const { counts } = this._states;
        const total = counts[0]! + counts[1]! + counts[2]! + counts[3]!;
        return total === at;
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

    let done: Promise<unknown>;
    const opts = options as {
      dropWaitingJobs: boolean;
      dropErrorMessage: string;
      enqueueErrorMessage: string;
    };
    if (opts.dropWaitingJobs) {
      this._run = (_index: string, next: Job) => next.doDrop({ message: opts.dropErrorMessage });
      this._drainOne = (): Promise<null> => this.Promise.resolve(null);
      done = this._registerLock.schedule(() =>
        this._submitLock.schedule(() => {
          for (const v of Object.values(this._scheduled)) {
            if (this.jobStatus(v.job.options.id) === "RUNNING") {
              clearTimeout(v.timeout);
              clearTimeout(v.expiration);
              v.job.doDrop({ message: opts.dropErrorMessage });
            }
          }
          this._dropAllQueued(opts.dropErrorMessage);
          return waitForExecuting(0);
        }),
      );
    } else {
      done = this.schedule({ priority: NUM_PRIORITIES - 1, weight: 0 } as never, () =>
        waitForExecuting(1),
      );
    }

    this._receive = (job: Job) => job._reject(new BottleneckError(opts.enqueueErrorMessage));
    this.stop = (): Promise<never> =>
      this.Promise.reject(new BottleneckError("stop() has already been called"));

    return done as Promise<void>;
  }

  async _addToQueueImpl(job: Job): Promise<boolean> {
    let blocked: boolean, reachedHWM: boolean, strategy: unknown;
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
      let shifted: Job | undefined;
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

  _receive: (job: Job) => unknown = (job: Job): unknown => {
    if (this._states.jobStatus(job.options.id) != null) {
      job._reject(
        new BottleneckError(`A job with the same id already exists (id=${job.options.id})`),
      );
      return false;
    } else {
      job.doReceive();
      return this._submitLock.schedule(this._addToQueue, job);
    }
  };

  schedule<R>(task: () => R): Promise<Awaited<R>>;
  schedule<R, A extends unknown[]>(task: (...args: A) => R, ...args: A): Promise<Awaited<R>>;
  schedule<R>(options: JobOptions, task: () => R): Promise<Awaited<R>>;
  schedule<R, A extends unknown[]>(
    options: JobOptions,
    task: (...args: A) => R,
    ...args: A
  ): Promise<Awaited<R>>;
  schedule(...args: unknown[]): Promise<unknown> {
    let options: object | undefined;
    let task: unknown;
    if (typeof args[0] === "function") {
      task = args[0];
      args = args.slice(1);
      options = {};
    } else {
      options = args[0] as object;
      task = args[1];
      args = args.slice(2);
    }
    const job = new Job(
      task as (...args: never[]) => unknown,
      args as never[],
      options,
      this.jobDefaults,
      this.rejectOnDrop,
      this.Events,
      this._states,
    );
    this._receive(job);
    return job.promise;
  }

  wrap<R, A extends unknown[]>(
    fn: (...args: A) => R,
  ): ((...args: A) => Promise<Awaited<R>>) & {
    withOptions: (options: JobOptions, ...args: A) => Promise<Awaited<R>>;
  } {
    const run = (opts: JobOptions | null, thisArg: unknown, args: A): Promise<Awaited<R>> =>
      opts != null
        ? (this.schedule(opts, fn.bind(thisArg) as (...args: A) => R, ...args) as Promise<
            Awaited<R>
          >)
        : (this.schedule(fn.bind(thisArg) as (...args: A) => R, ...args) as Promise<Awaited<R>>);
    const wrapped = function (this: unknown, ...args: A): Promise<Awaited<R>> {
      return run(null, this, args);
    } as ((...args: A) => Promise<Awaited<R>>) & {
      withOptions: (options: JobOptions, ...args: A) => Promise<Awaited<R>>;
    };
    wrapped.withOptions = (options: JobOptions, ...args: A): Promise<Awaited<R>> =>
      run(options, undefined, args);
    return wrapped;
  }

  async updateSettings(options?: ConstructorOptions): Promise<this> {
    options ??= {};
    await this._store.__updateSettings__(overwrite(options, this.storeDefaults));
    overwrite(options, this.instanceDefaults, this);
    return this;
  }

  currentReservoir(): Promise<number | null> {
    return this._store.__currentReservoir__() as Promise<number | null>;
  }

  incrementReservoir(incr = 0): Promise<number | null> {
    return this._store.__incrementReservoir__(incr) as Promise<number | null>;
  }
}

export default Bottleneck;
