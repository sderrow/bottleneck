/**
 * Public option/value shapes. These types are re-exported from the package
 * root and form the published type contract (see test/types.test-d.ts).
 * Internal-only shapes (StoreOptions, ResolvedJobOptions, ...) live at the
 * bottom; they stay loose because user-supplied option objects are forwarded
 * opaquely through parser.load/overwrite.
 */
import type Bottleneck from "./Bottleneck";

/** Queue-shedding strategy when the queue length reaches `highWater`. */
export type Strategy = 1 | 2 | 3 | 4;

export type StrategyConstants = {
  /** Drop the oldest job with the lowest priority. */
  readonly LEAK: 1;
  /** Do not add the new job (ignores priority). */
  readonly OVERFLOW: 2;
  /** Blocked mode until `penalty` ms have passed without a new job. */
  readonly BLOCK: 3;
  /** Drop only jobs less important than the one being added. */
  readonly OVERFLOW_PRIORITY: 4;
};

/** Options accepted by `new Bottleneck(...)` and `updateSettings()`. */
export type ConstructorOptions = {
  /** How many jobs can be running at the same time. */
  readonly maxConcurrent?: number | null;
  /** How long to wait after launching a job before launching another one. */
  readonly minTime?: number | null;
  /** Queue length at which the selected `strategy` sheds load. */
  readonly highWater?: number | null;
  /** Which strategy to use if the queue gets longer than `highWater`. */
  readonly strategy?: Strategy | null;
  /** The `penalty` value used by the `Bottleneck.strategy.BLOCK` strategy. */
  readonly penalty?: number | null;
  /** How many jobs can be executed before the limiter stops executing jobs. */
  readonly reservoir?: number | null;
  /** Every `reservoirRefreshInterval` ms, `reservoir` resets to `reservoirRefreshAmount`. */
  readonly reservoirRefreshInterval?: number | null;
  /** The value `reservoir` resets to when `reservoirRefreshInterval` is in use. */
  readonly reservoirRefreshAmount?: number | null;
  /** The increment applied to `reservoir` when `reservoirIncreaseInterval` is in use. */
  readonly reservoirIncreaseAmount?: number | null;
  /** Every `reservoirIncreaseInterval` ms, `reservoir` increments by `reservoirIncreaseAmount`. */
  readonly reservoirIncreaseInterval?: number | null;
  /** The maximum value `reservoir` can reach when `reservoirIncreaseInterval` is in use. */
  readonly reservoirIncreaseMaximum?: number | null;
  /** Optional identifier. */
  readonly id?: string | null;
  /** Set to false to leave failed (dropped) jobs hanging instead of rejecting them. */
  readonly rejectOnDrop?: boolean | null;
  /** Set to true to track done jobs with counts() and jobStatus(). Uses more memory. */
  readonly trackDoneStatus?: boolean | null;
  /** Where the limiter stores its internal state: `local` (default) or `redis`/`ioredis` for Clustering. */
  readonly datastore?: string | null;
  /** Override the Promise library used by Bottleneck. */
  readonly Promise?: unknown;
  /** Passed directly to the redis client library you've selected. */
  readonly clientOptions?: unknown;
  /** **ioredis only.** When set, the client is created via `new Redis.Cluster(clusterNodes, clientOptions)`. */
  readonly clusterNodes?: unknown;
  /** The imported client library (`redis` or `ioredis`), required for clustering unless `client`/`connection` is provided. */
  readonly Redis?: unknown;
  /** A pre-built client to use instead of creating one from `clientOptions`. */
  readonly client?: unknown;
  /** A connection object from `new Bottleneck.RedisConnection` / `new Bottleneck.IORedisConnection`. */
  readonly connection?: unknown;
  /** When true, the limiter wipes existing Bottleneck state on the Redis db at startup. */
  readonly clearDatastore?: boolean | null;
  /** Redis TTL in ms for the limiter's keys (state removed after this much inactivity). Defaults to 300000 under a Group. */
  readonly timeout?: number | null;
  /** Every `heartbeatInterval` ms, the `reservoir` is assessed. */
  readonly heartbeatInterval?: number | null;
};

/** Per-job options accepted by `schedule()`, `wrap().withOptions` and `submit`-style calls. */
export type JobOptions = {
  /** A priority between 0 and 9; lower runs first. Default 5. */
  readonly priority?: number | null;
  /** Must be an integer >= 0; increases `running` counts and decreases `reservoir`. Default 1. */
  readonly weight?: number | null;
  /** Milliseconds a job has to finish before it is failed with a BottleneckError. */
  readonly expiration?: number | null;
  /** Optional identifier, helps with debug output. */
  readonly id?: string | null;
};

/** Options accepted by `stop()`. */
export type StopOptions = {
  /** When true, drop all RECEIVED/QUEUED/RUNNING jobs; when false, wait for them to complete. */
  readonly dropWaitingJobs?: boolean | null;
  /** Error message used to drop jobs when `dropWaitingJobs` is true. */
  readonly dropErrorMessage?: string | null;
  /** Error message used to reject jobs added after `stop()`. */
  readonly enqueueErrorMessage?: string | null;
};

/** Options accepted by `new Bottleneck.Batcher(...)`. */
export type BatcherOptions = {
  /** Maximum time (ms) a request waits before the batch is flushed to the `"batch"` event. */
  readonly maxTime?: number | null;
  /** Maximum number of requests in a batch. */
  readonly maxSize?: number | null;
};

/** Datastore-specific map of raw redis clients. */
export type ClientsList = { client?: any; subscriber?: any };

export type GroupLimiterPair = { key: string; limiter: Bottleneck };

export type EventInfo = {
  readonly args: any[];
  readonly options: {
    readonly id: string;
    readonly priority: number;
    readonly weight: number;
    readonly expiration?: number;
  };
};
export type EventInfoDropped = EventInfo & {
  readonly task: (...args: any[]) => any;
  readonly promise: Promise<any>;
};
export type EventInfoQueued = EventInfo & {
  readonly reachedHWM: boolean;
  readonly blocked: boolean;
};
export type EventInfoRetryable = EventInfo & { readonly retryCount: number };

export type Status = "RECEIVED" | "QUEUED" | "RUNNING" | "EXECUTING" | "DONE";
export type Counts = {
  RECEIVED: number;
  QUEUED: number;
  RUNNING: number;
  EXECUTING: number;
  DONE?: number;
};

/** Event map for the limiter. The `"failed"` listener returns a retry delay in ms (or nothing). */
export type BottleneckEvents = {
  debug: (message: string, info: any) => void;
  message: (message: string) => void;
  error: (error: unknown) => void;
  empty: () => void;
  idle: () => void;
  depleted: (empty: boolean) => void;
  dropped: (info: EventInfoDropped) => void;
  received: (info: EventInfo) => void;
  queued: (info: EventInfoQueued) => void;
  scheduled: (info: EventInfo) => void;
  executing: (info: EventInfoRetryable) => void;
  failed: (
    error: unknown,
    info: EventInfoRetryable,
  ) => Promise<number | void | null> | number | void | null;
  retry: (message: string, info: EventInfoRetryable) => void;
  done: (info: EventInfoRetryable) => void;
};

export type GroupEvents = {
  debug: (message: string, info: any) => void;
  error: (error: unknown) => void;
  created: (limiter: Bottleneck, key: string) => void;
};

export type BatcherEvents<T> = {
  debug: (message: string, info: any) => void;
  error: (error: unknown) => void;
  batch: (batch: T[]) => void;
};

// ---------------------------------------------------------------------------
// Internal shapes (not part of the published contract)
// ---------------------------------------------------------------------------

export type StoreOptions = {
  maxConcurrent: number | null;
  minTime: number;
  highWater: number | null;
  strategy: number | null;
  penalty: number | null;
  reservoir: number | null;
  reservoirRefreshInterval: number | null;
  reservoirRefreshAmount: number | null;
  reservoirIncreaseInterval: number | null;
  reservoirIncreaseAmount: number | null;
  reservoirIncreaseMaximum: number | null;
  [key: string]: unknown;
};

/** Fully-merged per-job options as they exist on a Job at runtime. */
export type ResolvedJobOptions = {
  priority: number;
  weight: number;
  expiration: number | null;
  id: string;
  [key: string]: unknown;
};

export type JobDefaults = ResolvedJobOptions;
