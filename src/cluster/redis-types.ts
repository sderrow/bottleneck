import type Events from "../Events";

/**
 * Minimal structural types for the optional Redis peer clients. Bottleneck
 * supports node-redis v4/v5/v6 and ioredis v5/v6, which are peer dependencies
 * and must never be imported at runtime (the `light` build contains no cluster
 * code at all), so the client surface is described structurally. Methods every
 * supported version has are required; methods that vary by version are not.
 *
 * These are the constraints on the clients passed to `RedisConnection` and
 * `IORedisConnection`: real clients from the supported versions satisfy them.
 */

// Method-shaped so listeners are checked bivariantly: real clients type them as
// EventEmitter's `(...args: any[]) => void`, and Bottleneck passes narrower
// callbacks like `(channel: string, message: string) => void`.
type Listener = { bivarianceHack(...args: unknown[]): void }["bivarianceHack"];

type BaseClient = {
  on(event: string, cb: Listener): unknown;
  removeListener(event: string, cb: Listener): unknown;
  publish(channel: string, message: string): Promise<unknown>;
  unsubscribe(channel: string): unknown;
};

type ScriptOptions = { keys: string[]; arguments: string[] };

/** A node-redis client. v4 closes with `quit`/`disconnect`; v5+ adds `close`/`destroy`. */
export type NodeRedisClient = BaseClient &
  ({ close(): unknown } | { close?: undefined; quit(): unknown }) &
  ({ destroy(): unknown } | { destroy?: undefined; disconnect(): unknown }) & {
    connect(): unknown;
    readonly isOpen: boolean;
    duplicate(): NodeRedisClient;
    setMaxListeners?(n: number): unknown;
    sendCommand(args: string[]): Promise<unknown>;
    evalSha(sha: string, options: ScriptOptions): Promise<unknown>;
    eval(script: string, options: ScriptOptions): Promise<unknown>;
    subscribe(channel: string, cb: (message: string) => void): unknown;
  };

/** An ioredis client: a standalone `Redis` or a `Redis.Cluster`. */
export type IORedisClient = BaseClient & {
  readonly status: string;
  once(event: string, cb: Listener): unknown;
  setMaxListeners(n: number): unknown;
  duplicate(): IORedisClient;
  quit(): Promise<unknown>;
  disconnect(): unknown;
  subscribe(channel: string): Promise<unknown>;
  pipeline(commands: unknown[][]): { exec(): Promise<unknown> };
  evalsha(sha: string, numKeys: number, ...args: string[]): Promise<unknown>;
  eval(script: string, numKeys: number, ...args: string[]): Promise<unknown>;
};

export type RedisClients<C extends BaseClient = NodeRedisClient | IORedisClient> = {
  client: C;
  subscriber: C;
};

/** The `redis` module statics Bottleneck touches. */
export type NodeRedisLib = {
  createClient(options: unknown): NodeRedisClient;
};

/** The `ioredis` constructor (`new Redis(...)`) plus its `Cluster` static. */
export type IORedisLib = {
  new (options: unknown): IORedisClient;
  Cluster: new (nodes: unknown, options: unknown) => IORedisClient;
};

/**
 * @internal Options only Bottleneck itself passes to a connection: the event
 * sink of the limiter/Group that built it, and whether the connection owns
 * (connects, listens to, and closes) `client` as well as its subscriber.
 */
export type ConnectionInternals = { Events?: Events; ownsClient?: boolean };
