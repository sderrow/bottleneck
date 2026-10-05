/**
 * Minimal structural types for the optional Redis peer clients. Bottleneck
 * supports node-redis v4/v5/v6 and ioredis v5/v6, which are peer dependencies
 * and must never be imported at runtime (the `light` build contains no cluster
 * code at all), so the client surface is described structurally. Methods every
 * supported version has are required; methods that vary by version are not.
 */

import type { ScriptName } from "./Scripts";

type BaseClient = {
  on(event: string, cb: (...args: any[]) => void): unknown;
  removeAllListeners?(event?: string): unknown;
  publish(channel: string, message: string): Promise<unknown>;
  unsubscribe(channel: string): unknown;
};

/** node-redis v4 closes with `quit`/`disconnect`; v5+ adds `close`/`destroy`. */
export type NodeRedisClient = BaseClient &
  ({ close(): unknown } | { close?: undefined; quit(): unknown }) &
  ({ destroy(): unknown } | { destroy?: undefined; disconnect(): unknown }) & {
    connect(): unknown;
    isOpen: boolean;
    duplicate(): NodeRedisClient;
    setMaxListeners?(n: number): unknown;
    sendCommand(args: string[]): Promise<unknown>;
    scriptLoad(script: string): Promise<string>;
    evalSha(sha: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
    subscribe(channel: string, cb: (message: string) => void): unknown;
  };

/**
 * Installed on ioredis clients by `defineCommand()` in
 * IORedisConnection._loadScripts, which runs before the connection is ready.
 */
type IORedisScriptCommands = Record<
  ScriptName,
  (numKeys: number, ...args: unknown[]) => Promise<unknown>
>;

export type IORedisClient = BaseClient & {
  status: string;
  once(event: string, cb: (...args: any[]) => void): unknown;
  setMaxListeners(n: number): unknown;
  /** Absent on ioredis Cluster clients. */
  duplicate?(): IORedisClient;
  quit(): Promise<unknown>;
  disconnect(): unknown;
  subscribe(channel: string): Promise<unknown>;
  pipeline(commands: unknown[]): { exec(): Promise<unknown> };
  defineCommand(name: string, options: { lua: string }): unknown;
  // ioredis Cluster clients only
  startupNodes?: unknown;
  options?: unknown;
} & IORedisScriptCommands;

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
