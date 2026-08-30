/**
 * Minimal structural types for the optional Redis peer clients. Bottleneck
 * supports node-redis v4/v5/v6 and ioredis v5/v6, which are peer dependencies
 * and must never be imported at runtime (the `light` build contains no cluster
 * code at all), so the client surface is described structurally and kept
 * intentionally loose.
 */

export type RedisLikeClient = {
  close?(): unknown;
  quit?(): unknown;
  destroy?(): unknown;
  disconnect?(): unknown;
  connect?(): unknown;
  duplicate?(): RedisLikeClient;
  isOpen?: boolean;
  status?: string;
  on?(event: string, cb: (...args: any[]) => void): unknown;
  once?(event: string, cb: (...args: any[]) => void): unknown;
  removeAllListeners?(event?: string): unknown;
  setMaxListeners?(n: number): unknown;
  sendCommand?(args: string[]): Promise<unknown>;
  scriptLoad?(script: string): Promise<string>;
  evalSha?(sha: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
  subscribe?(channel: string, cb?: (message: string) => void): unknown;
  unsubscribe?(channel: string): unknown;
  publish?(channel: string, message: string): Promise<unknown>;
  pipeline?(commands: unknown[]): { exec(): Promise<unknown> };
  defineCommand?(name: string, options: { lua: string }): unknown;
  // ioredis Cluster clients only
  startupNodes?: unknown;
  options?: unknown;
  [key: string]: unknown;
};

/** Constructor shape of either peer library (`new Redis(...)`) plus the
 * statics Bottleneck touches. */
export type RedisLib = {
  new (options: unknown): RedisLikeClient;
  createClient?(options: unknown): RedisLikeClient;
  Cluster?: new (nodes: unknown, options: unknown) => RedisLikeClient;
};
