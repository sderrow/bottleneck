import type {
  ConstructorOptions,
  GroupEvents,
  GroupLimiterPair,
  IORedisConnectionOptions,
  RedisConnectionOptions,
} from "./types";
import Bottleneck from "./Bottleneck";
import IORedisConnection from "./cluster/IORedisConnection";
import RedisConnection from "./cluster/RedisConnection";
import * as Scripts from "./cluster/Scripts";
import Events from "./Events";
import { load, overwrite } from "./parser";

type Connection = RedisConnection | IORedisConnection;

class Group {
  /** @internal */
  defaults = {
    timeout: 1000 * 60 * 5,
    connection: null,
    id: "group-key",
  };

  // Installed on the instance by Events (see Events constructor).
  declare on: {
    <E extends keyof GroupEvents>(event: E, listener: GroupEvents[E]): unknown;
    (event: string, listener: (...args: any[]) => unknown): unknown;
  };
  declare once: {
    <E extends keyof GroupEvents>(event: E, listener: GroupEvents[E]): unknown;
    (event: string, listener: (...args: any[]) => unknown): unknown;
  };
  declare removeAllListeners: (name?: string | null) => void;

  /** @internal */
  timeout: number = this.defaults.timeout;
  connection: Connection | null = null;
  id: string = this.defaults.id;
  /** @internal */
  limiterOptions: Record<string, unknown>;
  /** @internal */
  Events: Events;
  /** @internal */
  instances: Record<string, Bottleneck>;
  /** @internal */
  interval: ReturnType<typeof setInterval> | undefined;
  /** @internal */
  sharedConnection: boolean;
  /** @internal */
  Bottleneck: typeof Bottleneck;

  constructor(limiterOptions: ConstructorOptions = {}) {
    this.deleteKey = this.deleteKey.bind(this);
    this.limiterOptions = { ...limiterOptions } as Record<string, unknown>;
    load(this.limiterOptions, this.defaults, this);
    this.Events = new Events(this);
    this.instances = {};
    this._startAutoCleanup();
    this.sharedConnection = this.connection != null;
    this.Bottleneck = Bottleneck;

    if (this.connection == null) {
      if (this.limiterOptions.datastore === "redis") {
        // Options come from user limiterOptions; the constructor validates that
        // Redis or client is present at runtime.
        this.connection = new RedisConnection(
          Object.assign({}, this.limiterOptions, {
            Events: this.Events,
          }) as unknown as RedisConnectionOptions,
        );
      } else if (this.limiterOptions.datastore === "ioredis") {
        this.connection = new IORedisConnection(
          Object.assign({}, this.limiterOptions, {
            Events: this.Events,
          }) as unknown as IORedisConnectionOptions,
        );
      }
    }
  }

  key(key = ""): Bottleneck {
    let limiter = this.instances[key];
    if (!limiter) {
      limiter = new this.Bottleneck(
        Object.assign(this.limiterOptions, {
          id: `${this.id}-${key}`,
          timeout: this.timeout,
          connection: this.connection,
        }) as ConstructorOptions,
      );
      this.Events.trigger("created", limiter, key);
      this.instances[key] = limiter;
    }
    return limiter;
  }

  async deleteKey(key = ""): Promise<boolean> {
    let deleted: unknown;
    const instance = this.instances[key];
    if (this.connection) {
      deleted = await this.connection.__runCommand__([
        "del",
        ...Scripts.allKeys(`${this.id}-${key}`),
      ]);
    }
    if (instance != null) {
      delete this.instances[key];
      await instance.disconnect();
    }
    return instance != null || (deleted as number) > 0;
  }

  limiters(): GroupLimiterPair[] {
    return Object.entries(this.instances).map(([key, limiter]) => ({ key, limiter }));
  }

  keys(): string[] {
    return Object.keys(this.instances);
  }

  async clusterKeys(): Promise<string[]> {
    if (this.connection == null) {
      return Promise.resolve(this.keys());
    }
    const keys: string[] = [];
    let cursor: number | null = null;
    const start = `b_${this.id}-`.length;
    const end = "_settings".length;
    while (cursor !== 0) {
      const [next, found] = (await this.connection.__runCommand__([
        "scan",
        cursor ?? 0,
        "match",
        `b_${this.id}-*_settings`,
        "count",
        10000,
      ])) as [string | number | null, string[]];
      cursor = ~~next!;
      for (const k of found) {
        keys.push(k.slice(start, -end));
      }
    }
    return keys;
  }

  /** @internal */
  /** @internal */
  _startAutoCleanup(): void {
    clearInterval(this.interval);

    this.interval = setInterval(async () => {
      const time = Date.now();
      for (const [k, v] of Object.entries(this.instances)) {
        try {
          if (
            await (
              v._store as unknown as { __groupCheck__: (t: number) => Promise<boolean> }
            ).__groupCheck__(time)
          ) {
            await this.deleteKey(k);
          }
        } catch (e) {
          v.Events.trigger("error", e);
        }
      }
    }, this.timeout / 2).unref?.();
  }

  updateSettings(options: ConstructorOptions = {}): void {
    options ??= {};
    overwrite(options, this.defaults, this);
    overwrite(options, options, this.limiterOptions);
    if (options.timeout != null) {
      this._startAutoCleanup();
    }
  }

  disconnect(flush = true): Promise<void> | undefined {
    clearInterval(this.interval);
    if (!this.sharedConnection) {
      return this.connection?.disconnect(flush);
    }
  }
}

export default Group;
