import type Bottleneck from "../Bottleneck";
import type { StoreOptions } from "../types";
import type IORedisConnection from "./IORedisConnection";
import type RedisConnection from "./RedisConnection";
import type { ScriptName } from "./Scripts";
import { setDetachedInterval, setDetachedTimeout } from "../async-context";
import BottleneckError from "../BottleneckError";
import { load, overwrite } from "../parser";

class RedisDatastore {
  /** @internal */
  _disconnecting = false;
  _orphaned = 0;
  instance: Bottleneck;
  storeOptions: StoreOptions;
  Promise: PromiseConstructor = Promise;
  timeout: number | null = null;
  heartbeatInterval = 5000;
  clientTimeout = 10000;
  clearDatastore = false;
  connection!: RedisConnection | IORedisConnection;
  originalId: string;
  clientId: string;
  capacityPriorityCounters: Record<string, ReturnType<typeof setTimeout>> = {};
  heartbeat: ReturnType<typeof setInterval> | undefined;
  ready: Promise<void>;

  constructor(instance: Bottleneck, storeOptions: StoreOptions, storeInstanceOptions: object) {
    this.instance = instance;
    this.storeOptions = storeOptions;
    this.originalId = this.instance.id;
    this.clientId = this.instance._randomIndex();
    load(storeInstanceOptions, storeInstanceOptions, this);
    this.capacityPriorityCounters = {};
    // Errors from the connection's own sockets reach every limiter on it.
    this.connection.instances.add(this.instance);

    this.instance.connection = this.connection;
    this.instance.datastore = this.connection.datastore;

    this.ready = this._initReady();
    // Stored init promise: consumers await `ready` lazily, so suppress the
    // unhandled-rejection that would fire before anyone attaches a handler.
    this.ready.catch(() => {});
  }

  /** @internal */
  async _initReady(): Promise<void> {
    await this.connection.ready;
    await this.runScript("init", this.prepareInitSettings(this.clearDatastore));
    await this.connection.__addLimiter__(this.instance);
    await this.runScript("register_client", [this.instance.queued()]);
    if (!this._disconnecting) {
      this.heartbeat = setDetachedInterval(async () => {
        try {
          const running = Number((await this.runScript("heartbeat", [])) ?? 0);
          if (running > 0 || this._orphaned > 0) {
            this._orphaned = running;
            this.instance.Events.trigger("orphaned-jobs", { running });
          }
        } catch (e) {
          if (!this._disconnecting) {
            this.instance.Events.trigger("error", e);
          }
        }
      }, this.heartbeatInterval).unref?.();
    }
  }

  /** @internal */
  async __publish__(message: string): Promise<unknown> {
    await this.ready;
    return this.connection.client.publish(this.instance.channel(), `message:${message.toString()}`);
  }

  async onMessage(_channel: string, message: string): Promise<unknown> {
    try {
      const pos = message.indexOf(":");
      const [type, data] = [message.slice(0, pos), message.slice(pos + 1)];
      if (type === "capacity") {
        return await this.instance._drainAll(data.length > 0 ? ~~data : undefined);
      } else if (type === "capacity-priority") {
        const [rawCapacity = "", priorityClient, counter = ""] = data.split(":");
        const capacity = rawCapacity.length > 0 ? ~~rawCapacity : undefined;
        if (priorityClient === this.clientId) {
          this.instance.Events.trigger("capacity-priority", capacity);
          const drained = await this.instance._drainAll(capacity);
          const newCapacity = capacity != null ? capacity - (drained || 0) : "";
          return await this.connection.client.publish(
            this.instance.channel(),
            `capacity-priority:${newCapacity}::${counter}`,
          );
        } else if (priorityClient === "") {
          clearTimeout(this.capacityPriorityCounters[counter]);
          delete this.capacityPriorityCounters[counter];
          return this.instance._drainAll(capacity);
        } else {
          return (this.capacityPriorityCounters[counter] = setDetachedTimeout(async () => {
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

  /** @internal */
  async __disconnect__(_flush?: boolean): Promise<void> {
    this._disconnecting = true;
    clearInterval(this.heartbeat);
    this.connection.instances.delete(this.instance);
    await this.connection.__removeLimiter__(this.instance);
  }

  async runScript(name: ScriptName, args: unknown[]): Promise<unknown> {
    if (name !== "init" && name !== "register_client") {
      await this.ready;
    }
    const all_args = [Date.now(), this.clientId, ...args] as unknown[];
    this.instance.Events.trigger("debug", `Calling Redis script: ${name}.lua`, all_args);
    try {
      return await this.connection.__runScript__(name, this.originalId, all_args);
    } catch (e) {
      if (
        typeof (e as Error).message === "string" &&
        (e as Error).message.match(/^(.*\s)?SETTINGS_KEY_NOT_FOUND$/) !== null
      ) {
        if (name === "heartbeat") {
          return undefined;
        }
        await this.runScript("init", this.prepareInitSettings(false));
        return this.runScript(name, args);
      } else if (
        typeof (e as Error).message === "string" &&
        (e as Error).message.match(/^(.*\s)?UNKNOWN_CLIENT$/) !== null
      ) {
        await this.runScript("register_client", [this.instance.queued()]);
        return this.runScript(name, args);
      } else {
        throw e;
      }
    }
  }

  prepareArray(arr: unknown[]): string[] {
    return arr.map((x) => (x != null ? x.toString() : ""));
  }

  prepareObject(obj: object): string[] {
    const arr: string[] = [];
    for (const [k, v] of Object.entries(obj)) {
      arr.push(k, v != null ? v.toString() : "");
    }
    return arr;
  }

  prepareInitSettings(clear: boolean): string[] {
    const args = this.prepareObject(
      Object.assign({}, this.storeOptions, {
        id: this.originalId,
        version: this.instance.version,
        groupTimeout: this.timeout,
        clientTimeout: this.clientTimeout,
      }),
    );
    (args as unknown[]).unshift(clear ? 1 : 0, this.instance.version);
    return args;
  }

  convertBool(b: unknown): boolean {
    return !!b;
  }

  /** @internal */
  async __updateSettings__(options: StoreOptions): Promise<StoreOptions> {
    await this.runScript("update_settings", this.prepareObject(options));
    return overwrite(options, options, this.storeOptions);
  }

  /** @internal */
  __running__(): Promise<unknown> {
    return this.runScript("running", []);
  }

  /** @internal */
  __queued__(): Promise<unknown> {
    return this.runScript("queued", []);
  }

  /** @internal */
  __done__(): Promise<unknown> {
    return this.runScript("done", []);
  }

  /** @internal */
  async __groupCheck__(): Promise<boolean> {
    return this.convertBool(await this.runScript("group_check", []));
  }

  /** @internal */
  __incrementReservoir__(incr: number): Promise<unknown> {
    return this.runScript("increment_reservoir", [incr]);
  }

  /** @internal */
  __currentReservoir__(): Promise<unknown> {
    return this.runScript("current_reservoir", []);
  }

  /** @internal */
  async __check__(weight: number): Promise<boolean> {
    return this.convertBool(await this.runScript("check", this.prepareArray([weight])));
  }

  /** @internal */
  async __register__(
    index: string,
    weight: number,
    expiration: unknown,
  ): Promise<{
    success: boolean;
    wait: unknown;
    reservoir: unknown;
  }> {
    const [success, wait, reservoir] = (await this.runScript(
      "register",
      this.prepareArray([index, weight, expiration]),
    )) as unknown[];

    return {
      success: this.convertBool(success),
      wait,
      reservoir,
    };
  }

  /** @internal */
  async __submit__(
    queueLength: number,
    weight: number,
  ): Promise<{
    reachedHWM: boolean;
    blocked: boolean;
    strategy: unknown;
  }> {
    try {
      const [reachedHWM, blocked, strategy] = Array.from(
        (await this.runScript("submit", this.prepareArray([queueLength, weight]))) as unknown[],
      );
      return {
        reachedHWM: this.convertBool(reachedHWM),
        blocked: this.convertBool(blocked),
        strategy,
      };
    } catch (e) {
      if (/^(ERR )?OVERWEIGHT/.test((e as Error).message)) {
        const [, weight2, maxConcurrent] = (e as Error).message.split(":");
        throw new BottleneckError(
          `Impossible to add a job having a weight of ${weight2} to a limiter having a maxConcurrent setting of ${maxConcurrent}`,
          "OVERWEIGHT",
        );
      } else {
        throw e;
      }
    }
  }

  /** @internal */
  async __free__(index: string, _weight: number): Promise<{ running: unknown }> {
    const running = await this.runScript("free", this.prepareArray([index]));
    return { running };
  }
}

export default RedisDatastore;
