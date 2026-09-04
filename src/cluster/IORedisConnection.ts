import type Bottleneck from "../Bottleneck";
import type { IORedisConnectionOptions } from "../types";
import type { RedisLib, RedisLikeClient } from "./redis-types";
import BottleneckError from "../BottleneckError";
import Events from "../Events";
import { load } from "../parser";
import { normalizeReply } from "./normalizeReply";
import * as Scripts from "./Scripts";

class IORedisConnection {
  /** @internal */
  Redis: RedisLib | null = null;
  /** @internal */
  clientOptions: object = {};
  /** @internal */
  clusterNodes: unknown = null;
  /** @internal */
  client: RedisLikeClient | null = null;
  /** @internal */
  Events: Events | null = null;
  /** @internal */
  datastore = "ioredis";
  /** @internal */
  terminated = false;
  /** @internal */
  subscriber: RedisLikeClient;
  /** @internal */
  limiters: Record<string, Bottleneck> = {};
  ready: Promise<{ client: RedisLikeClient; subscriber: RedisLikeClient }>;

  // Installed on the instance by Events (see Events constructor).
  declare on: {
    (event: "error", listener: (error: unknown) => void): unknown;
    (event: string, listener: (...args: any[]) => unknown): unknown;
  };
  declare once: {
    (event: "error", listener: (error: unknown) => void): unknown;
    (event: string, listener: (...args: any[]) => unknown): unknown;
  };
  declare removeAllListeners: (name?: string | null) => void;

  /** @internal */
  defaults = {
    Redis: null,
    clientOptions: {},
    clusterNodes: null,
    client: null,
    Events: null,
  };

  constructor(options?: IORedisConnectionOptions) {
    load(options ?? {}, this.defaults, this);

    if (this.Redis == null && this.client == null) {
      throw new BottleneckError(
        "Bottleneck cluster mode requires a `Redis` library reference or a pre-built `client`. " +
          "Pass it explicitly: `new Bottleneck({ datastore: 'ioredis', Redis: require('ioredis'), clientOptions })`.",
      );
    }

    this.Events ??= new Events(this);
    this.terminated = false;

    if (this.clusterNodes != null) {
      this.client = new this.Redis!.Cluster!(this.clusterNodes, this.clientOptions);
      this.subscriber = new this.Redis!.Cluster!(this.clusterNodes, this.clientOptions);
    } else if (this.client != null && this.client.duplicate == null) {
      this.subscriber = new this.Redis!.Cluster!(this.client.startupNodes, this.client.options);
    } else {
      this.client ??= new this.Redis!(this.clientOptions);
      this.subscriber = this.client.duplicate!();
    }
    this.limiters = {};

    this.ready = this._initReady();
  }

  /** @internal */
  async _initReady() {
    await Promise.all([this._setup(this.client!, false), this._setup(this.subscriber, true)]);
    this._loadScripts();
    return { client: this.client!, subscriber: this.subscriber };
  }

  /** @internal */
  /** @internal */
  _setup(client: RedisLikeClient, sub: boolean): Promise<void> {
    client.setMaxListeners!(0);
    return new Promise((resolve) => {
      client.on!("error", (e: unknown) => {
        if (!this.terminated) {
          this.Events!.trigger("error", e);
        }
      });
      if (sub) {
        client.on!("message", (channel: string, message: string) => {
          (
            this.limiters[channel]?._store as unknown as {
              onMessage?: (channel: string, message: string) => Promise<unknown>;
            }
          )?.onMessage?.(channel, message);
        });
      }
      if (client.status === "ready") {
        resolve();
      } else {
        client.once!("ready", resolve);
      }
    });
  }

  /** @internal */
  /** @internal */
  _loadScripts(): void {
    Scripts.names.forEach((name) =>
      this.client!.defineCommand!(name, { lua: Scripts.payload(name) }),
    );
  }

  /** @internal */
  async __runCommand__(cmd: unknown[]): Promise<unknown> {
    await this.ready;
    const [[, value]] = (await this.client!.pipeline!([cmd]).exec()) as [[unknown, unknown]];
    // ioredis v6 can negotiate RESP3 (opt-in), where reply shapes such as
    // HGETALL and WITHSCORES differ from the RESP2 forms assumed below.
    return normalizeReply(cmd, value);
  }

  /** @internal */
  /** @internal */
  __runScript__(name: string, id: string, args: unknown[]): Promise<unknown> {
    const keys = Scripts.keys(name, id);
    const client = this.client as unknown as Record<string, (...a: unknown[]) => unknown>;
    return client[name]!(keys.length, ...keys, ...args) as Promise<unknown>;
  }

  /** @internal */
  async __addLimiter__(instance: Bottleneck): Promise<void> {
    await Promise.all(
      [instance.channel(), instance.channel_client()].map(async (channel) => {
        // ioredis returns a promise when subscribe is called without a callback.
        await this.subscriber.subscribe!(channel);
        this.limiters[channel] = instance;
      }),
    );
  }

  /** @internal */
  async __removeLimiter__(instance: Bottleneck): Promise<void> {
    await Promise.all(
      [instance.channel(), instance.channel_client()].map(async (channel) => {
        if (!this.terminated) {
          await this.subscriber.unsubscribe!(channel);
        }
        delete this.limiters[channel];
      }),
    );
  }

  async disconnect(flush = true): Promise<void> {
    for (const v of Object.values(this.limiters)) {
      clearInterval(v._store?.heartbeat);
    }
    this.limiters = {};
    this.terminated = true;

    this.client!.removeAllListeners?.("error");
    this.client!.on?.("error", () => {});
    this.subscriber.removeAllListeners?.("error");
    this.subscriber.on?.("error", () => {});

    if (flush) {
      await Promise.all([this.client!.quit!(), this.subscriber.quit!()]);
    } else {
      this.client!.disconnect!();
      this.subscriber.disconnect!();
    }
  }
}

export default IORedisConnection;
