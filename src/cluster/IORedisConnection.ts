import type Bottleneck from "../Bottleneck";
import type { IORedisConnectionOptions } from "../types";
import type { ConnectionInternals, IORedisClient, RedisClients } from "./redis-types";
import { runDetached } from "../async-context";
import BottleneckError from "../BottleneckError";
import Events from "../Events";
import { normalizeReply } from "./normalizeReply";
import * as Scripts from "./Scripts";

const noop = () => {};

const whenReady = (c: IORedisClient) =>
  new Promise<void>((resolve) => {
    if (c.status === "ready") {
      resolve();
    } else {
      c.once("ready", resolve);
    }
  });

class IORedisConnection<C extends IORedisClient = IORedisClient> {
  /** The client passed in. The connection only sends commands on it. */
  readonly client: C;
  /** The pub/sub client: `options.subscriber`, or a `client.duplicate()` the connection owns. */
  readonly subscriber: C;
  /** @internal */
  Events: Events;
  /** @internal */
  datastore = "ioredis";
  /** @internal */
  terminated = false;
  /** @internal */
  limiters: Record<string, Bottleneck> = {};
  /** @internal Clients this connection listens to and closes. */
  owned: C[];
  ready: Promise<RedisClients<C>>;

  // Installed on the instance by Events (see Events constructor).
  declare on: {
    (event: "error", listener: (error: unknown) => void): unknown;
    (event: string, listener: (...args: never[]) => unknown): unknown;
  };
  declare once: {
    (event: "error", listener: (error: unknown) => void): unknown;
    (event: string, listener: (...args: never[]) => unknown): unknown;
  };
  declare removeAllListeners: (name?: string | null) => void;

  constructor(options: IORedisConnectionOptions<C>) {
    const {
      client,
      subscriber,
      Events: events,
      ownsClient = false,
    }: Partial<IORedisConnectionOptions<C>> & ConnectionInternals = options ?? {};
    if (client == null) {
      throw new BottleneckError(
        "IORedisConnection requires an ioredis `client`: `new IORedisConnection({ client })`.",
        "MISSING_CLIENT",
      );
    }

    this.Events = events ?? new Events(this);
    this.client = client;
    // Sockets the connection creates must not capture the constructing
    // caller's async context: pub/sub-driven work would otherwise run (and
    // trace) inside it for the connection's whole lifetime.
    this.subscriber = subscriber ?? runDetached(() => client.duplicate() as C);
    this.owned = [
      ...(ownsClient ? [client] : []),
      ...(subscriber == null ? [this.subscriber] : []),
    ];

    this.ready = runDetached(() => this._initReady());
  }

  /** @internal */
  _onOwnedError = (e: unknown): void => {
    if (!this.terminated) {
      this.Events.trigger("error", e);
    }
  };

  /** @internal */
  _onMessage = (channel: string, message: string): void => {
    (
      this.limiters[channel]?._store as unknown as {
        onMessage?: (channel: string, message: string) => Promise<unknown>;
      }
    )?.onMessage?.(channel, message);
  };

  /** @internal */
  async _initReady(): Promise<RedisClients<C>> {
    for (const c of this.owned) {
      c.setMaxListeners(0);
      c.on("error", this._onOwnedError);
    }
    this.subscriber.on("message", this._onMessage);
    await Promise.all(this.owned.map(whenReady));
    return { client: this.client, subscriber: this.subscriber };
  }

  /** @internal */
  async __runCommand__(cmd: unknown[]): Promise<unknown> {
    await this.ready;
    const [[, value]] = (await this.client.pipeline([cmd]).exec()) as [[unknown, unknown]];
    // ioredis v6 can negotiate RESP3 (opt-in), where reply shapes such as
    // HGETALL and WITHSCORES differ from the RESP2 forms assumed below.
    return normalizeReply(cmd, value);
  }

  /** @internal */
  __runScript__(name: Scripts.ScriptName, id: string, args: unknown[]): Promise<unknown> {
    const keys = Scripts.keys(name, id);
    const all = [...keys, ...Scripts.stringifyArgs(args)];
    return Scripts.run(
      name,
      (sha) => this.client.evalsha(sha, keys.length, ...all),
      (script) => this.client.eval(script, keys.length, ...all),
    );
  }

  /** @internal */
  async __addLimiter__(instance: Bottleneck): Promise<void> {
    await Promise.all(
      [instance.channel(), instance.channel_client()].map(async (channel) => {
        // ioredis returns a promise when subscribe is called without a callback.
        await this.subscriber.subscribe(channel);
        this.limiters[channel] = instance;
      }),
    );
  }

  /** @internal */
  async __removeLimiter__(instance: Bottleneck): Promise<void> {
    await Promise.all(
      [instance.channel(), instance.channel_client()].map(async (channel) => {
        if (!this.terminated) {
          await this.subscriber.unsubscribe(channel);
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
    if (this.terminated) return;
    this.terminated = true;

    this.subscriber.removeListener("message", this._onMessage);
    for (const c of this.owned) {
      // Absorb late socket errors from clients that are closing.
      c.removeListener("error", this._onOwnedError);
      c.on("error", noop);
    }
    if (flush) {
      await Promise.all(this.owned.map((c) => c.quit()));
    } else {
      for (const c of this.owned) c.disconnect();
    }
  }
}

export default IORedisConnection;
