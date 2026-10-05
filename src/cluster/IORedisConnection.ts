import type Bottleneck from "../Bottleneck";
import type { IORedisConnectionOptions } from "../types";
import type { IORedisClient, RedisClients } from "./redis-types";
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
  /** @internal Limiters using this connection, which receive its subscriber's errors. */
  instances = new Set<Bottleneck>();
  /** @internal Whether the connection duplicated (and so closes) its subscriber. */
  ownsSubscriber: boolean;
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
    const { client, subscriber }: Partial<IORedisConnectionOptions<C>> = options ?? {};
    if (client == null) {
      throw new BottleneckError(
        "IORedisConnection requires an ioredis `client`: `new IORedisConnection({ client })`.",
        "MISSING_CLIENT",
      );
    }

    this.Events = new Events(this);
    this.client = client;
    this.ownsSubscriber = subscriber == null;
    // Sockets the connection creates must not capture the constructing
    // caller's async context: pub/sub-driven work would otherwise run (and
    // trace) inside it for the connection's whole lifetime.
    this.subscriber = subscriber ?? runDetached(() => client.duplicate() as C);

    this.ready = runDetached(() => this._initReady());
  }

  /** @internal */
  _onSubscriberError = (e: unknown): void => {
    if (this.terminated) return;
    this.Events.trigger("error", e);
    for (const instance of this.instances) {
      instance.Events.trigger("error", e);
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
    this.subscriber.on("message", this._onMessage);
    if (this.ownsSubscriber) {
      this.subscriber.setMaxListeners(0);
      this.subscriber.on("error", this._onSubscriberError);
      await whenReady(this.subscriber);
    }
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
        // Only release channels this limiter holds: one whose setup failed
        // never subscribed (unsubscribing on its dead subscriber would never
        // settle), and a later limiter with the same id may own `channel()`.
        if (this.limiters[channel] !== instance) return;
        delete this.limiters[channel];
        if (this.terminated) return;
        try {
          await this.subscriber.unsubscribe(channel);
        } catch (e) {
          // Closing the connection mid-UNSUBSCRIBE releases the channel anyway.
          if (!this.terminated) throw e;
        }
      }),
    );
  }

  /**
   * Stop the connection's limiters and close the subscriber it duplicated.
   * The client (and a subscriber passed in) stay open: close them yourself.
   */
  async disconnect(flush = true): Promise<void> {
    for (const v of Object.values(this.limiters)) {
      clearInterval(v._store?.heartbeat);
    }
    this.limiters = {};
    this.instances.clear();
    if (this.terminated) return;
    this.terminated = true;

    this.subscriber.removeListener("message", this._onMessage);
    if (this.ownsSubscriber) {
      // Absorb late socket errors from the closing subscriber.
      this.subscriber.removeListener("error", this._onSubscriberError);
      this.subscriber.on("error", noop);
      if (flush) {
        await this.subscriber.quit();
      } else {
        this.subscriber.disconnect();
      }
    }
  }
}

export default IORedisConnection;
