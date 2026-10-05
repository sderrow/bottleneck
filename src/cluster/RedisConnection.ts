import type Bottleneck from "../Bottleneck";
import type { RedisConnectionOptions } from "../types";
import type { NodeRedisClient, RedisClients } from "./redis-types";
import { runDetached } from "../async-context";
import BottleneckError from "../BottleneckError";
import Events from "../Events";
import { normalizeReply } from "./normalizeReply";
import * as Scripts from "./Scripts";

const closeClient = (c: NodeRedisClient) => (typeof c.close === "function" ? c.close() : c.quit());
const destroyClient = (c: NodeRedisClient) =>
  typeof c.destroy === "function" ? c.destroy() : c.disconnect();
const safe = async (run: () => unknown): Promise<undefined> => {
  try {
    await run();
    return undefined;
  } catch {
    return undefined;
  }
};
const noop = () => {};

class RedisConnection<C extends NodeRedisClient = NodeRedisClient> {
  /** The client passed in. The connection only sends commands on it. */
  readonly client: C;
  /** The pub/sub client: `options.subscriber`, or a `client.duplicate()` the connection owns. */
  readonly subscriber: C;
  /** @internal */
  Events: Events;
  /** @internal */
  datastore = "redis";
  /** @internal */
  terminated = false;
  /** @internal */
  limiters: Record<string, Bottleneck> = {};
  /** @internal Limiters using this connection, which receive its subscriber's errors. */
  instances = new Set<Bottleneck>();
  /** @internal Whether the connection duplicated (and so connects and closes) its subscriber. */
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

  constructor(options: RedisConnectionOptions<C>) {
    const { client, subscriber }: Partial<RedisConnectionOptions<C>> = options ?? {};
    if (client == null) {
      throw new BottleneckError(
        "RedisConnection requires a node-redis `client`: `new RedisConnection({ client })`.",
        "MISSING_CLIENT",
      );
    }
    for (const c of [client, subscriber]) {
      if (c?.isOpen === false) {
        throw new BottleneckError(
          "RedisConnection requires a connected node-redis client: call `client.connect()` first.",
          "CLIENT_NOT_OPEN",
        );
      }
    }

    this.Events = new Events(this);
    this.client = client;
    this.ownsSubscriber = subscriber == null;
    // Sockets the connection creates must not capture the constructing
    // caller's async context: pub/sub-driven work would otherwise run (and
    // trace) inside it for the connection's whole lifetime.
    this.subscriber = subscriber ?? runDetached(() => client.duplicate() as C);

    this.ready = runDetached(() => this._initReady());
    // Stored init promise: consumers await `ready` lazily, so suppress the
    // unhandled-rejection that would fire before anyone attaches a handler.
    this.ready.catch(() => {});
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
  async _initReady(): Promise<RedisClients<C>> {
    if (this.ownsSubscriber) {
      this.subscriber.setMaxListeners?.(0);
      this.subscriber.on("error", this._onSubscriberError);
      if (this.subscriber.isOpen === false) {
        await this.subscriber.connect();
      }
    }
    return { client: this.client, subscriber: this.subscriber };
  }

  /** @internal */
  async __runCommand__(cmd: unknown[]): Promise<unknown> {
    await this.ready;
    const reply = await this.client.sendCommand(Scripts.stringifyArgs(cmd));
    return normalizeReply(cmd, reply);
  }

  /** @internal */
  __runScript__(name: Scripts.ScriptName, id: string, args: unknown[]): Promise<unknown> {
    const options = { keys: Scripts.keys(name, id), arguments: Scripts.stringifyArgs(args) };
    return Scripts.run(
      name,
      (sha) => this.client.evalSha(sha, options),
      (script) => this.client.eval(script, options),
    );
  }

  /** @internal */
  async __addLimiter__(instance: Bottleneck): Promise<void> {
    await Promise.all(
      [instance.channel(), instance.channel_client()].map(async (channel) => {
        await this.subscriber.subscribe(channel, (message: string) => {
          (
            this.limiters[channel]?._store as unknown as {
              onMessage?: (channel: string, message: string) => Promise<unknown>;
            }
          )?.onMessage?.(channel, message);
        });
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

    if (this.ownsSubscriber) {
      // Absorb late socket errors from the closing subscriber.
      this.subscriber.removeListener("error", this._onSubscriberError);
      this.subscriber.on("error", noop);
      await safe(() => (flush ? closeClient(this.subscriber) : destroyClient(this.subscriber)));
    }
  }
}

export default RedisConnection;
