import type Bottleneck from "../Bottleneck";
import type { RedisConnectionOptions } from "../types";
import type { ConnectionInternals, NodeRedisClient, RedisClients } from "./redis-types";
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
  /** @internal Clients this connection connects, listens to, and closes. */
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

  constructor(options: RedisConnectionOptions<C>) {
    const {
      client,
      subscriber,
      Events: events,
      ownsClient = false,
    }: Partial<RedisConnectionOptions<C>> & ConnectionInternals = options ?? {};
    if (client == null) {
      throw new BottleneckError(
        "RedisConnection requires a node-redis `client`: `new RedisConnection({ client })`.",
        "MISSING_CLIENT",
      );
    }
    for (const c of ownsClient ? [subscriber] : [client, subscriber]) {
      if (c?.isOpen === false) {
        throw new BottleneckError(
          "RedisConnection requires a connected node-redis client: call `client.connect()` first.",
          "CLIENT_NOT_OPEN",
        );
      }
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
    // Stored init promise: consumers await `ready` lazily, so suppress the
    // unhandled-rejection that would fire before anyone attaches a handler.
    this.ready.catch(() => {});
  }

  /** @internal */
  _onOwnedError = (e: unknown): void => {
    if (!this.terminated) {
      this.Events.trigger("error", e);
    }
  };

  /** @internal */
  async _initReady(): Promise<RedisClients<C>> {
    await Promise.all(
      this.owned.map(async (c) => {
        c.setMaxListeners?.(0);
        c.on("error", this._onOwnedError);
        if (c.isOpen === false) {
          await c.connect();
        }
      }),
    );
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

    await Promise.all(
      this.owned.map((c) => {
        // Absorb late socket errors from clients that are closing.
        c.removeListener("error", this._onOwnedError);
        c.on("error", noop);
        return safe(() => (flush ? closeClient(c) : destroyClient(c)));
      }),
    );
  }
}

export default RedisConnection;
