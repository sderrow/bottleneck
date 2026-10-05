import type Bottleneck from "../Bottleneck";
import type { RedisConnectionOptions } from "../types";
import type { NodeRedisClient, NodeRedisLib, RedisClients } from "./redis-types";
import BottleneckError from "../BottleneckError";
import Events from "../Events";
import { load } from "../parser";
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

const connectIfNeeded = async (c: NodeRedisClient) => {
  if (c.isOpen === false) {
    await c.connect();
  }
};

const stringifyArgs = (args: unknown[]): string[] =>
  args.map((a) => (a == null ? "" : typeof a === "string" ? a : String(a)));

class RedisConnection {
  /** @internal */
  Redis: NodeRedisLib | null;
  /** @internal */
  clientOptions: object;
  /** @internal */
  client: NodeRedisClient;
  /** @internal */
  Events: Events;
  /** @internal */
  datastore = "redis";
  /** @internal */
  terminated = false;
  /** @internal */
  shas: Partial<Record<Scripts.ScriptName, string>> = {};
  /** @internal */
  subscriber: NodeRedisClient;
  /** @internal */
  limiters: Record<string, Bottleneck> = {};
  ready: Promise<RedisClients<NodeRedisClient>>;

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

  constructor(options?: RedisConnectionOptions) {
    const opts = load(options ?? {}, this.defaults);
    this.Redis = opts.Redis;
    this.clientOptions = opts.clientOptions;

    const client = opts.client ?? opts.Redis?.createClient(opts.clientOptions);
    if (client == null) {
      throw new BottleneckError(
        "Bottleneck cluster mode requires a `Redis` library reference or a pre-built `client`. " +
          "Pass it explicitly: `new Bottleneck({ datastore: 'redis', Redis: require('redis'), clientOptions })`.",
        "MISSING_CLIENT",
      );
    }

    this.Events = opts.Events ?? new Events(this);
    this.terminated = false;

    this.client = client;
    this.subscriber = this.client.duplicate();
    this.limiters = {};

    this.ready = this._initReady();
    // Stored init promise: consumers await `ready` lazily, so suppress the
    // unhandled-rejection that would fire before anyone attaches a handler.
    this.ready.catch(() => {});
  }

  /** @internal */
  defaults: {
    Redis: NodeRedisLib | null;
    clientOptions: object;
    client: NodeRedisClient | null;
    Events: Events | null;
  } = {
    Redis: null,
    clientOptions: {},
    client: null,
    Events: null,
  };

  /** @internal */
  async _initReady() {
    await Promise.all([this._setup(this.client, false), this._setup(this.subscriber, true)]);
    await this._loadScripts();
    return { client: this.client, subscriber: this.subscriber };
  }

  /** @internal */
  async _setup(client: NodeRedisClient, _sub: boolean): Promise<void> {
    client.setMaxListeners?.(0);
    client.on("error", (e: unknown) => {
      if (!this.terminated) {
        this.Events.trigger("error", e);
      }
    });
    await connectIfNeeded(client);
  }

  /** @internal */
  async _loadScript(name: Scripts.ScriptName): Promise<string> {
    const sha = await this.client.scriptLoad(Scripts.payload(name));
    this.shas[name] = sha;
    return sha;
  }

  /** @internal */
  _loadScripts(): Promise<unknown[]> {
    return Promise.all(
      Scripts.names.map(async (k) => {
        try {
          return await this._loadScript(k);
        } catch (e) {
          if (!this.terminated) throw e;
        }
      }),
    );
  }

  /** @internal */
  async __runCommand__(cmd: unknown[]): Promise<unknown> {
    await this.ready;
    const reply = await this.client.sendCommand(stringifyArgs(cmd));
    return normalizeReply(cmd, reply);
  }

  /** @internal */
  async __runScript__(name: Scripts.ScriptName, id: string, args: unknown[]): Promise<unknown> {
    const keys = Scripts.keys(name, id);
    const stringArgs = stringifyArgs(args);
    try {
      const sha = this.shas[name] ?? (await this._loadScript(name));
      return await this.client.evalSha(sha, { keys, arguments: stringArgs });
    } catch (e) {
      if (
        typeof (e as Error)?.message === "string" &&
        (e as Error).message.startsWith("NOSCRIPT")
      ) {
        const sha = await this._loadScript(name);
        return await this.client.evalSha(sha, { keys, arguments: stringArgs });
      }
      throw e;
    }
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

    this.client.removeAllListeners?.("error");
    this.client.on?.("error", () => {});
    this.subscriber.removeAllListeners?.("error");
    this.subscriber.on?.("error", () => {});

    if (flush) {
      await Promise.all([
        safe(() => closeClient(this.client)),
        safe(() => closeClient(this.subscriber)),
      ]);
    } else {
      await Promise.all([
        safe(() => destroyClient(this.client)),
        safe(() => destroyClient(this.subscriber)),
      ]);
    }
  }
}

export default RedisConnection;
