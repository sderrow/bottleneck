import type Bottleneck from "../Bottleneck";
import type { RedisLib, RedisLikeClient } from "./redis-types";
import BottleneckError from "../BottleneckError";
import Events from "../Events";
import { load } from "../parser";
import { normalizeReply } from "./normalizeReply";
import * as Scripts from "./Scripts";

const closeClient = (c: RedisLikeClient) => (typeof c.close === "function" ? c.close() : c.quit!());
const destroyClient = (c: RedisLikeClient) =>
  typeof c.destroy === "function" ? c.destroy() : c.disconnect!();
const safe = async (run: () => unknown): Promise<undefined> => {
  try {
    await run();
    return undefined;
  } catch {
    return undefined;
  }
};

const connectIfNeeded = async (c: RedisLikeClient) => {
  if (typeof c.connect === "function" && c.isOpen === false) {
    await c.connect();
  }
};

const stringifyArgs = (args: unknown[]): string[] =>
  args.map((a) => (a == null ? "" : typeof a === "string" ? a : String(a)));

class RedisConnection {
  Redis: RedisLib | null = null;
  clientOptions: object = {};
  client: RedisLikeClient | null = null;
  Events: Events | null = null;
  datastore = "redis";
  terminated = false;
  shas: Record<string, string> = {};
  subscriber: RedisLikeClient;
  limiters: Record<string, Bottleneck> = {};
  ready: Promise<{ client: RedisLikeClient; subscriber: RedisLikeClient }>;

  constructor(options: object = {}) {
    options ??= {};
    load(options, this.defaults, this);

    if (this.Redis == null && this.client == null) {
      throw new BottleneckError(
        "Bottleneck cluster mode requires a `Redis` library reference or a pre-built `client`. " +
          "Pass it explicitly: `new Bottleneck({ datastore: 'redis', Redis: require('redis'), clientOptions })`.",
      );
    }

    this.Events ??= new Events(this);
    this.terminated = false;

    this.client ??= this.Redis!.createClient!(this.clientOptions);
    this.subscriber = this.client.duplicate!();
    this.limiters = {};

    this.ready = this._initReady();
    // Stored init promise: consumers await `ready` lazily, so suppress the
    // unhandled-rejection that would fire before anyone attaches a handler.
    this.ready.catch(() => {});
  }

  defaults = {
    Redis: null,
    clientOptions: {},
    client: null,
    Events: null,
  };

  /** @internal */
  async _initReady() {
    await Promise.all([this._setup(this.client!, false), this._setup(this.subscriber, true)]);
    await this._loadScripts();
    return { client: this.client!, subscriber: this.subscriber };
  }

  /** @internal */
  async _setup(client: RedisLikeClient, _sub: boolean): Promise<void> {
    client.setMaxListeners?.(0);
    client.on!("error", (e: unknown) => {
      if (!this.terminated) {
        this.Events!.trigger("error", e);
      }
    });
    await connectIfNeeded(client);
  }

  /** @internal */
  async _loadScript(name: string): Promise<string> {
    this.shas[name] = await this.client!.scriptLoad!(Scripts.payload(name));
    return this.shas[name]!;
  }

  /** @internal */
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
  /** @internal */
  async __runCommand__(cmd: unknown[]): Promise<unknown> {
    await this.ready;
    const reply = await this.client!.sendCommand!(stringifyArgs(cmd));
    return normalizeReply(cmd, reply);
  }

  /** @internal */
  /** @internal */
  async __runScript__(name: string, id: string, args: unknown[]): Promise<unknown> {
    const keys = Scripts.keys(name, id);
    const stringArgs = stringifyArgs(args);
    try {
      return await this.client!.evalSha!(this.shas[name]!, { keys, arguments: stringArgs });
    } catch (e) {
      if (
        typeof (e as Error)?.message === "string" &&
        (e as Error).message.startsWith("NOSCRIPT")
      ) {
        await this._loadScript(name);
        return await this.client!.evalSha!(this.shas[name]!, { keys, arguments: stringArgs });
      }
      throw e;
    }
  }

  /** @internal */
  /** @internal */
  async __addLimiter__(instance: Bottleneck): Promise<void> {
    await Promise.all(
      [instance.channel(), instance.channel_client()].map(async (channel) => {
        this.subscriber.subscribe!(channel, (message: string) => {
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
  /** @internal */
  async __removeLimiter__(instance: Bottleneck): Promise<void> {
    await Promise.all(
      [instance.channel(), instance.channel_client()].map(async (channel) => {
        if (!this.terminated) {
          this.subscriber.unsubscribe!(channel);
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

    this.client!.removeAllListeners?.("error");
    this.client!.on?.("error", () => {});
    this.subscriber.removeAllListeners?.("error");
    this.subscriber.on?.("error", () => {});

    if (flush) {
      await Promise.all([
        safe(() => closeClient(this.client!)),
        safe(() => closeClient(this.subscriber)),
      ]);
    } else {
      await Promise.all([
        safe(() => destroyClient(this.client!)),
        safe(() => destroyClient(this.subscriber)),
      ]);
    }
  }
}

export default RedisConnection;
