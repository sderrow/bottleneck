type ListenerCb = (...args: unknown[]) => unknown;
type Listener = {
  cb: ListenerCb;
  status: "many" | "once" | "none";
};

/**
 * Installs `on`/`once`/`removeAllListeners` onto an instance that does not
 * have an emitter of its own.
 */
class Events {
  instance: object;
  /** @internal */
  /** @internal */
  _events: Record<string, Listener[]> = {};

  constructor(instance: object) {
    this.instance = instance;
    const target = this.instance as Record<string, unknown>;
    if (target.on != null || target.once != null || target.removeAllListeners != null) {
      throw new Error("An Emitter already exists for this object");
    }
    target.on = (name: string, cb: ListenerCb) => this._addListener(name, "many", cb);
    target.once = (name: string, cb: ListenerCb) => this._addListener(name, "once", cb);
    target.removeAllListeners = (name: string | null = null) => {
      if (name != null) {
        delete this._events[name];
      } else {
        this._events = {};
      }
    };
  }
  /** @internal */
  /** @internal */
  _addListener(name: string, status: Listener["status"], cb: ListenerCb): object {
    this._events[name] ??= [];
    this._events[name].push({ cb, status });
    return this.instance;
  }
  listenerCount(name: string): number {
    return this._events[name]?.length ?? 0;
  }
  async trigger(name: string, ...args: unknown[]): Promise<unknown | undefined> {
    try {
      if (name !== "debug") {
        this.trigger("debug", `Event triggered: ${name}`, args);
      }

      if (this._events[name] == null) return;

      this._events[name] = this._events[name].filter((listener) => listener.status !== "none");
      const allEvents = await Promise.all(
        this._events[name].map(async (listener) => {
          if (listener.status === "once") listener.status = "none";
          try {
            return typeof listener.cb === "function" ? listener.cb(...(args || [])) : undefined;
          } catch (e) {
            if (name !== "error") this.trigger("error", e);
            return null;
          }
        }),
      );

      return allEvents.find((x) => x != null);
    } catch (error) {
      const e = error;
      if (name !== "error") {
        this.trigger("error", e);
      }
      return null;
    }
  }
}

export default Events;
