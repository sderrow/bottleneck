class Events {
  constructor(instance) {
    this.instance = instance;
    this._events = {};
    if (
      this.instance.on != null ||
      this.instance.once != null ||
      this.instance.removeAllListeners != null
    ) {
      throw new Error("An Emitter already exists for this object");
    }
    this.instance.on = (name, cb) => this._addListener(name, "many", cb);
    this.instance.once = (name, cb) => this._addListener(name, "once", cb);
    this.instance.removeAllListeners = (name = null) => {
      if (name != null) {
        delete this._events[name];
      } else {
        this._events = {};
      }
    };
  }
  _addListener(name, status, cb) {
    this._events[name] ??= [];
    this._events[name].push({ cb, status });
    return this.instance;
  }
  listenerCount(name) {
    return this._events[name]?.length ?? 0;
  }
  async trigger(name, ...args) {
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

module.exports = Events;
