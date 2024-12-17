/* eslint-disable
    no-constant-condition,
    no-undef,
*/
// TODO: This file was created by bulk-decaffeinate.
// Fix any style issues and re-enable lint.
/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/main/docs/suggestions.md
 */
class Events {
  constructor(instance) {
    this.instance = instance;
    this._events = {};
    if ((this.instance.on != null) || (this.instance.once != null) || (this.instance.removeAllListeners != null)) {
      throw new Error("An Emitter already exists for this object");
    }
    this.instance.on = (name, cb) => this._addListener(name, "many", cb);
    this.instance.once = (name, cb) => this._addListener(name, "once", cb);
    this.instance.removeAllListeners = (name=null) => {
      if (name != null) { return delete this._events[name]; } else { return this._events = {}; }
    };
  }
  _addListener(name, status, cb) {
    if (this._events[name] == null) { this._events[name] = []; }
    this._events[name].push({cb, status});
    return this.instance;
  }
  listenerCount(name) {
    if (this._events[name] != null) { return this._events[name].length; } else { return 0; }
  }
  trigger(name, ...args) {
    try {
      if (name !== "debug") { this.trigger("debug", `Event triggered: ${name}`, args); }
      if (this._events[name] == null) { return; }
      this._events[name] = this._events[name].filter(listener => listener.status !== "none");
      const promises = this._events[name].map(listener => {
        if (listener.status === "none") { return; }
        if (listener.status === "once") { listener.status = "none"; }
        try {
          const returned = typeof listener.cb === 'function' ? listener.cb(...Array.from(args || [])) : undefined;
          if (typeof (returned != null ? returned.then : undefined) === "function") {
            return await(returned);
          } else {
            return returned;
          }
        } catch (e) {
          if ("name" !== "error") { this.trigger("error", e); }
          return null;
        }
      });
      return (await(Promise.all(promises))).find(x => x != null);
    } catch (error) {
      const e = error;
      if ("name" !== "error") { this.trigger("error", e); }
      return null;
    }
  }
}

module.exports = Events;
