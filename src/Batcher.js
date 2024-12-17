/* eslint-disable
    no-unused-vars,
*/
const parser = require("./parser");
const Events = require("./Events");

class Batcher {
  static initClass() {
    this.prototype.defaults = {
      maxTime: null,
      maxSize: null,
    };
  }

  constructor(options) {
    if (options == null) {
      options = {};
    }
    this.options = options;
    parser.load(this.options, this.defaults, this);
    this.Events = new Events(this);
    this._arr = [];
    this._resetPromise();
    this._lastFlush = Date.now();
  }

  _resetPromise() {
    this._promise = new Promise((res, rej) => {
      this._resolve = res;
    });
  }

  _flush() {
    clearTimeout(this._timeout);
    this._lastFlush = Date.now();
    this._resolve();
    this.Events.trigger("batch", this._arr);
    this._arr = [];
    this._resetPromise();
  }

  add(data) {
    this._arr.push(data);
    const ret = this._promise;
    if (this._arr.length === this.maxSize) {
      this._flush();
    } else if (this.maxTime != null && this._arr.length === 1) {
      this._timeout = setTimeout(() => {
        this._flush();
      }, this.maxTime);
    }
    return ret;
  }
}
Batcher.initClass();

module.exports = Batcher;
