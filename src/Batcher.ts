const parser = require("./parser");
const Events = require("./Events");

class Batcher {
  defaults = { maxTime: null, maxSize: null };

  constructor(options) {
    this.options = options ?? {};
    parser.load(this.options, this.defaults, this);
    this.Events = new Events(this);
    this._arr = [];
    this._resetPromise();
    this._lastFlush = Date.now();
  }

  _resetPromise() {
    this._promise = new Promise((res) => {
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
    const existingPromise = this._promise;
    if (this._arr.length === this.maxSize) {
      this._flush();
    } else if (this.maxTime != null && this._arr.length === 1) {
      this._timeout = setTimeout(() => {
        this._flush();
      }, this.maxTime);
    }
    return existingPromise;
  }
}

module.exports = Batcher;
