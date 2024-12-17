/* eslint-disable
    no-unused-vars,
*/
// TODO: This file was created by bulk-decaffeinate.
// Fix any style issues and re-enable lint.
/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * DS206: Consider reworking classes to avoid initClass
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/main/docs/suggestions.md
 */
const parser = require("./parser");
const Events = require("./Events");

class Batcher {
  static initClass() {
    this.prototype.defaults = {
      maxTime: null,
      maxSize: null,
      Promise,
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
    return (this._promise = new this.Promise((res, rej) => {
      return (this._resolve = res);
    }));
  }

  _flush() {
    clearTimeout(this._timeout);
    this._lastFlush = Date.now();
    this._resolve();
    this.Events.trigger("batch", this._arr);
    this._arr = [];
    return this._resetPromise();
  }

  add(data) {
    this._arr.push(data);
    const ret = this._promise;
    if (this._arr.length === this.maxSize) {
      this._flush();
    } else if (this.maxTime != null && this._arr.length === 1) {
      this._timeout = setTimeout(() => {
        return this._flush();
      }, this.maxTime);
    }
    return ret;
  }
}
Batcher.initClass();

module.exports = Batcher;
