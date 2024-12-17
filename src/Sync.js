/* eslint-disable
    no-undef,
*/
// TODO: This file was created by bulk-decaffeinate.
// Fix any style issues and re-enable lint.
/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/main/docs/suggestions.md
 */
const DLList = require("./DLList");
class Sync {
  constructor(name, Promise) {
    this.schedule = this.schedule.bind(this);
    this.name = name;
    this.Promise = Promise;
    this._running = 0;
    this._queue = new DLList();
  }
  isEmpty() {
    return this._queue.length === 0;
  }
  _tryToRun() {
    if (this._running < 1 && this._queue.length > 0) {
      this._running++;
      const { task, args, resolve, reject } = this._queue.shift();
      const cb = (() => {
        try {
          const returned = await(task(...Array.from(args || [])));
          return () => resolve(returned);
        } catch (error) {
          return () => reject(error);
        }
      })();
      this._running--;
      this._tryToRun();
      return cb();
    }
  }
  schedule(task, ...args) {
    let reject;
    let resolve = (reject = null);
    const promise = new this.Promise(function (_resolve, _reject) {
      resolve = _resolve;
      return (reject = _reject);
    });
    this._queue.push({ task, args, resolve, reject });
    this._tryToRun();
    return promise;
  }
}

module.exports = Sync;
