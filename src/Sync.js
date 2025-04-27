const DLList = require("./DLList");
class Sync {
  constructor(name) {
    this.schedule = this.schedule.bind(this);
    this.name = name;
    this._running = 0;
    this._queue = new DLList();
  }
  isEmpty() {
    return this._queue.length === 0;
  }
  async _tryToRun() {
    if (this._running < 1 && this._queue.length > 0) {
      this._running++;
      const { task, args, resolve, reject } = this._queue.shift();
      let cb;
      try {
        const returned = await task(...(args || []));
        cb = () => resolve(returned);
      } catch (error) {
        cb = () => reject(error);
      }
      this._running--;
      this._tryToRun();
      cb();
    }
  }
  schedule(task, ...args) {
    let reject;
    let resolve = (reject = null);
    const promise = new Promise(function (_resolve, _reject) {
      resolve = _resolve;
      reject = _reject;
    });
    this._queue.push({ task, args, resolve, reject });
    this._tryToRun();
    return promise;
  }
}

module.exports = Sync;
