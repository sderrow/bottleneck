const DLList = require("./DLList");
const Events = require("./Events");

class Queues {
  constructor(num_priorities) {
    this.Events = new Events(this);
    this._length = 0;
    this._lists = [];
    for (let i = 0; i < num_priorities; i++) {
      const list = new DLList(
        () => this.incr(),
        () => this.decr(),
      );
      this._lists.push(list);
    }
  }

  incr() {
    if (this._length++ === 0) {
      return this.Events.trigger("leftzero");
    }
  }

  decr() {
    if (--this._length === 0) {
      return this.Events.trigger("zero");
    }
  }

  push(job) {
    return this._lists[job.options.priority].push(job);
  }

  queued(priority) {
    if (priority != null) {
      return this._lists[priority].length;
    } else {
      return this._length;
    }
  }

  shiftAll(fn) {
    return this._lists.forEach((list) => list.forEachShift(fn));
  }

  getFirst(arr) {
    for (const list of arr ?? this._lists) {
      if (list.length > 0) return list;
    }
    return [];
  }

  shiftLastFrom(priority) {
    return this.getFirst(this._lists.slice(priority).reverse()).shift();
  }
}

module.exports = Queues;
