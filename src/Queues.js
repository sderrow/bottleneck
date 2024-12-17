/* eslint-disable
    no-unused-vars,
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
const DLList = require("./DLList");
const Events = require("./Events");

class Queues {

  constructor(num_priorities) {
    this.Events = new Events(this);
    this._length = 0;
    this._lists = __range__(1, num_priorities, true).map((i) => new DLList((() => this.incr()), (() => this.decr())));
  }

  incr() { if (this._length++ === 0) { return this.Events.trigger("leftzero"); } }

  decr() { if (--this._length === 0) { return this.Events.trigger("zero"); } }

  push(job) { return this._lists[job.options.priority].push(job); }

  queued(priority) { if (priority != null) { return this._lists[priority].length; } else { return this._length; } }

  shiftAll(fn) { return this._lists.forEach(list => list.forEachShift(fn)); }

  getFirst(arr) {
    if (arr == null) { arr = this._lists; }
    for (var list of Array.from(arr)) {
      if (list.length > 0) { return list; }
    }
    return [];
  }

  shiftLastFrom(priority) { return this.getFirst(this._lists.slice(priority).reverse()).shift(); }
}

module.exports = Queues;

function __range__(left, right, inclusive) {
  let range = [];
  let ascending = left < right;
  let end = !inclusive ? right : ascending ? right + 1 : right - 1;
  for (let i = left; ascending ? i < end : i > end; ascending ? i++ : i--) {
    range.push(i);
  }
  return range;
}