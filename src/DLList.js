// TODO: This file was created by bulk-decaffeinate.
// Sanity-check the conversion and remove this comment.
/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/main/docs/suggestions.md
 */
class DLList {
  constructor(incr, decr) {
    this.incr = incr;
    this.decr = decr;
    this._first = null;
    this._last = null;
    this.length = 0;
  }
  push(value) {
    this.length++;
    if (typeof this.incr === "function") {
      this.incr();
    }
    const node = { value, prev: this._last, next: null };
    if (this._last != null) {
      this._last.next = node;
      this._last = node;
    } else {
      this._first = this._last = node;
    }
    return undefined;
  }
  shift() {
    if (this._first == null) {
      return;
    } else {
      this.length--;
      if (typeof this.decr === "function") {
        this.decr();
      }
    }
    const { value } = this._first;
    if ((this._first = this._first.next) != null) {
      this._first.prev = null;
    } else {
      this._last = null;
    }
    return value;
  }
  first() {
    if (this._first != null) {
      return this._first.value;
    }
  }
  getArray() {
    let node = this._first;
    return (() => {
      const result = [];
      while (node != null) {
        var ref;
        result.push(((ref = node), (node = node.next), ref.value));
      }
      return result;
    })();
  }
  forEachShift(cb) {
    let node = this.shift();
    while (node != null) {
      cb(node);
      node = this.shift();
    }
    return undefined;
  }
  debug() {
    let node = this._first;
    return (() => {
      const result = [];
      while (node != null) {
        var ref;
        result.push(
          ((ref = node),
          (node = node.next),
          {
            value: ref.value,
            prev: ref.prev != null ? ref.prev.value : undefined,
            next: ref.next != null ? ref.next.value : undefined,
          }),
        );
      }
      return result;
    })();
  }
}

module.exports = DLList;
