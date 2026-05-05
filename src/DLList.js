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
    this.incr?.();
    const node = { value, prev: this._last, next: null };
    if (this._last != null) {
      this._last.next = node;
      this._last = node;
    } else {
      this._first = this._last = node;
    }
  }
  shift() {
    if (this._first == null) {
      return;
    } else {
      this.length--;
      this.decr?.();
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
    return this._first?.value;
  }
  getArray() {
    let node = this._first;
    const result = [];
    while (node != null) {
      let ref;
      result.push(((ref = node), (node = node.next), ref.value));
    }
    return result;
  }
  forEachShift(cb) {
    let node = this.shift();
    while (node != null) {
      cb(node);
      node = this.shift();
    }
  }
  debug() {
    let node = this._first;
    const result = [];
    while (node != null) {
      let ref;
      result.push(
        ((ref = node),
        (node = node.next),
        {
          value: ref.value,
          prev: ref.prev?.value,
          next: ref.next?.value,
        }),
      );
    }
    return result;
  }
}

module.exports = DLList;
