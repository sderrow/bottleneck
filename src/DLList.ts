export type Node<T> = {
  value: T;
  prev: Node<T> | null;
  next: Node<T> | null;
};

class DLList<T = unknown> {
  incr?: () => void;
  decr?: () => void;
  /** @internal */
  _first: Node<T> | null = null;
  /** @internal */
  _last: Node<T> | null = null;
  length = 0;

  constructor(incr?: () => void, decr?: () => void) {
    this.incr = incr;
    this.decr = decr;
  }
  push(value: T): Node<T> {
    this.length++;
    this.incr?.();
    const node: Node<T> = { value, prev: this._last, next: null };
    if (this._last != null) {
      this._last.next = node;
      this._last = node;
    } else {
      this._first = this._last = node;
    }
    return node;
  }
  /** Unlink `node` in O(1). False if it isn't in this list (already shifted or removed). */
  remove(node: Node<T>): boolean {
    if (node.prev != null ? node.prev.next !== node : this._first !== node) {
      return false;
    }
    if (node.prev != null) {
      node.prev.next = node.next;
    } else {
      this._first = node.next;
    }
    if (node.next != null) {
      node.next.prev = node.prev;
    } else {
      this._last = node.prev;
    }
    node.prev = node.next = null;
    this.length--;
    this.decr?.();
    return true;
  }
  shift(): T | undefined {
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
  first(): T | undefined {
    return this._first?.value;
  }
  getArray(): T[] {
    let node = this._first;
    const result: T[] = [];
    while (node != null) {
      let ref;
      result.push(((ref = node), (node = node.next), ref.value));
    }
    return result;
  }
  forEachShift(cb: (value: T) => void): void {
    let node = this.shift();
    while (node != null) {
      cb(node);
      node = this.shift();
    }
  }
  debug(): unknown[] {
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

export default DLList;
