import { describe, it, expect } from "vitest";
import DLList from "../src/DLList";

class FakeQueues {
  _length = 0;
  incr = () => this._length++;
  decr = () => this._length--;
  fns = [this.incr, this.decr] as [() => void, () => void];
}

describe("DLList", () => {
  it("Should be created and be empty", () => {
    const list = new DLList();
    expect(list.getArray().length).toStrictEqual(0);
  });

  it("Should be possible to append once", () => {
    const queues = new FakeQueues();
    const list = new DLList(...queues.fns);
    list.push(5);
    const arr = list.getArray();
    expect(arr.length).toStrictEqual(1);
    expect(list.length).toStrictEqual(1);
    expect(queues._length).toStrictEqual(1);
    expect(arr[0]).toStrictEqual(5);
  });

  it("Should be possible to append multiple times", () => {
    const queues = new FakeQueues();
    const list = new DLList(...queues.fns);
    list.push(5);
    list.push(6);
    let arr = list.getArray();
    expect(arr.length).toStrictEqual(2);
    expect(list.length).toStrictEqual(2);
    expect(queues._length).toStrictEqual(2);
    expect(arr[0]).toStrictEqual(5);
    expect(arr[1]).toStrictEqual(6);

    list.push(10);

    arr = list.getArray();
    expect(arr.length).toStrictEqual(3);
    expect(list.length).toStrictEqual(3);
    expect(arr[0]).toStrictEqual(5);
    expect(arr[1]).toStrictEqual(6);
    expect(arr[2]).toStrictEqual(10);
  });

  it("Should be possible to shift an empty list", () => {
    const queues = new FakeQueues();
    const list = new DLList(...queues.fns);
    expect(list.length).toStrictEqual(0);
    expect(list.shift()).toStrictEqual(undefined);
    let arr = list.getArray();
    expect(arr.length).toStrictEqual(0);
    expect(list.length).toStrictEqual(0);
    expect(list.shift()).toStrictEqual(undefined);
    arr = list.getArray();
    expect(arr.length).toStrictEqual(0);
    expect(list.length).toStrictEqual(0);
    expect(queues._length).toStrictEqual(0);
  });

  it("Should be possible to append then shift once", () => {
    const queues = new FakeQueues();
    const list = new DLList(...queues.fns);
    list.push(5);
    expect(list.length).toStrictEqual(1);
    expect(list.shift()).toStrictEqual(5);
    const arr = list.getArray();
    expect(arr.length).toStrictEqual(0);
    expect(list.length).toStrictEqual(0);
    expect(queues._length).toStrictEqual(0);
  });

  it("Should be possible to append then shift multiple times", () => {
    const queues = new FakeQueues();
    const list = new DLList(...queues.fns);
    list.push(5);
    expect(list.length).toStrictEqual(1);
    expect(list.shift()).toStrictEqual(5);
    expect(list.length).toStrictEqual(0);

    list.push(6);
    expect(list.length).toStrictEqual(1);
    expect(list.shift()).toStrictEqual(6);
    expect(list.length).toStrictEqual(0);
    expect(queues._length).toStrictEqual(0);
  });

  it("Should expose debug information for each node", () => {
    const queues = new FakeQueues();
    const list = new DLList(...queues.fns);
    list.push(1);
    list.push(2);
    expect(list.debug()).toStrictEqual([
      { value: 1, prev: undefined, next: 2 },
      { value: 2, prev: 1, next: undefined },
    ]);
  });

  it("Should pass a full test", () => {
    const queues = new FakeQueues();
    const list = new DLList(...queues.fns);
    list.push(10);
    expect(list.length).toStrictEqual(1);
    list.push("11");
    expect(list.length).toStrictEqual(2);
    list.push(12);
    expect(list.length).toStrictEqual(3);
    expect(queues._length).toStrictEqual(3);

    expect(list.shift()).toStrictEqual(10);
    expect(list.length).toStrictEqual(2);
    expect(list.shift()).toStrictEqual("11");
    expect(list.length).toStrictEqual(1);

    list.push(true);
    expect(list.length).toStrictEqual(2);

    const arr = list.getArray();
    expect(arr[0]).toStrictEqual(12);
    expect(arr[1]).toStrictEqual(true);
    expect(arr.length).toStrictEqual(2);
    expect(queues._length).toStrictEqual(2);
  });

  it("Should return the first value without shifting", () => {
    const queues = new FakeQueues();
    const list = new DLList(...queues.fns);
    expect(list.first()).toStrictEqual(undefined);
    expect(list.first()).toStrictEqual(undefined);

    list.push(1);
    expect(list.first()).toStrictEqual(1);
    expect(list.first()).toStrictEqual(1);

    list.push(2);
    expect(list.first()).toStrictEqual(1);
    expect(list.first()).toStrictEqual(1);

    expect(list.shift()).toStrictEqual(1);
    expect(list.first()).toStrictEqual(2);
    expect(list.first()).toStrictEqual(2);

    expect(list.shift()).toStrictEqual(2);
    expect(list.first()).toStrictEqual(undefined);
    expect(list.first()).toStrictEqual(undefined);

    expect(list.first()).toStrictEqual(undefined);
    expect(list.shift()).toStrictEqual(undefined);
    expect(list.first()).toStrictEqual(undefined);
  });
});
