import DLList from "./DLList";

type QueuedTask = {
  task: (...args: unknown[]) => unknown;
  args: unknown[] | null;
  resolve: (value: unknown) => void;
  reject: (reason?: unknown) => void;
};

class Sync {
  schedule: <R, A extends unknown[]>(task: (...args: A) => R, ...args: A) => Promise<R>;
  name: string;
  /** @internal */
  /** @internal */
  _running = 0;
  /** @internal */
  /** @internal */
  _queue: DLList<QueuedTask>;

  constructor(name: string) {
    this.schedule = this._schedule.bind(this);
    this.name = name;
    this._queue = new DLList<QueuedTask>();
  }
  isEmpty(): boolean {
    return this._queue.length === 0;
  }
  /** @internal */
  async _tryToRun(): Promise<void> {
    if (this._running < 1 && this._queue.length > 0) {
      this._running++;
      const { task, args, resolve, reject } = this._queue.shift() as QueuedTask;
      let cb: () => void;
      try {
        const returned = await task(...(args ?? []));
        cb = () => resolve(returned);
      } catch (error) {
        cb = () => reject(error);
      }
      this._running--;
      this._tryToRun();
      cb();
    }
  }
  /** @internal */
  _schedule<R, A extends unknown[]>(task: (...args: A) => R, ...args: A): Promise<R> {
    let reject: (reason?: unknown) => void;
    let resolve: (value: R) => void = (reject = null as never);
    const promise = new Promise<R>((_resolve, _reject) => {
      resolve = _resolve;
      reject = _reject;
    });
    this._queue.push({ task, args, resolve, reject } as unknown as QueuedTask);
    this._tryToRun();
    return promise;
  }
}

export default Sync;
