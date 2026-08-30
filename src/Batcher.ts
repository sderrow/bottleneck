import Events from "./Events";
import { load } from "./parser";

type BatcherDefaults = {
  maxTime: number | null;
  maxSize: number | null;
};

class Batcher {
  defaults: BatcherDefaults = { maxTime: null, maxSize: null };
  maxTime: number | null = null;
  maxSize: number | null = null;
  options: object;
  Events: Events;
  _arr: unknown[];
  _timeout: ReturnType<typeof setTimeout> | undefined;
  _lastFlush: number;
  _promise: Promise<unknown> = null as never;
  _resolve: (value: unknown) => void = null as never;

  constructor(options?: object) {
    this.options = options ?? {};
    load(this.options, this.defaults, this);
    this.Events = new Events(this);
    this._arr = [];
    this._resetPromise();
    this._lastFlush = Date.now();
  }

  _resetPromise(): void {
    this._promise = new Promise((res) => {
      this._resolve = res;
    });
  }

  _flush(): void {
    clearTimeout(this._timeout);
    this._lastFlush = Date.now();
    this._resolve(undefined);
    this.Events.trigger("batch", this._arr);
    this._arr = [];
    this._resetPromise();
  }

  add(data: unknown): Promise<unknown> {
    this._arr.push(data);
    const existingPromise = this._promise;
    if (this.maxSize != null && this._arr.length === this.maxSize) {
      this._flush();
    } else if (this.maxTime != null && this._arr.length === 1) {
      this._timeout = setTimeout(() => {
        this._flush();
      }, this.maxTime);
    }
    return existingPromise;
  }
}

export default Batcher;
