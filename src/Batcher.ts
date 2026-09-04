import type { BatcherEvents, BatcherOptions } from "./types";
import Events from "./Events";
import { load } from "./parser";

type BatcherDefaults = {
  /** @internal */
  maxTime: number | null;
  /** @internal */
  maxSize: number | null;
};

class Batcher<T = any> {
  /** @internal */
  defaults: BatcherDefaults = { maxTime: null, maxSize: null };
  /** @internal */
  maxTime: number | null = null;
  /** @internal */
  maxSize: number | null = null;
  /** @internal */
  options: BatcherOptions;
  /** @internal */
  Events: Events;
  /** @internal */
  /** @internal */
  _arr: T[];
  /** @internal */
  /** @internal */
  _timeout: ReturnType<typeof setTimeout> | undefined;
  /** @internal */
  /** @internal */
  _lastFlush: number;
  /** @internal */
  /** @internal */
  _promise: Promise<void> = null as never;
  /** @internal */
  /** @internal */
  _resolve: (value: void) => void = null as never;

  // Installed on the instance by Events (see Events constructor).
  declare on: {
    <E extends keyof BatcherEvents<T>>(event: E, listener: BatcherEvents<T>[E]): unknown;
    (event: string, listener: (...args: any[]) => unknown): unknown;
  };
  declare once: {
    <E extends keyof BatcherEvents<T>>(event: E, listener: BatcherEvents<T>[E]): unknown;
    (event: string, listener: (...args: any[]) => unknown): unknown;
  };
  declare removeAllListeners: (name?: string | null) => void;

  constructor(options?: BatcherOptions) {
    this.options = options ?? {};
    load(this.options, this.defaults, this);
    this.Events = new Events(this);
    this._arr = [];
    this._resetPromise();
    this._lastFlush = Date.now();
  }

  /** @internal */
  /** @internal */
  _resetPromise(): void {
    this._promise = new Promise((res) => {
      this._resolve = res;
    });
  }

  /** @internal */
  /** @internal */
  _flush(): void {
    clearTimeout(this._timeout);
    this._lastFlush = Date.now();
    this._resolve(undefined);
    this.Events.trigger("batch", this._arr);
    this._arr = [];
    this._resetPromise();
  }

  add(data: T): Promise<void> {
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
