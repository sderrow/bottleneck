import type Bottleneck from "./Bottleneck";
import type { StoreOptions } from "./types";
import BottleneckError from "./BottleneckError";
import { load, overwrite } from "./parser";
import sleep from "./sleep";

type LocalStoreInstanceOptions = {
  Promise: PromiseConstructor;
  timeout: number | null;
  heartbeatInterval: number;
};

class LocalDatastore {
  instance: Bottleneck;
  storeOptions: StoreOptions;
  Promise: PromiseConstructor = Promise;
  timeout: number | null = null;
  heartbeatInterval: number = 250;
  heartbeat: ReturnType<typeof setInterval> | undefined;
  _disconnecting = false;
  clientId: string;
  _nextRequest: number;
  _lastReservoirRefresh: number;
  _lastReservoirIncrease: number;
  _running = 0;
  _done = 0;
  _unblockTime = 0;
  ready: Promise<unknown> = Promise.resolve();
  clients: Record<string, unknown> = {};

  constructor(
    instance: Bottleneck,
    storeOptions: StoreOptions,
    storeInstanceOptions: Partial<LocalStoreInstanceOptions>,
  ) {
    this.instance = instance;
    this.storeOptions = storeOptions;
    this.clientId = this.instance._randomIndex();
    load(storeInstanceOptions, storeInstanceOptions as object, this);
    this._nextRequest = this._lastReservoirRefresh = this._lastReservoirIncrease = Date.now();
    this._running = 0;
    this._done = 0;
    this._unblockTime = 0;
    this.ready = Promise.resolve();
    this.clients = {};
    this._startHeartbeat();
  }

  _startHeartbeat(): void {
    if (this.heartbeat) {
      clearInterval(this.heartbeat);
    }

    if (
      (this.storeOptions.reservoirRefreshInterval != null &&
        this.storeOptions.reservoirRefreshAmount != null) ||
      (this.storeOptions.reservoirIncreaseInterval != null &&
        this.storeOptions.reservoirIncreaseAmount != null)
    ) {
      this.heartbeat = setInterval(() => {
        const now = Date.now();
        if (
          this.storeOptions.reservoirRefreshInterval != null &&
          now >= this._lastReservoirRefresh + this.storeOptions.reservoirRefreshInterval
        ) {
          this._lastReservoirRefresh = now;
          this.storeOptions.reservoir = this.storeOptions.reservoirRefreshAmount;
          this.instance._drainAll(this.computeCapacity());
        }

        if (
          this.storeOptions.reservoirIncreaseInterval != null &&
          now >= this._lastReservoirIncrease + this.storeOptions.reservoirIncreaseInterval
        ) {
          const {
            reservoirIncreaseAmount: amount,
            reservoirIncreaseMaximum: maximum,
            reservoir,
          } = this.storeOptions;
          this._lastReservoirIncrease = now;
          const incr =
            maximum != null && amount != null
              ? Math.min(amount, maximum - (reservoir ?? 0))
              : amount;
          if (incr != null && incr > 0) {
            this.storeOptions.reservoir = (this.storeOptions.reservoir ?? 0) + incr;
            this.instance._drainAll(this.computeCapacity());
          }
        }
      }, this.heartbeatInterval).unref?.();
    }
  }

  async __publish__(message: string): Promise<unknown> {
    await this.yieldLoop();
    return this.instance.Events.trigger("message", message.toString());
  }

  async __disconnect__(_flush?: boolean): Promise<void> {
    await this.yieldLoop();
    clearInterval(this.heartbeat);
  }

  yieldLoop(t?: number): Promise<void> {
    return sleep(t ?? 0);
  }

  computePenalty(): number {
    return this.storeOptions.penalty != null
      ? this.storeOptions.penalty
      : 15 * this.storeOptions.minTime || 5000;
  }

  async __updateSettings__(options: StoreOptions): Promise<boolean> {
    await this.yieldLoop();
    overwrite(options, options, this.storeOptions);
    this._startHeartbeat();
    this.instance._drainAll(this.computeCapacity());
    return true;
  }

  async __running__(): Promise<number> {
    await this.yieldLoop();
    return this._running;
  }

  async __queued__(): Promise<number> {
    await this.yieldLoop();
    return this.instance.queued();
  }

  async __done__(): Promise<number> {
    await this.yieldLoop();
    return this._done;
  }

  async __groupCheck__(time: number): Promise<boolean> {
    await this.yieldLoop();
    return this._nextRequest + (this.timeout ?? 0) < time;
  }

  computeCapacity(): number | null {
    const { maxConcurrent, reservoir } = this.storeOptions;
    if (maxConcurrent != null && reservoir != null) {
      return Math.min(maxConcurrent - this._running, reservoir);
    } else if (maxConcurrent != null) {
      return maxConcurrent - this._running;
    } else if (reservoir != null) {
      return reservoir;
    } else {
      return null;
    }
  }

  conditionsCheck(weight: number): boolean {
    const capacity = this.computeCapacity();
    return capacity == null || weight <= capacity;
  }

  async __incrementReservoir__(incr: number): Promise<number | null> {
    await this.yieldLoop();
    this.storeOptions.reservoir = (this.storeOptions.reservoir ?? 0) + incr;
    const reservoir = this.storeOptions.reservoir;
    this.instance._drainAll(this.computeCapacity());
    return reservoir;
  }

  async __currentReservoir__(): Promise<number | null> {
    await this.yieldLoop();
    return this.storeOptions.reservoir;
  }

  isBlocked(now: number): boolean {
    return this._unblockTime >= now;
  }

  check(weight: number, now: number): boolean {
    return this.conditionsCheck(weight) && this._nextRequest - now <= 0;
  }

  async __check__(weight: number): Promise<boolean> {
    await this.yieldLoop();
    const now = Date.now();
    return this.check(weight, now);
  }

  async __register__(
    index: string,
    weight: number,
    _expiration: number | null,
  ): Promise<{ success: boolean; wait?: number; reservoir?: number | null }> {
    await this.yieldLoop();
    const now = Date.now();
    if (this.conditionsCheck(weight)) {
      this._running += weight;
      if (this.storeOptions.reservoir != null) {
        this.storeOptions.reservoir -= weight;
      }
      const wait = Math.max(this._nextRequest - now, 0);
      this._nextRequest = now + wait + this.storeOptions.minTime;
      return { success: true, wait, reservoir: this.storeOptions.reservoir };
    } else {
      return { success: false };
    }
  }

  strategyIsBlock(): boolean {
    return this.storeOptions.strategy === 3;
  }

  async __submit__(
    queueLength: number,
    weight: number,
  ): Promise<{
    reachedHWM: boolean;
    blocked: boolean;
    strategy: number | null;
  }> {
    await this.yieldLoop();
    if (this.storeOptions.maxConcurrent != null && weight > this.storeOptions.maxConcurrent) {
      throw new BottleneckError(
        `Impossible to add a job having a weight of ${weight} to a limiter having a maxConcurrent setting of ${this.storeOptions.maxConcurrent}`,
      );
    }
    const now = Date.now();
    const reachedHWM =
      this.storeOptions.highWater != null &&
      queueLength === this.storeOptions.highWater &&
      !this.check(weight, now);
    const blocked = this.strategyIsBlock() && (reachedHWM || this.isBlocked(now));
    if (blocked) {
      this._unblockTime = now + this.computePenalty();
      this._nextRequest = this._unblockTime + this.storeOptions.minTime;
      this.instance._dropAllQueued();
    }
    return { reachedHWM, blocked, strategy: this.storeOptions.strategy };
  }

  async __free__(index: string, weight: number): Promise<{ running: number }> {
    await this.yieldLoop();
    this._running -= weight;
    this._done += weight;
    this.instance._drainAll(this.computeCapacity());
    return { running: this._running };
  }
}

export default LocalDatastore;
