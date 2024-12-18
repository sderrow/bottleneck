// TODO: This file was created by bulk-decaffeinate.
// Fix any style issues and re-enable lint.
/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * DS103: Rewrite code to no longer use __guard__, or convert again using --optional-chaining
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/main/docs/suggestions.md
 */
const parser = require("./parser");
const BottleneckError = require("./BottleneckError");

class LocalDatastore {
  constructor(instance, storeOptions, storeInstanceOptions) {
    this.instance = instance;
    this.storeOptions = storeOptions;
    this.clientId = this.instance._randomIndex();
    parser.load(storeInstanceOptions, storeInstanceOptions, this);
    this._nextRequest = this._lastReservoirRefresh = this._lastReservoirIncrease = Date.now();
    this._running = 0;
    this._done = 0;
    this._unblockTime = 0;
    this.ready = Promise.resolve();
    this.clients = {};
    this._startHeartbeat();
  }

  _startHeartbeat() {
    if (
      this.heartbeat == null &&
      ((this.storeOptions.reservoirRefreshInterval != null &&
        this.storeOptions.reservoirRefreshAmount != null) ||
        (this.storeOptions.reservoirIncreaseInterval != null &&
          this.storeOptions.reservoirIncreaseAmount != null))
    ) {
      return __guardMethod__(
        (this.heartbeat = setInterval(
          () => {
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
              const incr = maximum != null ? Math.min(amount, maximum - reservoir) : amount;
              if (incr > 0) {
                this.storeOptions.reservoir += incr;
                return this.instance._drainAll(this.computeCapacity());
              }
            }
          },

          this.heartbeatInterval,
        )),
        "unref",
        (o) => o.unref(),
      );
    } else {
      return clearInterval(this.heartbeat);
    }
  }

  async __publish__(message) {
    await this.yieldLoop();
    return this.instance.Events.trigger("message", message.toString());
  }

  async __disconnect__() {
    await this.yieldLoop();
    clearInterval(this.heartbeat);
  }

  yieldLoop(t) {
    return new Promise((resolve) => setTimeout(resolve, t ?? 0));
  }

  computePenalty() {
    return this.storeOptions.penalty != null
      ? this.storeOptions.penalty
      : 15 * this.storeOptions.minTime || 5000;
  }

  __updateSettings__(options) {
    await(this.yieldLoop());
    parser.overwrite(options, options, this.storeOptions);
    this._startHeartbeat();
    this.instance._drainAll(this.computeCapacity());
    return true;
  }

  __running__() {
    await(this.yieldLoop());
    return this._running;
  }

  __queued__() {
    await(this.yieldLoop());
    return this.instance.queued();
  }

  __done__() {
    await(this.yieldLoop());
    return this._done;
  }

  __groupCheck__(time) {
    await(this.yieldLoop());
    return this._nextRequest + this.timeout < time;
  }

  computeCapacity() {
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

  conditionsCheck(weight) {
    const capacity = this.computeCapacity();
    return capacity == null || weight <= capacity;
  }

  __incrementReservoir__(incr) {
    await(this.yieldLoop());
    const reservoir = (this.storeOptions.reservoir += incr);
    this.instance._drainAll(this.computeCapacity());
    return reservoir;
  }

  __currentReservoir__() {
    await(this.yieldLoop());
    return this.storeOptions.reservoir;
  }

  isBlocked(now) {
    return this._unblockTime >= now;
  }

  check(weight, now) {
    return this.conditionsCheck(weight) && this._nextRequest - now <= 0;
  }

  __check__(weight) {
    await(this.yieldLoop());
    const now = Date.now();
    return this.check(weight, now);
  }

  __register__(index, weight, expiration) {
    await(this.yieldLoop());
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

  strategyIsBlock() {
    return this.storeOptions.strategy === 3;
  }

  __submit__(queueLength, weight) {
    await(this.yieldLoop());
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

  __free__(index, weight) {
    await(this.yieldLoop());
    this._running -= weight;
    this._done += weight;
    this.instance._drainAll(this.computeCapacity());
    return { running: this._running };
  }
}

module.exports = LocalDatastore;

function __guardMethod__(obj, methodName, transform) {
  if (typeof obj !== "undefined" && obj !== null && typeof obj[methodName] === "function") {
    return transform(obj, methodName);
  } else {
    return undefined;
  }
}
