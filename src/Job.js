const NUM_PRIORITIES = 10;
const DEFAULT_PRIORITY = 5;

const parser = require("./parser");
const BottleneckError = require("./BottleneckError");

class Job {
  constructor(task, args, options, jobDefaults, rejectOnDrop, Events, _states) {
    this.task = task;
    this.args = args;
    this.rejectOnDrop = rejectOnDrop;
    this.Events = Events;
    this._states = _states;
    this.options = parser.load(options, jobDefaults);
    this.options.priority = this._sanitizePriority(this.options.priority);
    if (this.options.id === jobDefaults.id) {
      this.options.id = `${this.options.id}-${this._randomIndex()}`;
    }
    this.promise = new Promise((_resolve, _reject) => {
      this._resolve = _resolve;
      this._reject = _reject;
    });
    this.retryCount = 0;
  }

  _sanitizePriority(priority) {
    const sProperty = ~~priority !== priority ? DEFAULT_PRIORITY : priority;
    if (sProperty < 0) {
      return 0;
    } else if (sProperty > NUM_PRIORITIES - 1) {
      return NUM_PRIORITIES - 1;
    } else {
      return sProperty;
    }
  }

  _randomIndex() {
    return Math.random().toString(36).slice(2);
  }

  doDrop(params) {
    const { error, message = "This job has been dropped by Bottleneck" } = params || {};
    if (this._states.remove(this.options.id)) {
      if (this.rejectOnDrop) {
        this._reject(error ?? new BottleneckError(message));
      }
      this.Events.trigger("dropped", {
        args: this.args,
        options: this.options,
        task: this.task,
        promise: this.promise,
      });
      return true;
    } else {
      return false;
    }
  }

  _assertStatus(expected) {
    const status = this._states.jobStatus(this.options.id);
    if (!(status === expected || (expected === "DONE" && status === null))) {
      throw new BottleneckError(
        `Invalid job status ${status}, expected ${expected}. Please open an issue at https://github.com/SGrondin/bottleneck/issues`,
      );
    }
  }

  doReceive() {
    this._states.start(this.options.id);
    return this.Events.trigger("received", { args: this.args, options: this.options });
  }

  doQueue(reachedHWM, blocked) {
    this._assertStatus("RECEIVED");
    this._states.next(this.options.id);
    return this.Events.trigger("queued", {
      args: this.args,
      options: this.options,
      reachedHWM,
      blocked,
    });
  }

  doRun() {
    if (this.retryCount === 0) {
      this._assertStatus("QUEUED");
      this._states.next(this.options.id);
    } else {
      this._assertStatus("EXECUTING");
    }
    return this.Events.trigger("scheduled", { args: this.args, options: this.options });
  }

  async doExecute(chained, clearGlobalState, run, free) {
    if (this.retryCount === 0) {
      this._assertStatus("RUNNING");
      this._states.next(this.options.id);
    } else {
      this._assertStatus("EXECUTING");
    }
    const eventInfo = { args: this.args, options: this.options, retryCount: this.retryCount };
    this.Events.trigger("executing", eventInfo);

    try {
      const passed = await (chained != null
        ? chained.schedule(this.options, this.task, ...this.args)
        : this.task(...(this.args || [])));

      if (clearGlobalState()) {
        this.doDone(eventInfo);
        await free(this.options, eventInfo);
        this._assertStatus("DONE");
        return this._resolve(passed);
      }
    } catch (error) {
      return this._onFailure(error, eventInfo, clearGlobalState, run, free);
    }
  }

  doExpire(clearGlobalState, run, free) {
    if (this._states.jobStatus(this.options.id === "RUNNING")) {
      this._states.next(this.options.id);
    }
    this._assertStatus("EXECUTING");
    const eventInfo = { args: this.args, options: this.options, retryCount: this.retryCount };
    const error = new BottleneckError(`This job timed out after ${this.options.expiration} ms.`);
    return this._onFailure(error, eventInfo, clearGlobalState, run, free);
  }

  async _onFailure(error, eventInfo, clearGlobalState, run, free) {
    if (clearGlobalState()) {
      const retry = await this.Events.trigger("failed", error, eventInfo);
      if (retry != null) {
        const retryAfter = ~~retry;
        this.Events.trigger(
          "retry",
          `Retrying ${this.options.id} after ${retryAfter} ms`,
          eventInfo,
        );
        this.retryCount++;
        return run(retryAfter);
      } else {
        this.doDone(eventInfo);
        await free(this.options, eventInfo);
        this._assertStatus("DONE");
        return this._reject(error);
      }
    }
  }

  doDone(eventInfo) {
    this._assertStatus("EXECUTING");
    this._states.next(this.options.id);
    return this.Events.trigger("done", eventInfo);
  }
}

module.exports = Job;
