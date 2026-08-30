import type Bottleneck from "./Bottleneck";
import type Events from "./Events";
import type States from "./States";
import type { EventInfo, EventInfoRetryable, JobDefaults, ResolvedJobOptions } from "./types";
import BottleneckError from "./BottleneckError";
import { load } from "./parser";
import randomIndex from "./random-index";

const NUM_PRIORITIES = 10;
const DEFAULT_PRIORITY = 5;

class Job {
  task: (...args: never[]) => unknown;
  args: never[] | null;
  rejectOnDrop: boolean;
  Events: Events;
  _states: States;
  options: ResolvedJobOptions;
  promise: Promise<unknown>;
  retryCount = 0;
  _resolve: (value: unknown) => void = null as never;
  _reject: (reason?: unknown) => void = null as never;

  constructor(
    task: (...args: never[]) => unknown,
    args: never[] | null,
    options: object | null | undefined,
    jobDefaults: JobDefaults,
    rejectOnDrop: boolean,
    Events: Events,
    _states: States,
  ) {
    this.task = task;
    this.args = args;
    this.rejectOnDrop = rejectOnDrop;
    this.Events = Events;
    this._states = _states;
    this.options = load(options ?? {}, jobDefaults) as ResolvedJobOptions;
    this.options.priority = this._sanitizePriority(this.options.priority);
    if (this.options.id === jobDefaults.id) {
      this.options.id = `${this.options.id}-${randomIndex()}`;
    }
    this.promise = new Promise((_resolve, _reject) => {
      this._resolve = _resolve;
      this._reject = _reject;
    });
    this.retryCount = 0;
  }

  _sanitizePriority(priority: unknown): number {
    const sProperty = ~~(priority as number) !== priority ? DEFAULT_PRIORITY : (priority as number);
    if (sProperty < 0) {
      return 0;
    } else if (sProperty > NUM_PRIORITIES - 1) {
      return NUM_PRIORITIES - 1;
    } else {
      return sProperty;
    }
  }

  doDrop(params?: { error?: unknown; message?: string }): boolean {
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

  _assertStatus(expected: string): void {
    const status = this._states.jobStatus(this.options.id);
    if (!(status === expected || (expected === "DONE" && status === null))) {
      throw new BottleneckError(
        `Invalid job status ${status}, expected ${expected}. Please open an issue at https://github.com/SGrondin/bottleneck/issues`,
      );
    }
  }

  doReceive(): Promise<unknown> | undefined {
    this._states.start(this.options.id);
    return this.Events.trigger("received", { args: this.args, options: this.options });
  }

  doQueue(reachedHWM: boolean, blocked: boolean): Promise<unknown> | undefined {
    this._assertStatus("RECEIVED");
    this._states.next(this.options.id);
    return this.Events.trigger("queued", {
      args: this.args,
      options: this.options,
      reachedHWM,
      blocked,
    });
  }

  doRun(): Promise<unknown> | undefined {
    if (this.retryCount === 0) {
      this._assertStatus("QUEUED");
      this._states.next(this.options.id);
    } else {
      this._assertStatus("EXECUTING");
    }
    return this.Events.trigger("scheduled", { args: this.args, options: this.options });
  }

  async doExecute(
    chained: Bottleneck | null,
    clearGlobalState: () => boolean,
    run: (retryAfter: number) => unknown,
    free: (options: ResolvedJobOptions, eventInfo: EventInfo) => Promise<unknown>,
  ): Promise<unknown> {
    if (this.retryCount === 0) {
      this._assertStatus("RUNNING");
      this._states.next(this.options.id);
    } else {
      this._assertStatus("EXECUTING");
    }
    const eventInfo: EventInfoRetryable = {
      args: this.args ?? [],
      options: this.options as unknown as EventInfo["options"],
      retryCount: this.retryCount,
    };
    this.Events.trigger("executing", eventInfo);

    try {
      const passed = await (chained != null
        ? chained.schedule(this.options as never, this.task as never, ...(this.args ?? []))
        : (this.task as (...args: never[]) => unknown)(...(this.args ?? [])));

      if (clearGlobalState()) {
        this.doDone(eventInfo);
        await free(this.options, eventInfo);
        this._assertStatus("DONE");
        this._resolve(passed);
      }
    } catch (error) {
      return this._onFailure(error, eventInfo, clearGlobalState, run, free);
    }
  }

  doExpire(
    clearGlobalState: () => boolean,
    run: (retryAfter: number) => unknown,
    free: (options: ResolvedJobOptions, eventInfo: EventInfo) => Promise<unknown>,
  ): Promise<unknown> | undefined {
    if (this._states.jobStatus(this.options.id) === "RUNNING") {
      this._states.next(this.options.id);
    }
    this._assertStatus("EXECUTING");
    const eventInfo: EventInfoRetryable = {
      args: this.args ?? [],
      options: this.options as unknown as EventInfo["options"],
      retryCount: this.retryCount,
    };
    const error = new BottleneckError(`This job timed out after ${this.options.expiration} ms.`);
    return this._onFailure(error, eventInfo, clearGlobalState, run, free);
  }

  async _onFailure(
    error: unknown,
    eventInfo: EventInfo,
    clearGlobalState: () => boolean,
    run: (retryAfter: number) => unknown,
    free: (options: ResolvedJobOptions, eventInfo: EventInfo) => Promise<unknown>,
  ): Promise<unknown> {
    if (clearGlobalState()) {
      const retry = await this.Events.trigger("failed", error, eventInfo);
      if (retry != null) {
        const retryAfter = ~~(retry as number);
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

  doDone(eventInfo: EventInfo): Promise<unknown> | undefined {
    this._assertStatus("EXECUTING");
    this._states.next(this.options.id);
    return this.Events.trigger("done", eventInfo);
  }
}

export default Job;
