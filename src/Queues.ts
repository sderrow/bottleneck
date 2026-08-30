import type Job from "./Job";
import DLList from "./DLList";
import Events from "./Events";

type JobLike = { options: { priority: number } };

class Queues<J extends JobLike = Job> {
  Events: Events;
  _length = 0;
  _lists: DLList<J>[];

  // Installed on the instance by Events (see Events constructor).
  declare on: (name: string, cb: (...args: any[]) => void) => unknown;
  declare once: (name: string, cb: (...args: any[]) => void) => unknown;
  declare removeAllListeners: (name?: string | null) => void;

  constructor(num_priorities: number) {
    this.Events = new Events(this);
    this._lists = [];
    for (let i = 0; i < num_priorities; i++) {
      const list = new DLList<J>(
        () => this.incr(),
        () => this.decr(),
      );
      this._lists.push(list);
    }
  }

  incr(): Promise<unknown> | undefined {
    if (this._length++ === 0) {
      return this.Events.trigger("leftzero");
    }
  }

  decr(): Promise<unknown> | undefined {
    if (--this._length === 0) {
      return this.Events.trigger("zero");
    }
  }

  push(job: J): void {
    this._lists[job.options.priority]!.push(job);
  }

  queued(priority?: number): number {
    if (priority != null) {
      return this._lists[priority]?.length ?? 0;
    } else {
      return this._length;
    }
  }

  shiftAll(fn: (job: J) => void): void {
    this._lists.forEach((list) => list.forEachShift(fn));
  }

  getFirst(arr?: DLList<J>[]): DLList<J> {
    for (const list of arr ?? this._lists) {
      if (list.length > 0) return list;
    }
    return [] as unknown as DLList<J>;
  }

  shiftLastFrom(priority: number): J | undefined {
    return this.getFirst(this._lists.slice(priority).reverse()).shift();
  }
}

export default Queues;
