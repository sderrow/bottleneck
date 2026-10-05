import type { Counts, Status } from "./types";
import BottleneckError from "./BottleneckError";

const STATUSES = ["RECEIVED", "QUEUED", "RUNNING", "EXECUTING", "DONE"] as const;
type Pos = 0 | 1 | 2 | 3 | 4;
const NEXT = [1, 2, 3, 4, null] as const;
const EXECUTING = 3;
const DONE = 4;

class States {
  /** @internal */
  _jobs: Record<string, Pos> = {};
  counts: [number, number, number, number, number] = [0, 0, 0, 0, 0];
  /** @internal The last status a job passes through before it's forgotten. */
  _final: Pos;

  constructor(trackDone: boolean) {
    this._final = trackDone ? DONE : EXECUTING;
  }

  next(id: string): void {
    const current = this._jobs[id];
    if (current == null) return;
    const next = NEXT[current];
    this.counts[current]--;
    if (next != null && next <= this._final) {
      this.counts[next]++;
      this._jobs[id] = next;
    } else {
      delete this._jobs[id];
    }
  }

  start(id: string): number {
    this._jobs[id] = 0;
    return this.counts[0]++;
  }

  remove(id: string): boolean {
    const current = this._jobs[id];
    if (current != null) {
      this.counts[current]--;
      delete this._jobs[id];
    }
    return current != null;
  }

  jobStatus(id: string): Status | null {
    const pos = this._jobs[id];
    return pos != null ? STATUSES[pos] : null;
  }

  statusJobs(status?: Status): string[] {
    if (status != null) {
      const pos = STATUSES.indexOf(status);
      if (pos < 0 || pos > this._final) {
        const valid = STATUSES.slice(0, this._final + 1);
        throw new BottleneckError(`status must be one of ${valid.join(", ")}`);
      }
      const result = [];
      for (const [k, v] of Object.entries(this._jobs)) {
        if (v === pos) {
          result.push(k);
        }
      }
      return result;
    } else {
      return Object.keys(this._jobs);
    }
  }

  statusCounts(): Counts {
    const [received, queued, running, executing, done] = this.counts;
    const counts: Counts = {
      RECEIVED: received,
      QUEUED: queued,
      RUNNING: running,
      EXECUTING: executing,
    };
    if (this._final === DONE) {
      counts.DONE = done;
    }
    return counts;
  }
}

export default States;
