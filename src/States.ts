import BottleneckError from "./BottleneckError";

class States {
  status: string[];
  /** @internal */
  /** @internal */
  _jobs: Record<string, number> = {};
  counts: number[];

  constructor(status: string[]) {
    this.status = status;
    this.counts = this.status.map(() => 0);
  }

  next(id: string): void {
    const current = this._jobs[id];
    if (current == null) return;
    const next = current + 1;
    if (next < this.status.length) {
      this.counts[current]!--;
      this.counts[next]!++;
      this._jobs[id] = next;
    } else {
      this.counts[current]!--;
      delete this._jobs[id];
    }
  }

  start(id: string): number {
    const initial = 0;
    this._jobs[id] = initial;
    return this.counts[initial]!++;
  }

  remove(id: string): boolean {
    const current = this._jobs[id];
    if (current != null) {
      this.counts[current]!--;
      delete this._jobs[id];
    }
    return current != null;
  }

  jobStatus(id: string): string | null {
    const pos = this._jobs[id];
    return pos != null ? (this.status[pos] ?? null) : null;
  }

  statusJobs(status?: string): string[] {
    if (status != null) {
      const pos = this.status.indexOf(status);
      if (pos < 0) {
        throw new BottleneckError(`status must be one of ${this.status.join(", ")}`);
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

  statusCounts(): Record<string, number> {
    return this.counts.reduce<Record<string, number>>((acc, v, i) => {
      acc[this.status[i] as string] = v;
      return acc;
    }, {});
  }
}

export default States;
