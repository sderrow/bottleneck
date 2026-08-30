const BottleneckError = require("./BottleneckError");
class States {
  constructor(status) {
    this.status = status;
    this._jobs = {};
    this.counts = this.status.map(() => 0);
  }

  next(id) {
    const current = this._jobs[id];
    const next = current + 1;
    if (current != null && next < this.status.length) {
      this.counts[current]--;
      this.counts[next]++;
      this._jobs[id]++;
    } else if (current != null) {
      this.counts[current]--;
      delete this._jobs[id];
    }
  }

  start(id) {
    const initial = 0;
    this._jobs[id] = initial;
    return this.counts[initial]++;
  }

  remove(id) {
    const current = this._jobs[id];
    if (current != null) {
      this.counts[current]--;
      delete this._jobs[id];
    }
    return current != null;
  }

  jobStatus(id) {
    return this.status[this._jobs[id]] ?? null;
  }

  statusJobs(status) {
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

  statusCounts() {
    return this.counts.reduce((acc, v, i) => {
      acc[this.status[i]] = v;
      return acc;
    }, {});
  }
}

module.exports = States;
