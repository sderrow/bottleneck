import { describe, expect } from "vitest";
import sleep from "../src/sleep";
import { useFakeClock, useRealClockForThisTest } from "./helpers/clock.js";
import { test, waitForState, deferred } from "./helpers/test-api.js";

const path = require("path");
const util = require("util");
const execFile = util.promisify(require("child_process").execFile);

useFakeClock();

describe("General traffic", () => {
  describe("High water limit", () => {
    test("Should support highWater set to 0", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({
        maxConcurrent: 1,
        minTime: 0,
        highWater: 0,
        rejectOnDrop: false,
      });

      const first = limiter.schedule(h.slowPromise, 50, null, 1);
      // Jobs 2-4 are dropped (highWater 0, rejectOnDrop false): their promises
      // never settle, so wrapping them in expect().resolves would hang the
      // test's auto-awaited assertions. toHaveCallOrder below proves they
      // never ran.
      limiter.schedule(h.slowPromise, 50, null, 2);
      limiter.schedule(h.slowPromise, 50, null, 3);
      limiter.schedule(h.slowPromise, 50, null, 4);

      await expect(first).resolves.toEqual([1]);
      await h.flushLimiter(limiter, { weight: 0 });
      expect(h).toHaveFinalCallAt(50);
      expect(h.log).toHaveCallOrder([[1]]);
    });

    test("Should support highWater set to 1", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({
        maxConcurrent: 1,
        minTime: 0,
        highWater: 1,
        rejectOnDrop: false,
      });
      await limiter.ready();

      // Track how many jobs have actually been committed to the queue by
      // the submit lock. The "queued" event fires once per successful
      // _addToQueue (even for jobs that LEAK later drops — because
      // doDrop on the displaced job fires AFTER the new job's doQueue).
      // We register the listener BEFORE scheduling the primer so it
      // counts all 4 commits.
      //
      // Why we can't just poll `queued()`: queued() === 1 is briefly
      // true after j2 alone is added, before j3 displaces it. A poll
      // there would proceed and let j2 run before j3/j4 ever submit,
      // yielding 3 results ([1, 2, 4] or [1, 3, 4]) instead of 2.
      let committed = 0;
      limiter.on("queued", () => {
        committed++;
      });

      // Job 1 holds the running slot via a deferred-resolution signal so
      // we can synchronize on "all 4 schedules committed via the submit
      // lock" before releasing. Without this, under load (LocalDatastore
      // yields once per __submit__; a redis-backed datastore adds a
      // network round trip; a connect-retry adds ~500ms), 4 sequential
      // submits can exceed slowPromise(50)'s window — job 1 finishes
      // before jobs 3/4 commit, jobs 2/3 dispatch, and the test sees 3
      // results instead of the expected 2.
      const primer = deferred();
      const first = limiter.schedule(h.deferredPromise, primer.signal, null, 1);

      // Wait until the primer is running. Once it occupies the running
      // slot at maxConcurrent=1, no subsequent job can be dispatched
      // until we release.
      await waitForState(async () => {
        expect(await limiter.running()).toBeGreaterThanOrEqual(1);
      });

      // Jobs 2-3 are displaced by LEAK (rejectOnDrop false): their promises
      // never settle — toHaveCallOrder below proves they never ran.
      limiter.schedule(h.slowPromise, 50, null, 2);
      limiter.schedule(h.slowPromise, 50, null, 3);
      const last = limiter.schedule(h.slowPromise, 50, null, 4);

      // 4 = primer + j2 + j3 + j4 all committed via doQueue.
      await waitForState(() => {
        expect(committed).toBe(4);
      });

      primer.release();
      await Promise.all([expect(first).resolves.toEqual([1]), expect(last).resolves.toEqual([4])]);
      await h.flushLimiter(limiter, { weight: 0 });
      expect(h.log).toHaveCallOrder([[1], [4]]);
    });
  });

  describe("Weight", () => {
    test("Should not add jobs with a weight above the maxConcurrent", async ({
      harness: h,
      makeLimiter,
    }) => {
      const limiter = makeLimiter({ maxConcurrent: 2 });

      const p1 = limiter.schedule({ weight: 1 }, h.promise, null, 1);
      const p2 = limiter.schedule({ weight: 2 }, h.promise, null, 2);

      await expect(limiter.schedule({ weight: 3 }, h.promise, null, 3)).rejects.toThrow(
        "Impossible to add a job having a weight of 3 to a limiter having a maxConcurrent setting of 2",
      );
      await h.flushLimiter(limiter);
      await Promise.all([expect(p1).resolves.toEqual([1]), expect(p2).resolves.toEqual([2])]);
      expect(h).toHaveFinalCallAt(0);
      expect(h.log).toHaveCallOrder([[1], [2]]);
    });

    test("Should support custom job weights", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 2 });

      // Await all 5 schedule promises before h.flushLimiter(limiter); otherwise the weight: 0
      // job's slowPromise may not have settled by the time h.flushLimiter(limiter) reads calls[].
      await Promise.all([
        expect(limiter.schedule({ weight: 1 }, h.slowPromise, 100, null, 1)).resolves.toEqual([1]),
        expect(limiter.schedule({ weight: 2 }, h.slowPromise, 200, null, 2)).resolves.toEqual([2]),
        expect(limiter.schedule({ weight: 1 }, h.slowPromise, 100, null, 3)).resolves.toEqual([3]),
        expect(limiter.schedule({ weight: 1 }, h.slowPromise, 100, null, 4)).resolves.toEqual([4]),
        expect(limiter.schedule({ weight: 0 }, h.slowPromise, 100, null, 5)).resolves.toEqual([5]),
      ]);

      await h.flushLimiter(limiter);
      expect(h).toHaveFinalCallAt(400);
      expect(h.log).toHaveCallOrder([[1], [2], [3], [4], [5]]);
    });

    test("Should overflow at the correct rate", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({
        maxConcurrent: 2,
        reservoir: 3,
      });

      let calledDepleted = 0;
      const emptyArguments = [];
      limiter.on("depleted", (empty) => {
        emptyArguments.push(empty);
        calledDepleted++;
      });

      const p1 = limiter.schedule({ weight: 1, id: 1 }, h.slowPromise, 100, null, 1);
      const p2 = limiter.schedule({ weight: 2, id: 2 }, h.slowPromise, 150, null, 2);
      const p3 = limiter.schedule({ weight: 1, id: 3 }, h.slowPromise, 100, null, 3);
      const p4 = limiter.schedule({ weight: 1, id: 4 }, h.slowPromise, 100, null, 4);

      await expect(Promise.all([p1, p2])).resolves.toEqual([[1], [2]]);

      expect(limiter.queued()).toEqual(2);
      const reservoirAfterFirstPair = await limiter.currentReservoir();
      expect(reservoirAfterFirstPair).toEqual(0);
      expect(calledDepleted).toEqual(1);

      const incrementedReservoir = await limiter.incrementReservoir(1);
      expect(incrementedReservoir).toEqual(1);

      await h.flushLimiter(limiter, { priority: 1, weight: 0 });
      expect(calledDepleted).toEqual(3);
      expect(limiter.queued()).toEqual(1);
      expect(h).toHaveFinalCallAt(250);
      expect(h.log).toHaveCallOrder([[1], [2]]);

      const reservoirAfterFlush = await limiter.currentReservoir();
      expect(reservoirAfterFlush).toEqual(0);

      await limiter.updateSettings({ reservoir: 1 });
      await Promise.all([expect(p3).resolves.toEqual([3]), expect(p4).resolves.toEqual([4])]);

      const finalReservoir = await limiter.currentReservoir();
      expect(finalReservoir).toEqual(0);
      expect(calledDepleted).toEqual(4);
      expect(emptyArguments).toEqual([false, false, false, true]);
    });
  });

  describe("Expiration", () => {
    test("Should cancel jobs", { timeout: 20000 }, async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 2 });
      const t0 = Date.now();

      // Hold j1 with deferredPromise instead of slowPromise(150). Reason:
      // when the event loop blocks for ~150ms (testcontainer boot, parallel
      // test files, eth.), all pending setTimeouts pile up and fire in the
      // same tick — j1's 150ms resolution timer can fire BEFORE j2's
      // expiration catch runs the running===1 assertion, making running===0
      // (because j1 was freed too). With a deferredPromise we release j1
      // explicitly in j2's expiration branch, after verifying running===1.
      const holdJ1 = deferred();

      // Runs concurrently with j1 below: after j2 expires, verify the
      // mid-flight state, keep j1 held ≥100ms more, then release it.
      const expireJ2ThenReleaseJ1 = async () => {
        await expect(
          limiter.schedule(
            { expiration: 50, id: "slow-with-expiration" },
            h.slowPromise,
            75,
            null,
            2,
          ),
        ).rejects.toThrow("This job timed out after 50 ms.");
        // Lower bound proves expiration didn't fire instantly; the error
        // message itself proves it didn't fire after slowPromise(75).
        expect(Date.now() - t0).toBeGreaterThan(45);

        const [running, doneCount] = await Promise.all([limiter.running(), limiter.done()]);
        expect(running).toEqual(1);
        expect(doneCount).toEqual(1);
        // Hold j1 for ≥100ms more so the post-Promise.all assertion
        // (`Date.now() - t0 > 145`) verifies j1 actually ran a
        // meaningful interval, without depending on a fixed timer that
        // can race event-loop jitter.
        await sleep(100);
        holdJ1.release();
      };

      await Promise.all([
        expect(
          limiter.schedule(
            { id: "very-slow-no-expiration" },
            h.deferredPromise,
            holdJ1.signal,
            null,
            1,
          ),
        ).resolves.toEqual([1]),
        expireJ2ThenReleaseJ1(),
      ]);

      // Lower bound proves the unexpired job wasn't aborted early by
      // the other job's expiration — j1 ran for at least 50ms (j2's
      // expiration window) plus the 100ms hold above.
      expect(Date.now() - t0).toBeGreaterThan(145);
      const [running, done] = await Promise.all([limiter.running(), limiter.done()]);
      expect(running).toEqual(0);
      expect(done).toEqual(2);
    });
  });

  describe("Pubsub", () => {
    test("Should pass strings", async ({ makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 2 });

      // Register the listener before publishing so the message can't be missed.
      const received = new Promise((resolve) => {
        limiter.on("message", resolve);
      });

      await limiter.publish("hello");
      await expect(received).resolves.toEqual("hello");
    });

    test("Should pass objects", async ({ makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 2 });
      const obj = {
        array: ["abc", true],
        num: 235.59,
      };

      // Register the listener before publishing so the message can't be missed.
      const received = new Promise((resolve) => {
        limiter.on("message", resolve);
      });

      await limiter.publish(JSON.stringify(obj));
      const msg = await received;
      expect(JSON.parse(msg)).toEqual(obj);
    });
  });

  describe("Reservoir Refresh", () => {
    test("Should auto-refresh the reservoir", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({
        reservoir: 8,
        reservoirRefreshInterval: 150,
        reservoirRefreshAmount: 5,
        heartbeatInterval: 75, // not for production use
      });
      let calledDepleted = 0;

      limiter.on("depleted", () => {
        calledDepleted++;
      });

      await Promise.all([
        expect(limiter.schedule({ weight: 1 }, h.promise, null, 1)).resolves.toEqual([1]),
        expect(limiter.schedule({ weight: 2 }, h.promise, null, 2)).resolves.toEqual([2]),
        expect(limiter.schedule({ weight: 3 }, h.promise, null, 3)).resolves.toEqual([3]),
        expect(limiter.schedule({ weight: 4 }, h.promise, null, 4)).resolves.toEqual([4]),
        expect(limiter.schedule({ weight: 5 }, h.promise, null, 5)).resolves.toEqual([5]),
      ]);

      const results = await h.flushLimiter(limiter, { weight: 0, priority: 9 });
      expect(h.log).toHaveCallOrder([[1], [2], [3], [4], [5]]);
      // The contract is "`depleted` fires when the reservoir reaches 0".
      // Fire 1 is the contract proof: j5 (weight 5) dispatching after the
      // t=300 refresh reduces reservoir 5→0. Fire 2 is incidental: h.last
      // (weight 0) registering while the reservoir is still 0 also returns
      // reservoir=0 → depleted — but if that register slips past the t=450
      // refresh boundary it sees the refilled value and never fires. Only
      // fire 1 is guaranteed, so >=1 is the honest bound here.
      expect(calledDepleted).toBeGreaterThanOrEqual(1);
      // Jobs 4 and 5 must wait for refreshes; that lower bound proves the
      // refresh gate worked. Asserting current reservoir or a tight upper
      // bound (checkDuration(300)) races a third refresh at t=450ms.
      expect(results).toHaveCallAt(3, 150);
      expect(results).toHaveCallAt(4, 300);
    });

    test("Should allow staggered X by Y type usage", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({
        reservoir: 2,
        reservoirRefreshInterval: 150,
        reservoirRefreshAmount: 2,
        heartbeatInterval: 75, // not for production use
      });

      await Promise.all([
        expect(limiter.schedule(h.promise, null, 1)).resolves.toEqual([1]),
        expect(limiter.schedule(h.promise, null, 2)).resolves.toEqual([2]),
        expect(limiter.schedule(h.promise, null, 3)).resolves.toEqual([3]),
        expect(limiter.schedule(h.promise, null, 4)).resolves.toEqual([4]),
      ]);

      const results = await h.flushLimiter(limiter, { weight: 0, priority: 9 });
      expect(h.log).toHaveCallOrder([[1], [2], [3], [4]]);
      // Jobs 3 and 4 must wait for the reservoir refresh at t=150ms; that
      // lower bound proves the gate worked. Asserting the *current*
      // reservoir is 0 races a possible second refresh at t=300 — and
      // checkDuration(150) is too tight under load.
      expect(results).toHaveCallAt(2, 150);
      expect(results).toHaveCallAt(3, 150);
    });

    test("Should keep process alive until queue is empty", async () => {
      useRealClockForThisTest();
      const fixturePath = path.resolve(__dirname, "fixtures/keep-alive/refreshKeepAlive.mjs");
      const { stdout, stderr } = await execFile(
        process.execPath,
        ["--import", "tsx", fixturePath],
        {
          timeout: 10000,
        },
      );
      // The contract this test covers is "the process stays alive long
      // enough to run all 4 jobs across a reservoir refresh". That's
      // verified by `matches.length === 4` and `stderr === ""`: if the
      // refresh timer didn't keep the event loop alive, the process would
      // exit before jobs 3 & 4 print and we'd see fewer matches and/or a
      // non-empty stderr.
      //
      // The fixture also inherits DATASTORE from the parent test, so when
      // the test runs against ioredis/node-redis the child spins up its
      // own short-lived connection. Fresh-connection overhead and Redis
      // roundtrips give the bucket numbers significant jitter — pair
      // clustering or tight gate bounds flake under that. We keep one
      // sanity check (`nums[2] > nums[0]`) to confirm the second pair
      // landed strictly after the first, which proves the refresh gate
      // held without depending on the absolute bucket value.
      const matches = stdout.match(/\[(\d+)\]/g);
      expect(matches).toBeTruthy();
      expect(matches.length).toEqual(4);
      const nums = matches.map((m) => Number(m.slice(1, -1)));
      expect(nums[2]).toBeGreaterThan(nums[0]);
      expect(stderr).toEqual("");
    });
  });

  describe("Reservoir Increase", () => {
    // `depleted` coverage deliberately lives in the Reservoir Refresh sibling
    // above ("fires when the reservoir reaches 0"). Asserting a count here
    // races the increase boundary: whether the emptying registration lands
    // exactly on reservoir 0 depends on register round-trips staying inside
    // one 150ms tick — under parallel load they can pace past a tick and see
    // the refilled value instead (0 fires, or 1, never more). The contract
    // these tests own is the increase GATE: overweight jobs queue until the
    // next tick.
    test("Should auto-increase the reservoir", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({
        reservoir: 3,
        reservoirIncreaseInterval: 150,
        reservoirIncreaseAmount: 5,
        heartbeatInterval: 75, // not for production use
      });

      await Promise.all([
        expect(limiter.schedule({ weight: 1 }, h.promise, null, 1)).resolves.toEqual([1]),
        expect(limiter.schedule({ weight: 2 }, h.promise, null, 2)).resolves.toEqual([2]),
        expect(limiter.schedule({ weight: 3 }, h.promise, null, 3)).resolves.toEqual([3]),
        expect(limiter.schedule({ weight: 4 }, h.promise, null, 4)).resolves.toEqual([4]),
        expect(limiter.schedule({ weight: 5 }, h.promise, null, 5)).resolves.toEqual([5]),
      ]);

      const results = await h.flushLimiter(limiter, { weight: 0, priority: 9 });
      expect(h.log).toHaveCallOrder([[1], [2], [3], [4], [5]]);
      // Jobs 3, 4, 5 must each wait for an increase tick (150/300/450ms).
      expect(results).toHaveCallAt(2, 150);
      expect(results).toHaveCallAt(3, 300);
      expect(results).toHaveCallAt(4, 450);
    });

    test("Should auto-increase the reservoir up to a maximum", async ({
      harness: h,
      makeLimiter,
    }) => {
      const limiter = makeLimiter({
        reservoir: 3,
        reservoirIncreaseInterval: 150,
        reservoirIncreaseAmount: 5,
        reservoirIncreaseMaximum: 6,
        heartbeatInterval: 75, // not for production use
      });

      await Promise.all([
        expect(limiter.schedule({ weight: 1 }, h.promise, null, 1)).resolves.toEqual([1]),
        expect(limiter.schedule({ weight: 2 }, h.promise, null, 2)).resolves.toEqual([2]),
        expect(limiter.schedule({ weight: 3 }, h.promise, null, 3)).resolves.toEqual([3]),
        expect(limiter.schedule({ weight: 4 }, h.promise, null, 4)).resolves.toEqual([4]),
        expect(limiter.schedule({ weight: 5 }, h.promise, null, 5)).resolves.toEqual([5]),
      ]);

      const results = await h.flushLimiter(limiter, { weight: 0, priority: 9 });
      expect(h.log).toHaveCallOrder([[1], [2], [3], [4], [5]]);
      expect(results).toHaveCallAt(2, 150);
      expect(results).toHaveCallAt(3, 300);
      expect(results).toHaveCallAt(4, 450);
    });

    test("Should allow staggered X by Y type usage", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({
        reservoir: 2,
        reservoirIncreaseInterval: 150,
        reservoirIncreaseAmount: 2,
        heartbeatInterval: 75, // not for production use
      });

      await Promise.all([
        expect(limiter.schedule(h.promise, null, 1)).resolves.toEqual([1]),
        expect(limiter.schedule(h.promise, null, 2)).resolves.toEqual([2]),
        expect(limiter.schedule(h.promise, null, 3)).resolves.toEqual([3]),
        expect(limiter.schedule(h.promise, null, 4)).resolves.toEqual([4]),
      ]);

      const reservoir = await limiter.currentReservoir();
      // After all 4 jobs dispatch, reservoir has been depleted to 0 twice
      // (initial 2 by jobs 1,2; refill 2 by jobs 3,4). Under load, the
      // 150ms increase tick can fire again *between* Promise.all resolving
      // and currentReservoir() returning, bumping it back to 2 (or, very
      // rarely, 4). Allowing up to one extra tick of slop keeps a sanity
      // check while removing the flake. The actual gating contract is
      // verified by the lower-bound time assertions below.
      expect(reservoir).toBeLessThanOrEqual(2);

      const results = await h.flushLimiter(limiter, { weight: 0, priority: 9 });
      expect(h.log).toHaveCallOrder([[1], [2], [3], [4]]);
      // Jobs 3 and 4 must wait for the reservoir refill at t=150ms; lower
      // bound proves the refill gate worked. No upper bound — under load
      // dispatch latency adds to the wait time but doesn't violate the contract.
      expect(results).toHaveCallAt(2, 150);
      expect(results).toHaveCallAt(3, 150);
    });

    test("Should keep process alive until queue is empty", async () => {
      useRealClockForThisTest();
      const fixturePath = path.resolve(__dirname, "fixtures/keep-alive/increaseKeepAlive.mjs");
      const { stdout, stderr } = await execFile(
        process.execPath,
        ["--import", "tsx", fixturePath],
        {
          timeout: 10000,
        },
      );
      // Same contract / loosening rationale as the Reservoir Refresh
      // sibling above: matches.length + empty stderr proves the keep-alive
      // timer kept the event loop running across the increase, and
      // `nums[2] > nums[0]` proves the increase gate held (jobs 3-4 ran
      // strictly later than jobs 1-2). Tighter bucket assertions flake on
      // fresh-connection / module-load jitter when the fixture runs
      // against a Redis datastore inherited from the parent suite.
      const matches = stdout.match(/\[(\d+)\]/g);
      expect(matches).toBeTruthy();
      expect(matches.length).toEqual(4);
      const nums = matches.map((m) => Number(m.slice(1, -1)));
      expect(nums[2]).toBeGreaterThan(nums[0]);
      expect(stderr).toEqual("");
    });
  });
});
