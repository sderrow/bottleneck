# Test reliability: no timing flakes

Shared test files run in two modes: the `local` project (fake timers, exact
virtual time) and the redis projects (real timers + real network round trips;
parallel workers routinely stall the event loop 100ms+). A shared test file
must pass deterministically in both.

## The rule

Never fix a flake by making durations, timeouts, or tolerances longer — that
only makes the race rarer. Replace the race with an ordering guarantee:

- **Hold states with deferred signals** (`deferred()` + `h.deferredPromise`),
  never `slowPromise(ms)`. A held job makes EXECUTING/QUEUED/DONE stable and
  log order deterministic until the test releases it.
- **Barrier submissions with `await enqueued(limiter)`** before releasing
  anything, so queue contents at the first capacity event don't depend on
  round-trip ordering.
- **Observe transient states inside their transition events, not by polling.**
  In src/Job.js every lifecycle method transitions state BEFORE triggering its
  event, and `doExecute` is timer-gated — so a state read inside the job's own
  handler is guaranteed, while a polled window (e.g. RUNNING) that a late
  register can skip entirely (`wait = max(nextRequest - now, 0)`) is
  unfixable at any polling speed.
- **Bound timestamp arithmetic with measured elapsed time** — take `t`/`t2`
  around the read; the script's `now` (client `Date.now()`, passed per script)
  samples in between. Exact values assert a stall-free scheduler. Lower
  bounds on server-enforced gates (dispatch cannot precede `nextRequest`) are
  sound at any magnitude.
- **Assert the product's contract, not timing-dependent counts** — e.g.
  "depleted fires when a register lands on reservoir 0" means `>= 1`, never
  `=== N`.
- **Gate fake-clock-only tests on `process.env.DATASTORE == null`**, not
  `isFakeClock()` (timers install in `beforeEach`, after collection).
- **Never guess at an unreproduced flake.** Capture the failing assertion
  first: run the full `pnpm test:ci` matrix under load and tee the FULL output
  to a per-run file (`pnpm test:ci 2>&1 | tee /tmp/tci-$run.log`) — grep the
  summary live, but diagnose from the saved `Failed Tests` blocks, which
  terminal scrollback loses. Single-file reruns usually pass because
  event-loop congestion is part of the failure.

Canonical examples: test/priority.test.js, test/general.test.js ("Counts and
statuses"), test/stop.test.js (incl. the DATASTORE-gated scheduled-job drop),
test/cluster-coordination.test.js (causality policy at top of file),
test/cluster.test.js ("missed intervals" — measured bounds).
