import { describe, expect } from "vitest";
import type { NodeRedisClient } from "../src/cluster/redis-types";
import { BottleneckError } from "../src/index";
import Bottleneck from "./bottleneck";
import { deferred, enqueued, test } from "./helpers/test-api";
import { wrongType } from "./helpers/wrong-type";

/*
 * The `code` on BottleneckError is the stable programmatic signal (messages
 * can be customized via dropErrorMessage and friends). No timing assertions
 * here — every case resolves by construction, so this file is safe under
 * both fake and real clocks and against any datastore.
 */

const codeOf = (value: unknown): string | undefined => (value as BottleneckError).code;

const capture = (promise: Promise<unknown>, message: string): Promise<unknown> =>
  promise.then(
    () => {
      throw new Error(message);
    },
    (e) => e,
  );

describe("BottleneckError codes", () => {
  test("shed jobs carry code DROPPED", async ({ makeLimiter }) => {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      highWater: 1,
      strategy: Bottleneck.strategy.OVERFLOW,
    });
    const hold = deferred();
    const running = limiter.schedule(() => hold.signal);
    const queued = limiter.schedule(() => "queued");
    await enqueued(limiter);

    const err = await capture(
      limiter.schedule(() => "victim"),
      "victim should have been dropped",
    );
    expect(err).toBeInstanceOf(BottleneckError);
    expect(codeOf(err)).toEqual("DROPPED");

    hold.release();
    await expect(running).resolves.toBeUndefined();
    await expect(queued).resolves.toEqual("queued");
  });

  test("timed-out jobs carry code EXPIRED", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    const err = await capture(
      limiter.schedule({ expiration: 20 }, () => new Promise<never>(() => {})),
      "job should have expired",
    );
    expect(err).toBeInstanceOf(BottleneckError);
    expect(codeOf(err)).toEqual("EXPIRED");
    expect((err as Error).message).toEqual("This job timed out after 20 ms.");
  });

  test("duplicate job ids carry code DUPLICATE_JOB_ID", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    const hold = deferred();
    const first = limiter.schedule({ id: "dupe" }, () => hold.signal);
    await enqueued(limiter);

    const err = await capture(
      limiter.schedule({ id: "dupe" }, () => "second"),
      "duplicate id should have been rejected",
    );
    expect(err).toBeInstanceOf(BottleneckError);
    expect(codeOf(err)).toEqual("DUPLICATE_JOB_ID");

    hold.release();
    await expect(first).resolves.toBeUndefined();
  });

  test("overweight jobs carry code OVERWEIGHT", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    const err = await capture(
      limiter.schedule({ weight: 2 }, () => "never"),
      "overweight job should have been rejected",
    );
    expect(err).toBeInstanceOf(BottleneckError);
    expect(codeOf(err)).toEqual("OVERWEIGHT");
  });

  test("jobs added after stop carry code STOPPED", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });
    await limiter.stop();
    const err = await capture(
      limiter.schedule(() => "never"),
      "post-stop job should have been rejected",
    );
    expect(err).toBeInstanceOf(BottleneckError);
    expect(codeOf(err)).toEqual("STOPPED");
  });

  test("constructor misuse carries codes", async () => {
    const invalidArgs = await capture(
      Promise.resolve().then(() => new Bottleneck("nope" as never)),
      "v1-style args should have thrown",
    );
    expect(codeOf(invalidArgs)).toEqual("INVALID_ARGUMENTS");

    const badStore = await capture(
      Promise.resolve().then(() => new Bottleneck({ datastore: "bogus" })),
      "bad datastore should have thrown",
    );
    expect(codeOf(badStore)).toEqual("INVALID_DATASTORE");

    const noClient = await capture(
      Promise.resolve().then(() => new Bottleneck.RedisConnection({} as never)),
      "clientless connection should have thrown",
    );
    expect(codeOf(noClient)).toEqual("MISSING_CLIENT");

    const closedClient = await capture(
      Promise.resolve().then(
        () =>
          new Bottleneck.RedisConnection({ client: wrongType<NodeRedisClient>({ isOpen: false }) }),
      ),
      "unconnected client should have thrown",
    );
    expect(codeOf(closedClient)).toEqual("CLIENT_NOT_OPEN");
  });
});
