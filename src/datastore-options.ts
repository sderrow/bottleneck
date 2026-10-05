import BottleneckError from "./BottleneckError";

/** Options Bottleneck v5 removed in favor of `connection`. */
const REMOVED_OPTIONS = ["Redis", "client", "clientOptions", "clusterNodes"];

/**
 * Reject the pre-v5 ways of asking for Redis (rather than silently running a
 * local limiter): a Redis-backed limiter or Group now takes a `connection`
 * built from the consumer's own client. `datastore: "local"` stays harmless.
 */
export function validateDatastoreOptions(options: object): void {
  const values = options as Record<string, unknown>;
  const removed = REMOVED_OPTIONS.filter((name) => values[name] != null);
  const { datastore } = values;
  if (datastore === "redis" || datastore === "ioredis") removed.unshift("datastore");
  if (removed.length > 0) {
    throw new BottleneckError(
      `Bottleneck v5 removed the ${removed.map((name) => `\`${name}\``).join(", ")} option(s). ` +
        "Build a connection from your own Redis client and pass it instead: " +
        "`new Bottleneck({ connection: new IORedisConnection({ client }) })`.",
      "LEGACY_REDIS_OPTIONS",
    );
  }
  if (datastore != null && datastore !== "local") {
    throw new BottleneckError(`Invalid datastore type: ${String(datastore)}`, "INVALID_DATASTORE");
  }
}
