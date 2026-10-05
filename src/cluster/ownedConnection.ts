import type Events from "../Events";
import type { ConnectionInternals, IORedisLib, NodeRedisLib } from "./redis-types";
import BottleneckError from "../BottleneckError";
import IORedisConnection from "./IORedisConnection";
import RedisConnection from "./RedisConnection";

type OwnedConnectionOptions = {
  Redis?: unknown;
  clientOptions?: unknown;
  clusterNodes?: unknown;
  Events: Events;
};

/**
 * Build a connection, and the client it owns, from a limiter's or Group's
 * `Redis` / `clientOptions` / `clusterNodes` options.
 */
export default function ownedConnection(
  datastore: string,
  { Redis, clientOptions = {}, clusterNodes = null, Events }: OwnedConnectionOptions,
): RedisConnection | IORedisConnection {
  if (datastore !== "redis" && datastore !== "ioredis") {
    throw new BottleneckError(`Invalid datastore type: ${datastore}`, "INVALID_DATASTORE");
  }
  if (Redis == null) {
    throw new BottleneckError(
      "Bottleneck cluster mode requires a `Redis` library reference or a pre-built `client`. " +
        `Pass it explicitly: \`new Bottleneck({ datastore: '${datastore}', Redis, clientOptions })\`.`,
      "MISSING_CLIENT",
    );
  }
  const internals: ConnectionInternals = { Events, ownsClient: true };
  if (datastore === "redis") {
    const client = (Redis as NodeRedisLib).createClient(clientOptions);
    return new RedisConnection({ client, ...internals });
  }
  const lib = Redis as IORedisLib;
  const client =
    clusterNodes != null ? new lib.Cluster(clusterNodes, clientOptions) : new lib(clientOptions);
  return new IORedisConnection({ client, ...internals });
}
