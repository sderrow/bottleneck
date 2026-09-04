const arrayToObject = (arr: unknown[]): Record<string, unknown> => {
  const obj: Record<string, unknown> = {};
  for (let i = 0; i < arr.length; i += 2) {
    obj[arr[i] as string] = arr[i + 1];
  }
  return obj;
};

export { arrayToObject };

/**
 * node-redis negotiates RESP2 by default in v4/v5 but RESP3 by default in v6;
 * ioredis negotiates RESP2 by default but RESP3 when the caller opts in (v6+).
 * Under RESP3 several reply shapes differ from the flat, all-string arrays the
 * rest of Bottleneck assumes:
 *   - HGETALL          -> map object instead of a flat [field, value, ...] array
 *   - WITHSCORES      -> [[member, score:number], ...] instead of [member, "score", ...]
 * Normalize back to the RESP2 canonical shape so the library behaves identically
 * across redis v4/v5/v6 and ioredis v5/v6 regardless of the negotiated protocol.
 */
export const normalizeReply = (cmd: unknown[], reply: unknown): unknown => {
  const name = String(cmd[0]).toLowerCase();
  if (name === "hgetall") {
    // RESP2: flat array -> object. RESP3: already an object; pass through.
    return Array.isArray(reply) ? arrayToObject(reply) : reply;
  }
  // RESP3 WITHSCORES results arrive as pairs ([member, score:number] arrays
  // from node-redis) or as a map object ({ member: score } from ioredis with
  // `replyMapping: "resp3"`). Flatten both to the RESP2 [member, "score", ...]
  // form.
  if (cmd.some((a) => typeof a === "string" && a.toLowerCase() === "withscores")) {
    if (Array.isArray(reply) && reply.length > 0 && Array.isArray(reply[0])) {
      const flat: unknown[] = [];
      for (const [member, score] of reply as [unknown, unknown][]) {
        flat.push(member, String(score));
      }
      return flat;
    }
    if (reply !== null && typeof reply === "object" && !Array.isArray(reply)) {
      const flat: unknown[] = [];
      for (const [member, score] of Object.entries(reply)) {
        flat.push(member, String(score));
      }
      return flat;
    }
  }
  return reply;
};
