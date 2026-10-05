/**
 * Machine-readable reason carried by a {@link BottleneckError}. Unlike
 * `message` (which callers can customize via `dropErrorMessage` and friends),
 * `code` is stable and safe to branch on.
 */
export type BottleneckErrorCode =
  | "DROPPED"
  | "EXPIRED"
  | "STOPPED"
  | "DUPLICATE_JOB_ID"
  | "OVERWEIGHT"
  | "INVALID_DATASTORE"
  | "INVALID_ARGUMENTS"
  | "MISSING_CLIENT"
  | "CLIENT_NOT_OPEN"
  | "LEGACY_REDIS_OPTIONS";

class BottleneckError extends Error {
  /** Stable machine-readable reason; absent on internal invariant failures. */
  code?: BottleneckErrorCode;
  constructor(message?: string, code?: BottleneckErrorCode) {
    super(message);
    this.name = "BottleneckError";
    if (code !== undefined) this.code = code;
  }
}

export default BottleneckError;
