/**
 * Pass a deliberately wrong-shaped value where `T` is expected, for tests of
 * invalid input. `T` is usually inferred from the call site, e.g.
 * `limiter.jobStatus(wrongType(3))`.
 */
export function wrongType<T>(value: unknown): T {
  return value as T;
}
