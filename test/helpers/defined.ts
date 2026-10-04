/**
 * Returns `value`, or throws if it is null/undefined. Use instead of a `!`
 * non-null assertion when a test reads an element it expects to exist, so a
 * missing value fails with a clear message rather than a TypeError later.
 */
export function defined<T>(value: T, label = "value"): NonNullable<T> {
  if (value == null) {
    throw new Error(`Expected ${label} to be defined, got ${String(value)}`);
  }
  return value;
}
