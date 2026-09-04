type Dict = Record<string, unknown>;

/**
 * Copies each key of `defaults` into `onto` (a new object by default),
 * taking the value from `received` when present, falling back to the
 * default value otherwise.
 */
export function load<T extends object, O extends object>(
  received: object,
  defaults: T,
  onto: O,
): O & T;
export function load<D extends object>(received: object, defaults: D): D;
export function load(received: object, defaults: object, onto?: object): object {
  const target = (onto ?? {}) as Dict;
  const source = received as Dict;
  for (const [k, v] of Object.entries(defaults)) {
    target[k] = source[k] ?? v;
  }
  return target;
}

/**
 * Copies into `onto` (a new object by default) only the keys that exist on
 * `defaults`, taking the value from `received`.
 */
export function overwrite<T extends object, O extends object>(
  received: object,
  defaults: T,
  onto: O,
): O & T;
export function overwrite<D extends object>(received: object, defaults: D): D;
export function overwrite(received: object, defaults: object, onto?: object): object {
  const target = (onto ?? {}) as Dict;
  const source = received as Dict;
  for (const [k, v] of Object.entries(source)) {
    if ((defaults as Dict)[k] !== undefined) {
      target[k] = v;
    }
  }
  return target;
}
