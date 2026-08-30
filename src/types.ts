/**
 * Shared option/value shapes used across the limiter and its datastores.
 * Kept loose (index signatures) because user-supplied option objects are
 * forwarded opaquely through parser.load/overwrite.
 */

export type StoreOptions = {
  maxConcurrent: number | null;
  minTime: number;
  highWater: number | null;
  strategy: number | null;
  penalty: number | null;
  reservoir: number | null;
  reservoirRefreshInterval: number | null;
  reservoirRefreshAmount: number | null;
  reservoirIncreaseInterval: number | null;
  reservoirIncreaseAmount: number | null;
  reservoirIncreaseMaximum: number | null;
  [key: string]: unknown;
};

export type JobOptions = {
  priority: number;
  weight: number;
  expiration: number | null;
  id: string;
  [key: string]: unknown;
};

export type JobDefaults = JobOptions;

export type EventInfo = {
  args: unknown[] | null;
  options: JobOptions;
  retryCount?: number;
};
