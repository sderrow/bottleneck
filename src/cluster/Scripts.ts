import lua from "./lua/index";

const headers: Record<string, string> = {
  refs: lua["refs.lua"] as string,
  validate_keys: lua["validate_keys.lua"] as string,
  validate_client: lua["validate_client.lua"] as string,
  refresh_expiration: lua["refresh_expiration.lua"] as string,
  process_tick: lua["process_tick.lua"] as string,
  conditions_check: lua["conditions_check.lua"] as string,
  get_time: lua["get_time.lua"] as string,
};

export const allKeys = (id: string): string[] => [
  // HASH
  `b_${id}_settings`,

  // HASH
  // job index -> weight
  `b_${id}_job_weights`,

  // ZSET
  // job index -> expiration
  `b_${id}_job_expirations`,

  // HASH
  // job index -> client
  `b_${id}_job_clients`,

  // ZSET
  // client -> sum running
  `b_${id}_client_running`,

  // HASH
  // client -> num queued
  `b_${id}_client_num_queued`,

  // ZSET
  // client -> last job registered
  `b_${id}_client_last_registered`,

  // ZSET
  // client -> last seen
  `b_${id}_client_last_seen`,
];

type Template = {
  keys: typeof allKeys;
  headers: string[];
  refresh_expiration: boolean;
  code: string;
};

const templates: Record<string, Template> = {
  init: {
    keys: allKeys,
    headers: ["process_tick"],
    refresh_expiration: true,
    code: lua["init.lua"] as string,
  },
  group_check: {
    keys: allKeys,
    headers: [],
    refresh_expiration: false,
    code: lua["group_check.lua"] as string,
  },
  register_client: {
    keys: allKeys,
    headers: ["validate_keys"],
    refresh_expiration: true,
    code: lua["register_client.lua"] as string,
  },
  blacklist_client: {
    keys: allKeys,
    headers: ["validate_keys", "validate_client"],
    refresh_expiration: false,
    code: lua["blacklist_client.lua"] as string,
  },
  heartbeat: {
    keys: allKeys,
    headers: ["validate_keys", "validate_client", "process_tick"],
    refresh_expiration: false,
    code: lua["heartbeat.lua"] as string,
  },
  update_settings: {
    keys: allKeys,
    headers: ["validate_keys", "validate_client", "process_tick"],
    refresh_expiration: true,
    code: lua["update_settings.lua"] as string,
  },
  running: {
    keys: allKeys,
    headers: ["validate_keys", "validate_client", "process_tick"],
    refresh_expiration: false,
    code: lua["running.lua"] as string,
  },
  queued: {
    keys: allKeys,
    headers: ["validate_keys", "validate_client"],
    refresh_expiration: false,
    code: lua["queued.lua"] as string,
  },
  done: {
    keys: allKeys,
    headers: ["validate_keys", "validate_client", "process_tick"],
    refresh_expiration: false,
    code: lua["done.lua"] as string,
  },
  check: {
    keys: allKeys,
    headers: ["validate_keys", "validate_client", "process_tick", "conditions_check"],
    refresh_expiration: false,
    code: lua["check.lua"] as string,
  },
  submit: {
    keys: allKeys,
    headers: ["validate_keys", "validate_client", "process_tick", "conditions_check"],
    refresh_expiration: true,
    code: lua["submit.lua"] as string,
  },
  register: {
    keys: allKeys,
    headers: ["validate_keys", "validate_client", "process_tick", "conditions_check"],
    refresh_expiration: true,
    code: lua["register.lua"] as string,
  },
  free: {
    keys: allKeys,
    headers: ["validate_keys", "validate_client", "process_tick"],
    refresh_expiration: true,
    code: lua["free.lua"] as string,
  },
  current_reservoir: {
    keys: allKeys,
    headers: ["validate_keys", "validate_client", "process_tick"],
    refresh_expiration: false,
    code: lua["current_reservoir.lua"] as string,
  },
  increment_reservoir: {
    keys: allKeys,
    headers: ["validate_keys", "validate_client", "process_tick"],
    refresh_expiration: true,
    code: lua["increment_reservoir.lua"] as string,
  },
};

export const names = Object.keys(templates);

export const keys = (name: string, id: string): string[] => templates[name]!.keys(id);

export const payload = (name: string): string => {
  const template = templates[name]!;
  return Array.prototype
    .concat(
      headers.refs,
      template.headers.map((h) => headers[h]),
      template.refresh_expiration ? headers.refresh_expiration : "",
      template.code,
    )
    .join("\n");
};
