exports.load = function (received, defaults, onto) {
  onto ??= {};
  for (const [k, v] of Object.entries(defaults)) {
    onto[k] = received[k] ?? v;
  }
  return onto;
};

exports.overwrite = function (received, defaults, onto) {
  onto ??= {};
  for (const [k, v] of Object.entries(received)) {
    if (defaults[k] !== undefined) {
      onto[k] = v;
    }
  }
  return onto;
};
