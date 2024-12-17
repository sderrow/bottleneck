// TODO: This file was created by bulk-decaffeinate.
// Sanity-check the conversion and remove this comment.
/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/main/docs/suggestions.md
 */
exports.load = function (received, defaults, onto) {
  if (onto == null) {
    onto = {};
  }
  for (var k in defaults) {
    var v = defaults[k];
    onto[k] = received[k] != null ? received[k] : v;
  }
  return onto;
};

exports.overwrite = function (received, defaults, onto) {
  if (onto == null) {
    onto = {};
  }
  for (var k in received) {
    var v = received[k];
    if (defaults[k] !== undefined) {
      onto[k] = v;
    }
  }
  return onto;
};
