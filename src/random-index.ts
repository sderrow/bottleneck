/** Compact random id used for job indexes and datastore client ids. */
const randomIndex = () => Math.random().toString(36).slice(2);

module.exports = randomIndex;
