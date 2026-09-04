/** Compact random id used for job indexes and datastore client ids. */
const randomIndex = (): string => Math.random().toString(36).slice(2);

export default randomIndex;
