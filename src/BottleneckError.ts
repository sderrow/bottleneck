class BottleneckError extends Error {
  constructor(message) {
    super(message);
    this.name = "BottleneckError";
  }
}

module.exports = BottleneckError;
