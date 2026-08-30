class BottleneckError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = "BottleneckError";
  }
}

export default BottleneckError;
