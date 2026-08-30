import Bottleneck from "../../bottleneck.mjs";

const now = Date.now();
const limiter = new Bottleneck({
  reservoir: 2,
  reservoirIncreaseAmount: 2,
  reservoirIncreaseInterval: 200,
});

const f1 = () => {
  const secDiff = Math.floor((Date.now() - now) / 100);
  return Promise.resolve(`[${secDiff}]`);
};

for (let i = 0; i < 4; i++) {
  const x = await limiter.schedule(f1);
  process.stdout.write(x);
}
