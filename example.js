import { LSMTree } from "./lsm/index.js";

const lsmTree = new LSMTree({
  dataFolder: "./data",
  levelPrefix: "kvdb",
  flushIntervalMs: 100,
});

for (let i = 0; i < 10000; i++) {
  console.log(i);
  await new Promise((resolve) => setTimeout(resolve, 50));
  const val = Math.trunc(Math.random() * 1_000);
  lsmTree.put(i, val);
}

// how to od a sleep?

// await Promise.resolve();
