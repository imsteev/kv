import { LSMTree } from "./lsm/index.js";

const lsmTree = new LSMTree({ dataFolder: "./data", levelPrefix: "kvdb" });

for (let i = 0; i < 25; i++) {
  const key = Math.trunc(Math.random() * 1_000);
  lsmTree.put(key, i * 10);
}

await lsmTree.mergeIntoLevel(0);
