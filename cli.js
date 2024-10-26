import { LSMTree } from "./lsm/index.js";

const store = await LSMTree.load("./data", "kvdb");

for (let i = 0; i < 100; i++) {
  const key = Math.trunc(Math.random() * 1_000);
  store.put(key, i * 10);
}

await store.merge("./data/kvdb.0");
await store.get(25);
