import { LSMTree } from "./lsm/index.js";

const store = await LSMTree.load("./data", "kvdb");

await store.put(1, 2);
await store.put(2, 2);
await store.put(3, 2);

await LSMTree.flush(store, "./data", "kvdb");

process.on("SIGINT", () => {
  console.log("interrupted! shutting down");
  process.exit();
});
