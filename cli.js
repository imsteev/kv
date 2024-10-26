import { LSMTree } from "./lsm/index.js";
import { incrementLevels } from "./lsm/manager.js";

const store = await LSMTree.load("./data", "kvdb");

await store.put(1, 2);
await store.put(2, 2);
await store.put(3, 2);

await LSMTree.flush(store, "./data", "kvdb");
