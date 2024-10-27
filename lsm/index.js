import fs from "fs/promises";

import { SortedStringTable } from "./sstable.js";
import path from "path";

export class LSMTree {
  constructor({ initialState, dataFolder, levelPrefix }) {
    this.memtable = initialState || {};
    this.dataFolder = dataFolder;
    this.levelPrefix = levelPrefix;
  }

  // looks in-memory first, if not found will look at sstables on disk.
  async get(key) {
    if (key in this.memtable) {
      return this.memtable[key];
    }
    let val;
    for await (const filename of fs.glob(this.prefixPath)) {
      const st = new SortedStringTable(filename);
      let val = await st.find(key);
      if (val !== null) {
        return val;
      }
    }
    return val;
  }

  // todo: make this more robust.
  async put(key, value) {
    this.memtable[`${key}`] = value;
  }

  /**
   * load will load an SSTable from disk, if it exists. otherwise returns an
   * empty tree.
   * @param {*} folder
   * @param {*} prefix
   */
  async load() {
    let f;
    try {
      f = await fs.open(this.levelPath(0));
    } catch {
      console.log("level file doesn't exist, nothing to load.");
      return;
    }

    // file exists. populate memtable.
    for await (const line of f.readLines()) {
      const i = line.indexOf(":");
      if (i === -1) {
        continue;
      }
      const key = line.slice(0, i);
      const val = line.slice(i + 1);
      this.memtable[key] = val;
    }
  }

  /**
   * merge merges the memtable to levelFile, in merge-sorted fashion. if levelFile
   * exists, assumes it is sorted by key.
   * @param {*} level int
   */
  async mergeIntoLevel(level) {
    const entries = Object.entries(this.memtable);
    const st = new SortedStringTable(this.levelPath(level));
    try {
      await st.mergeKeyVals(entries);
    } catch (err) {
      console.log("failed to merge");
      throw err;
    }
  }

  get prefixPath() {
    return path.join(this.dataFolder, this.levelPrefix);
  }

  levelPath(i) {
    return this.prefixPath + "." + i;
  }
}
