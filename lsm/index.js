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

  async flush() {
    const keyVals = Object.entries(this.memtable);
    keyVals.sort((a, b) =>
      a[0].localeCompare(b[0], undefined, { numeric: true })
    );
    return fs.writeFile(
      this.levelPath(0),
      keyVals
        .map(([k, v]) => {
          return `${k}:${v}`;
        })
        .join("\n"),
      { encoding: "utf-8" }
    );
  }

  async incrementLevels() {
    let i = 100;
    for (let i = 100; i >= 0; i--) {
      try {
        await fs.rename(this.levelPath(i), this.levelPath(i + 1));
      } catch {}
    }
  }

  get prefixPath() {
    return path.join(this.dataFolder, this.levelPrefix);
  }

  levelPath(i) {
    return this.prefixPath + "." + i;
  }
}
