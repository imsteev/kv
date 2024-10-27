import fs from "fs/promises";

import { SortedStringTable } from "./sstable.js";
import path from "path";

export class LSMTree {
  constructor({ initialState, dataFolder, levelPrefix, flushIntervalMs }) {
    this.dataFolder = dataFolder;
    this.levelPrefix = levelPrefix;

    this.memtable = initialState || {};
    this.flushMemtable = {};

    this.isFlushing = false;

    setInterval(async () => {
      await this.flush();
    }, flushIntervalMs);
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

  // flush flushes memtable to disk. newer files have lower numbers (this makes
  // it easier on LSMTree code since you don't have to track how many files there
  // are).
  async flush(level = 0) {
    console.log({
      isFlushing: this.isFlushing,
      length: Object.keys(this.memtable).length,
    });
    if (this.isFlushing || Object.keys(this.memtable).length < 50) {
      return;
    }
    this.isFlushing = true;
    const tmp = this.memtable;
    this.memtable = {};
    this.flushMemtable = tmp;
    const keyVals = Object.entries(this.flushMemtable);
    keyVals.sort((a, b) =>
      a[0].localeCompare(b[0], undefined, { numeric: true })
    );
    console.log("flushing!");
    try {
      await fs.appendFile(
        this.levelPath(level),
        keyVals.map(([k, v]) => {
          return `${k}:${v}\n`;
        }),
        { encoding: "utf-8" }
      );
    } finally {
      this.isFlushing = false;
      this.flushMemtable = {};
    }
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
