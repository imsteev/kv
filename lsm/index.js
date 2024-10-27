import fs from "fs/promises";
import path from "path";

import { SortedStringTable } from "./sstable.js";

export class LSMTree {
  constructor({
    initialState,
    dataFolder,
    levelPrefix,
    flushIntervalMs,

    // note: this is more of a "soft" max. there are some concurrency things to iron out to truly respect this number.
    memtableMaxItems = 10_000,
  }) {
    // Every LSMTree is associated with a folder on disk. persistent data are
    // store in "levels", which are suffixed with a number to indicate age.
    // For example if dataFolder="./data" and levelPrefix="kvdb", your first level
    // would be "./data/kvdb.0".
    this.dataFolder = dataFolder;
    this.levelPrefix = levelPrefix;

    // active, in-memory dataset
    this.memtable = initialState || {};
    this.memtableMaxItems = memtableMaxItems;

    // periodically the LSMTree will flush data if the memtable is too large.
    this.flushMemtable = {};
    this.isFlushing = false;

    setInterval(() => {
      this.flush();
    }, flushIntervalMs);
  }

  /**
   * looks in-memory first, if not found will look at sstables on disk.
   * @param {string} key
   * @returns {string | null}
   */
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

  /**
   *
   * @param {string} key
   * @param {string} value
   */
  async put(key, value) {
    this.memtable[key] = value;
  }

  /**
   * load will load an SSTable from disk, if it exists. otherwise returns an
   * empty tree.
   * @param {string} folder
   * @param {string} prefix
   */
  async load(level = 0) {
    let f;
    try {
      f = await fs.open(this.levelPath(level));
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
   * flushes memtable to disk. newer files have lower numbers (this makes
   * it easier on LSMTree code since you don't have to track how many files there
   * are).
   * @param {number} level
   * @returns
   */
  async flush(level = 0) {
    if (
      this.isFlushing ||
      Object.keys(this.memtable).length < this.memtableMaxItems
    ) {
      return;
    }
    this.isFlushing = true;

    // make space for a new level file.
    await this.incrementLevels(level);

    // swap!
    const tmp = this.memtable;
    this.memtable = {};
    this.flushMemtable = tmp;

    // todo: probably use a better data structure for keeping keys sorted.
    const keyVals = Object.entries(this.flushMemtable);
    keyVals.sort((a, b) =>
      a[0].localeCompare(b[0], undefined, { numeric: true })
    );
    try {
      await fs.appendFile(
        this.levelPath(level),
        keyVals.map(([k, v]) => {
          return `${k}:${v}\n`;
        }),
        { encoding: "utf-8" }
      );
    } catch (err) {
      console.log(`failed to flush data: ${err}`);
    } finally {
      // note: if error happens it's possible for data loss. eek!
      this.isFlushing = false;
      this.flushMemtable = {};
    }
  }

  /**
   * incrementLevels renames level files so that they make space for a new file
   * to be inserted at @baseLevel.
   * @param {number} baseLevel
   */
  async incrementLevels(baseLevel) {
    for (let i = 100; i >= baseLevel; i--) {
      try {
        await fs.rename(this.levelPath(i), this.levelPath(i + 1));
      } catch {
        console.log(`failed to rename level ${i}. skipping`);
      }
    }
  }

  get prefixPath() {
    return path.join(this.dataFolder, this.levelPrefix);
  }

  levelPath(i) {
    return this.prefixPath + "." + i;
  }
}
