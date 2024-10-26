import fs from "fs/promises";

import { SortedStringTable } from "./sstable.js";
import path from "path";

export class LSMTree {
  // does this tree need to know where the files go?
  constructor({ initialState, dataFolder, levelPrefix }) {
    this.memtable = initialState || {};
    this.dataFolder = dataFolder;
    this.levelPrefix = levelPrefix;
  }

  get prefixPath() {
    return path.join(this.dataFolder, this.levelPrefix);
  }
  // looks in-memory first, if not found will look at sstables on disk.
  async get(key) {
    if (key in this.memtable) {
      return this.memtable[key];
    }
    let val = null;
    for await (const filename of fs.glob(this.prefixPath)) {
      const st = new SortedStringTable(filename);
      val = await st.find(key);
      if (val !== null) {
        console.log("found in " + filename);
        return val;
      }
    }
    return null;
  }

  // todo: make this more robust.
  put(key, value) {
    // how to do concurrent writing properly? this is fine if the caller calls await, but if not, basically a whole bunch of the async functions get called, so everything gets put into memtable. then once they settle, we flush many times.
    this.memtable[key] = value;
  }

  // factory function to load from disk. will always load from the first level.
  static async load(folder, prefix) {
    const firstLevel = getLevelPath(folder, prefix, 0);
    try {
      await fs.access(firstLevel);
    } catch {
      return new LSMTree({ dataFolder: folder, levelPrefix: prefix });
    }

    // file exists. populate in-memory memtable.
    const f = await fs.open(firstLevel);
    const memtable = {};
    for await (const line of f.readLines()) {
      const [k, v] = getKeyValue(line, ":");
      memtable[k] = v;
    }

    return new LSMTree({
      initialState: memtable,
      dataFolder: folder,
      levelPrefix: prefix,
    });
  }

  // if levelFile is empty, this just writes the memtable straight to levelFile
  // otherwise, memtable gets merge-sorted in.
  /**
   *
   * @param {*} levelFile fs.FileHandle
   * @returns
   */
  async merge(levelFile) {
    const entries = Object.entries(this.memtable);

    try {
      const lf = await fs.readFile(levelFile, { encoding: "utf-8" });
      const lines = lf.split("\n");
      const sorted = [];
      let i = 0;
      let j = 0;
      while (i < lines.length || j < entries.length) {
        if (i < lines.length && j < entries.length) {
          const kv = getKeyValue(lines[i], ":");
          if (!kv) {
            i++;
            continue;
          }
          const [k, v] = kv;
          if (
            k.localeCompare(entries[j][0], undefined, { numeric: true }) <= 0
          ) {
            sorted.push([k, v]);
            i++;
          } else {
            sorted.push(entries[j]);
            j++;
          }
        } else if (i < lines.length) {
          sorted.push(lines[i]);
          i++;
        } else {
          sorted.push(entries[j]);
          j++;
        }
      }
      return fs.writeFile(
        levelFile,
        sorted.map(([k, v]) => `${k}:${v}\n`)
      );
    } catch (err) {
      console.log(err);
      entries.sort((a, b) =>
        a[0].localeCompare(b[0], undefined, { numeric: true })
      );
      return fs.appendFile(
        getLevelPath(this.dataFolder, this.levelPrefix, 0),
        entries.map(([k, v]) => `${k}:${v}\n`)
      );
    }
  }
}

// e.g returns "dataFolder/{prefix}.0" or "dataFolder/{prefix}.1" or ...
function getLevelPath(folder, prefix, i) {
  return path.join(folder, prefix + "." + i);
}

const getKeyValue = (line, delim = ":") => {
  const i = line.indexOf(delim);
  if (i === -1) {
    console.log("wot");
    return null;
  }
  return [line.slice(0, i), line.slice(i + 1)];
};

export async function incrementLevels(folder, prefix = "kvdb") {
  for (let i = 10; i >= 0; i--) {
    try {
      const oldName = path.join(folder, `${prefix}.${i}`);
      const newName = path.join(folder, `${prefix}.${i + 1}`);
      await fs.access(path.join(folder, `${prefix}.${i}`));
      await fs.rename(oldName, newName);
    } catch {}
  }
}
