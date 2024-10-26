import fs from "fs/promises";

import path from "path";

const IN_MEMORY_LIMIT_BYTES = 4000;

export class LSMTree {
  // does this tree need to know where the files go?
  constructor({ initialState, dataFolder, levelPrefix }) {
    this.primary = initialState || {};
    this.dataFolder = dataFolder;
    this.levelPrefix = levelPrefix;
  }

  // looks in-memory first, if not found will look at sstables on disk.
  async get(key) {
    if (key in this.primary) {
      return this.primary[key];
    }

    for (const st of this.sstables) {
      const val = await st.get(key);
      if (val !== null) {
        return val;
      }
    }

    return null;
  }

  async put(key, value) {
    this.primary[key] = value;
    // todo: flush to disk if size gets too big
  }

  // factory function to load from disk. will always load from the first level.
  static async load(folder, prefix) {
    const firstLevel = path.join(folder, prefix + ".0");
    try {
      await fs.access(firstLevel);
    } catch {
      return new LSMTree({ dataFolder: folder, levelPrefix: prefix });
    }

    // file exists. populate in-memory primary.
    const f = await fs.open(firstLevel);
    const primary = {};
    for await (const line of f.readLines()) {
      const [k, v] = getKeyValue(line, ":");
      primary[k] = v;
    }

    return new LSMTree({
      initialState: primary,
      dataFolder: folder,
      levelPrefix: prefix,
    });
  }

  static async flush(lsmTree, folder, prefix, delim = ":") {
    try {
      await incrementLevels(folder, prefix);
    } catch (err) {
      console.log("could not increment filenames.");
      throw err;
    }
    const entries = Object.entries(lsmTree.primary);
    entries.sort((a, b) => a[0].localeCompare(b[0]));
    return fs.appendFile(
      path.join(folder, `${prefix}.0`),
      entries.map(([k, v]) => `${k}${delim}${v}\n`)
    );
  }
}

function getKeyValue(line, delim) {
  const index = line.indexOf(delim);
  if (index == -1) {
    return null;
  }
  return [line.slice(0, index), line.slice(index + 1)];
}

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
