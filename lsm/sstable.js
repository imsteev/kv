import { open } from "fs/promises";

// SSTable should handle reading from a file.
export class SortedStringTable {
  constructor(filename, delimiter = ":") {
    this.filename = filename;
    this.delimiter = delimiter;
  }

  async get(key) {
    if (!key) {
      return null;
    }

    let val = null;
    try {
      const f = await open(this.filename);
      for await (const line of f.readLines()) {
        const i = line.indexOf(this.delimiter);
        if (i != -1 && line.slice(0, i) == key) {
          val = line.slice(i + 1);
          break;
        }
      }
    } finally {
      f.close();
    }
    return val;
  }

  /**
   *
   * @param {*} s1 key-value file sorted by key
   * @param {*} s2 key-value file sorted by key
   * @param {*} target
   */
  static merge(s1, s2, target) {}
}
