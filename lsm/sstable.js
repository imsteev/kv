import { open } from "fs/promises";

// SSTable should handle reading from a file.
export class SortedStringTable {
  constructor(filename, delimiter = ":") {
    this.filename = filename;
    this.delimiter = delimiter;
  }

  // improvement: binary search
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
    } catch (err) {
      throw err;
    } finally {
      f.close();
    }
    return val;
  }
}
