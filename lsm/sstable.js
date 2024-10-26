import { open } from "fs/promises";

// SSTable should handle reading from a file.
export class SortedStringTable {
  constructor(filename, delimiter = ":") {
    this.filename = filename;
    this.delimiter = delimiter;
  }

  // improvement: binary search
  async find(key) {
    if (!key) {
      return null;
    }
    let f;
    let val = null;
    try {
      f = await open(this.filename);
      for await (const line of f.readLines()) {
        const i = line.indexOf(this.delimiter);
        if (i != -1 && line.slice(0, i) == key) {
          return line.slice(i + 1);
        }
      }
    } finally {
      await f?.close();
    }
  }

  async write(data) {}
}
