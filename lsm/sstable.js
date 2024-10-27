import { appendFile, open, readFile, writeFile, rename } from "fs/promises";

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

    return val;
  }

  async mergeKeyVals(keyVals) {
    keyVals.sort((a, b) =>
      a[0].localeCompare(b[0], undefined, { numeric: true })
    );

    let lf;
    try {
      lf = await readFile(this.filename, { encoding: "utf-8" });
    } catch {
      return appendFile(
        this.filename,
        keyVals
          .map(([k, v]) => {
            return `${k}:${v}`;
          })
          .join("\n"),
        { encoding: "utf-8" }
      );
    }
    let i = 0;
    let j = 0;
    const lines = lf.split("\n");
    const sorted = [];
    while (i < lines.length && j < keyVals.length) {
      if (i < lines.length && j < keyVals.length) {
        const line = lines[i];
        const idx = line.indexOf(":");
        if (idx === -1) {
          i++;
          continue;
        }

        const k = line.slice(0, idx);
        const v = line.slice(idx + 1);
        if (k == undefined) {
          console.log("UNDEFINED");
        }
        if (k.localeCompare(keyVals[j][0], undefined, { numeric: true }) <= 0) {
          // todo: handle duplicates
          sorted.push([k, v]);
          i++;
        } else {
          sorted.push(keyVals[j++]);
        }
      } else if (j < keyVals.length) {
        sorted.push(keyVals[j++]);
      }
    }

    return writeFile(
      this.filename,
      sorted
        .map(([k, v]) => {
          return `${k}:${v}`;
        })
        .join("\n"),
      { encoding: "utf-8" }
    );
  }
}
