import assert from "node:assert/strict";
import { Readable, Writable } from "node:stream";
import test from "node:test";
import { DataStream } from "../../dist/index.js";

test("Framework streams preserve Node Readable/Writable interoperability", async () => {
  const chunks = [];
  const writable = new Writable({
    objectMode: true,
    write(chunk, _encoding, callback) {
      chunks.push(chunk);
      callback();
    },
  });
  DataStream.from(Readable.from([1, 2, 3])).pipe(writable);
  await new Promise((resolve, reject) => {
    writable.once("finish", resolve);
    writable.once("error", reject);
  });
  assert.deepEqual(chunks, [1, 2, 3]);
});
