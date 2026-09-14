import assert from "node:assert/strict";
import test from "node:test";
import { Writable } from "node:stream";
import { DataStream, StringStream } from "../../dist/index.js";

test("DataStream.CSVStringify preserves headers and quoted cells", async () => {
  assert.deepEqual(await DataStream.from([{ name: "Grace, Jr.", age: 42 }]).CSVStringify().toArray(), ["name,age\n", '"Grace, Jr.",42\n']);
});

test("StringStream.CSVParse returns keyed rows", async () => {
  assert.deepEqual(await StringStream.from("name,age\nAda,36\n").CSVParse().toArray(), [{ name: "Ada", age: "36" }]);
});

test("DataStream.CSVStringify honors a slow backpressured consumer", async () => {
  const rows = Array.from({ length: 120 }, (_, index) => ({ index, value: `value-${index}` }));
  const csv = DataStream.from(rows).CSVStringify();
  const received = [];
  let backpressured = 0;
  const slow = new Writable({
    objectMode: true,
    highWaterMark: 1,
    write(chunk, _encoding, callback) {
      received.push(chunk);
      setTimeout(callback, 1);
    }
  });
  const write = slow.write.bind(slow);
  slow.write = (...args) => {
    const result = write(...args);
    if (!result) backpressured += 1;
    return result;
  };
  const finished = new Promise((resolve, reject) => {
    slow.once("finish", resolve);
    slow.once("error", reject);
  });
  csv.pipe(slow);
  await finished;
  assert.ok(backpressured > 0);
  assert.equal(received.length, rows.length + 1);
  assert.equal(received[0], "index,value\n");
  assert.equal(received.at(-1), "119,value-119\n");
});
