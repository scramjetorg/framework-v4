import assert from "node:assert/strict";
import test from "node:test";

import framework, {
  DataStream,
  NumberStream,
  ReReadable,
  StringStream,
} from "../dist/index.js";

test("exports framework classes and retains Core transform behavior", async () => {
  assert.equal(framework.DataStream, DataStream);
  assert.equal(framework.StringStream, StringStream);
  assert.equal(framework.ReReadable, ReReadable);
  assert.deepEqual(
    await DataStream.from([1, 2, 3]).map(async (value) => value * 2).toArray(),
    [2, 4, 6],
  );
});

test("parses documented CSV rows and serializes records", async () => {
  const rows = await StringStream
    .from("name,age\nAda,36\n\"Grace, Jr.\",42\n")
    .CSVParse()
    .toArray();
  assert.deepEqual(rows, [
    { name: "Ada", age: "36" },
    { name: "Grace, Jr.", age: "42" },
  ]);

  const output = await DataStream
    .from([{ name: "Ada", age: 36 }])
    .CSVStringify()
    .toArray();
  assert.deepEqual(output, ["name,age\n", "Ada,36\n"]);
});

test("StringStream.from creates a CSV-capable framework stream", async () => {
  const rows = await StringStream.from("name,age\nAda,36\n").CSVParse().toArray();
  assert.deepEqual(rows, [{ name: "Ada", age: "36" }]);
});

test("integrates bounded replay and framework numeric helpers", async () => {
  const replay = new ReReadable({ length: 2, objectMode: true });
  replay.write("first");
  replay.end("second");
  assert.deepEqual(await DataStream.from(replay.rewind()).toArray(), ["first", "second"]);

  const numbers = NumberStream.from([1, 2, 3]);
  assert.equal(await numbers.sum(), 6);
});
