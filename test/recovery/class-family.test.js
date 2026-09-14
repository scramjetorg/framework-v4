import assert from "node:assert/strict";
import test from "node:test";
import {
  BufferStream,
  DataStream,
  MultiStream,
  NumberStream,
  StringStream,
  WindowStream,
} from "../../dist/index.js";

test("all Framework stream classes are constructable and factories retain siblings", async () => {
  assert.ok(DataStream.from([1]) instanceof DataStream);
  assert.ok(StringStream.fromArray(["a"]) instanceof StringStream);
  assert.ok(BufferStream.from([Buffer.from("a")]) instanceof BufferStream);
  assert.ok(NumberStream.from([1]) instanceof NumberStream);
  assert.ok(WindowStream.from([[1]]) instanceof WindowStream);
  assert.ok(MultiStream.from([]) instanceof MultiStream);
  assert.deepEqual(await StringStream.from("literal").toArray(), ["literal"]);
});

test("Framework classes remain subclassable", () => {
  class ChildDataStream extends DataStream {}
  class ChildStringStream extends StringStream {}
  class ChildBufferStream extends BufferStream {}
  class ChildMultiStream extends MultiStream {}
  class ChildNumberStream extends NumberStream {}
  class ChildWindowStream extends WindowStream {}

  assert.ok(ChildDataStream.from([1]) instanceof ChildDataStream);
  assert.ok(ChildStringStream.from("x") instanceof ChildStringStream);
  assert.ok(ChildBufferStream.from([Buffer.from("x")]) instanceof ChildBufferStream);
  assert.ok(ChildMultiStream.from([]) instanceof ChildMultiStream);
  assert.ok(ChildNumberStream.from([1]) instanceof ChildNumberStream);
  assert.ok(ChildWindowStream.from([[1]]) instanceof ChildWindowStream);
});

test("foundational cross-kind conversions return Framework siblings", async () => {
  const data = DataStream.from([{ value: 1 }]);
  assert.ok(data.JSONStringify() instanceof StringStream);
  assert.ok(data.CSVStringify() instanceof StringStream);
  assert.ok(StringStream.from('{"value":1}\n').JSONParse() instanceof DataStream);
  assert.ok(StringStream.from("value\n1\n").CSVParse() instanceof DataStream);
  assert.ok(StringStream.from("a\n").lines() instanceof DataStream);
  assert.deepEqual(await StringStream.from("literal").toArray(), ["literal"]);
});
