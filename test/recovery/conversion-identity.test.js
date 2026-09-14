import assert from "node:assert/strict";
import test from "node:test";
import { BufferStream, DataStream, MultiStream, StringStream } from "../../dist/index.js";

test("DataStream conversions return Framework BufferStream and StringStream siblings", async () => {
  const data = DataStream.from(["a", "b"]);
  const buffers = data.toBufferStream((value) => Buffer.from(value));
  const strings = DataStream.from([1, 2]).stringify();

  assert.ok(buffers instanceof BufferStream);
  assert.ok(strings instanceof StringStream);
  assert.deepEqual(await buffers.toArray(), [Buffer.from("a"), Buffer.from("b")]);
  assert.deepEqual(await strings.toArray(), ["1", "2"]);
});

test("Buffer and String conversions return Framework DataStream siblings", async () => {
  const buffer = BufferStream.from([Buffer.from("1"), Buffer.from("2")]);
  const parsed = buffer.toDataStream((value) => Number(value.toString()));
  const string = StringStream.from(["1", "2"]);
  const stringParsed = string.toDataStream((value) => Number(value));

  assert.ok(parsed instanceof DataStream);
  assert.ok(stringParsed instanceof DataStream);
  assert.ok(buffer.toStringStream() instanceof StringStream);
  assert.ok(string.toBufferStream() instanceof BufferStream);
  assert.deepEqual(await parsed.toArray(), [1, 2]);
  assert.deepEqual(await stringParsed.toArray(), [1, 2]);
});

test("MultiStream factory and mux return Framework siblings", async () => {
  const multi = MultiStream.from([[1, 2], [3, 4]]);
  const muxed = multi.mux();

  assert.ok(multi instanceof MultiStream);
  assert.ok(muxed instanceof DataStream);
  assert.deepEqual(await muxed.toArray(), [1, 2, 3, 4]);
});
