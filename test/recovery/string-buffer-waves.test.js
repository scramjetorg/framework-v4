import assert from "node:assert/strict";
import test from "node:test";
import { BufferStream, DataStream, StringStream } from "../../dist/index.js";

test("StringStream split handles delimiter and UTF-8 fragmentation", async () => {
  const source = new StringStream();
  const split = source.split("|");
  source.write("one|");
  source.write("two|thr");
  source.end("ee");
  assert.ok(split instanceof StringStream);
  assert.deepEqual(await split.toArray(), ["one", "two", "three"]);

  const lines = new StringStream();
  const lineStream = lines.lines();
  lines.write("a\r");
  lines.end("\nb\n");
  assert.deepEqual(await lineStream.toArray(), ["a", "b"]);
});

test("StringStream match, shift/pop, append/prepend, and parse preserve classes", async () => {
  const matched = StringStream.fromString("aa ID:AB12 bb ID:CD34").match(/ID:([A-Z0-9]{4})/g);
  assert.ok(matched instanceof StringStream);
  assert.deepEqual(await matched.toArray(), ["AB12", "CD34"]);

  let shifted;
  const remainder = StringStream.fromString("abcdef").shift(2, (value) => { shifted = value; });
  assert.deepEqual(await remainder.toArray(), ["cdef"]);
  assert.equal(shifted, "ab");
  let popped;
  assert.deepEqual(await StringStream.fromString("wxyz").pop(1, (value) => { popped = value; }).toArray(), ["xyz"]);
  assert.equal(popped, "w");

  assert.deepEqual(await StringStream.fromString("a", "utf8").append((value) => value.toUpperCase()).toArray(), ["aA"]);
  assert.deepEqual(await StringStream.fromString("a").prepend("x").toArray(), ["xa"]);
  assert.deepEqual(await StringStream.fromString("1|2|").split("|").parse(Number).toArray(), [1, 2]);
  assert.ok(StringStream.fromString("1").toDataStream(Number) instanceof DataStream);
  const captured = new StringStream();
  const capturedParsed = captured.parse(Number);
  captured.end("9");
  assert.deepEqual(await capturedParsed.toArray(), [9]);
});

test("StringStream JSON and CSV parsing handle multiline and quoted records", async () => {
  assert.deepEqual(await StringStream.fromString('{"a":1}\n{"a":2}\n').JSONParse().toArray(), [{ a: 1 }, { a: 2 }]);
  const csv = StringStream.fromString('name,note\n"Ada","line one\nline two"\n"Bob","say ""hi"""\n');
  assert.deepEqual(await csv.CSVParse().toArray(), [
    { name: "Ada", note: "line one\nline two" },
    { name: "Bob", note: 'say "hi"' }
  ]);
});

test("BufferStream split, breakup, shift/pop, parse, and encoding preserve siblings", async () => {
  const source = new BufferStream();
  const split = source.split(Buffer.from("|"));
  source.write(Buffer.from("ab|c"));
  source.end(Buffer.from("d||ef"));
  assert.ok(split instanceof BufferStream);
  assert.deepEqual(await split.toArray(), [Buffer.from("ab"), Buffer.from("cd"), Buffer.from("ef")]);

  assert.deepEqual(await BufferStream.from([Buffer.from("abcdef")]).breakup(2).toArray(), [Buffer.from("ab"), Buffer.from("cd"), Buffer.from("ef")]);
  let shifted;
  assert.deepEqual(await BufferStream.from([Buffer.from("abcd")]).shift(2, (value) => { shifted = value; }).toArray(), [Buffer.from("cd")]);
  assert.deepEqual(shifted, Buffer.from("ab"));
  assert.deepEqual(await BufferStream.from([Buffer.from("abcd")]).pop(2, () => {}).toArray(), [Buffer.from("cd")]);
  assert.deepEqual(await BufferStream.from([Buffer.from("41")]).parse((value) => value.toString("ascii")).toArray(), ["41"]);
  assert.equal((await BufferStream.from([Buffer.from("café", "utf8")]).toStringStream("utf8").toArray()).join(""), "café");
  const fragmentedUtf8 = new BufferStream();
  const decoded = fragmentedUtf8.toStringStream();
  fragmentedUtf8.write(Buffer.from([0xc3]));
  fragmentedUtf8.end(Buffer.from([0xa9]));
  assert.deepEqual(await decoded.toArray(), ["é"]);
  const captured = new BufferStream();
  const capturedBroken = captured.breakup(2);
  captured.end(Buffer.from("1234"));
  assert.deepEqual(await capturedBroken.toArray(), [Buffer.from("12"), Buffer.from("34")]);
});

test("String and Buffer transforms propagate parser errors", async () => {
  await assert.rejects(StringStream.fromString("not-json\n").JSONParse().toArray(), /not valid JSON/);
  await assert.rejects(StringStream.fromString('"unterminated\n').CSVParse().toArray(), /Unterminated quoted CSV cell/);
});
