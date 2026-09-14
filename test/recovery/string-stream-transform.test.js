import assert from "node:assert/strict";
import test from "node:test";
import { StringStream } from "../../dist/index.js";

test("StringStream append and prepend support values and callbacks", async () => {
  assert.deepEqual(await StringStream.from(["a", "b"]).append("!").prepend((value) => value.toUpperCase()).toArray(), ["A!a!", "B!b!"]);
});

test("StringStream.JSONParse parses one JSON value per line", async () => {
  assert.deepEqual(await StringStream.from('{"a":1}\n{"a":2}\n').JSONParse().toArray(), [{ a: 1 }, { a: 2 }]);
});
