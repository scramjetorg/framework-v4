import assert from "node:assert/strict";
import test from "node:test";
import { StringStream } from "../../dist/index.js";

test("StringStream.lines handles chunk boundaries", async () => {
  assert.deepEqual(await StringStream.from(["a\n", "b\n", "c"]).lines().toArray(), ["a", "b", "c"]);
});
