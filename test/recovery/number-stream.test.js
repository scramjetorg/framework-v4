import assert from "node:assert/strict";
import test from "node:test";
import { NumberStream } from "../../dist/index.js";

test("NumberStream sum and avg retain numeric aggregation", async () => {
  const stream = NumberStream.from([1, 2, 3]);
  assert.equal(await stream.sum(), 6);
  assert.equal(await NumberStream.from([1, 2, 3]).avg(), 2);
});
