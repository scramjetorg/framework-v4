import assert from "node:assert/strict";
import test from "node:test";
import { DataStream } from "../../dist/index.js";

test("DataStream factories retain array and iterator input", async () => {
  assert.deepEqual(await DataStream.fromArray([1, 2]).toArray(), [1, 2]);
  assert.deepEqual(await DataStream.fromIterator([3, 4][Symbol.iterator]()).toArray(), [3, 4]);
});
