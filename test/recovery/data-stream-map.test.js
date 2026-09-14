import assert from "node:assert/strict";
import test from "node:test";
import { DataStream } from "../../dist/index.js";

test("DataStream.map retains async transform behavior", async () => {
  assert.deepEqual(await DataStream.from([1, 2, 3]).map(async (value) => value * 2).toArray(), [2, 4, 6]);
});
