import assert from "node:assert/strict";
import test from "node:test";
import { DataStream } from "../../dist/index.js";

test("DataStream.filter retains predicate behavior", async () => {
  assert.deepEqual(await DataStream.from([1, 2, 3, 4]).filter((value) => value % 2 === 0).toArray(), [2, 4]);
});
