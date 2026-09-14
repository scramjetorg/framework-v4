import assert from "node:assert/strict";
import test from "node:test";
import { DataStream } from "../../dist/index.js";

test("DataStream sequence, aggregation, and serialization waves", async () => {
  assert.deepEqual(await DataStream.from([1, 2, 3, 4]).slice(1, 2).toArray(), [2, 3]);
  assert.deepEqual(await DataStream.from([1, 2]).endWith(3).unshift(0).toArray(), [0, 1, 2, 3]);
  assert.deepEqual(await DataStream.from([1, 2]).concat(DataStream.from([3, 4])).toArray(), [1, 2, 3, 4]);
  assert.deepEqual(await DataStream.from([1, 2, 3]).join(0).toArray(), [1, 0, 2, 0, 3]);
  assert.deepEqual(await DataStream.from([1, 2, 3]).flatMap((value) => [value, value]).toArray(), [1, 1, 2, 2, 3, 3]);
  assert.deepEqual(await DataStream.from([[1, 2], [3]]).flatten().toArray(), [1, 2, 3]);
  assert.deepEqual(await DataStream.from([1, 2]).remap((emit, value) => { emit(value); emit(value + 10); }).toArray(), [1, 11, 2, 12]);
  assert.deepEqual(await DataStream.from([1, 2]).debug(() => {}).toArray(), [1, 2]);
  assert.deepEqual(await DataStream.from([1, 2]).rate(10000).toArray(), [1, 2]);
  assert.deepEqual(await DataStream.from([1, 2, 3]).batch(2).toArray(), [[1, 2], [3]]);
  assert.deepEqual(await DataStream.from([1, 2, 3]).timeBatch(1000).toArray(), [[1, 2, 3]]);
  assert.equal((await DataStream.from([1, 2, 3]).toJSONArray().toArray()).join(""), "[1,2,3]");
  assert.equal((await DataStream.from([{ a: 1 }]).toJSONObject((value) => "a").toArray()).join(""), '{"a":{"a":1}}');
});

test("DataStream keep/rewind, peek, assign, empty, and accumulate waves", async () => {
  const retained = DataStream.from([1, 2, 3, 4]).keep(2);
  const peeked = [];
  retained.peek(2, (values) => peeked.push(...values));
  assert.deepEqual(await retained.toArray(), [1, 2, 3, 4]);
  assert.deepEqual(peeked, [1, 2]);
  assert.deepEqual(await retained.rewind().toArray(), [3, 4]);
  assert.deepEqual(await DataStream.from([{ value: 1 }]).assign({ extra: true }).toArray(), [{ value: 1, extra: true }]);
  let emptyCalled = false;
  DataStream.from([]).empty(() => { emptyCalled = true; }).resume();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(emptyCalled, true);
  const total = { value: 0 };
  await DataStream.from([1, 2, 3]).accumulate((sum, value) => { sum.value += value; }, total);
  assert.equal(total.value, 6);
});

test("execution methods throw without starting a backend", () => {
  assert.throws(() => DataStream.from([1]).exec("echo unsafe"), /unavailable during recovery/);
  assert.throws(() => DataStream.from([1]).distribute(), /unavailable during recovery/);
});
