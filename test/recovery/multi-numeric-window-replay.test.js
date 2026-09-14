import assert from "node:assert/strict";
import test from "node:test";
import { DataStream, MultiStream, NumberStream, WindowStream } from "../../dist/index.js";

test("MultiStream construction, add/remove, map, filter, and mux retain Framework classes", async () => {
  const first = DataStream.from([1, 2]);
  const second = DataStream.from([3, 4]);
  const multi = new MultiStream();
  assert.equal(multi.add(first), multi);
  assert.equal(multi.add(second), multi);
  assert.equal(multi.length, 2);

  const mapped = await multi.map((stream) => stream.map((value) => value * 10));
  assert.ok(mapped instanceof MultiStream);
  assert.deepEqual((await Promise.all(mapped.streams.map((stream) => stream.toArray()))).flat(), [10, 20, 30, 40]);

  const filtered = await MultiStream.from([[1], [2]]).filter(() => true);
  assert.ok(filtered instanceof MultiStream);
  assert.equal(filtered.length, 2);
  assert.ok(multi.mux() instanceof DataStream);
  const managed = new MultiStream();
  managed.add(first).add(second);
  assert.equal(managed.remove(first), managed);
  assert.equal(managed.length, 1);
});

test("MultiStream.dedupe returns one entry per retained stream reference", () => {
  const shared = DataStream.from([1]);
  const distinct = DataStream.from([1]);
  const multi = new MultiStream([shared, shared, distinct, shared]);
  const deduped = multi.dedupe();

  assert.ok(deduped instanceof MultiStream);
  assert.deepEqual(deduped.streams, [shared, distinct]);
  assert.equal(multi.length, 4);
});

test("MultiStream retained demux/mux transitions preserve stream values", async () => {
  const demuxed = DataStream.from([0, 1, 2, 3]).separate((value) => value % 2);
  assert.ok(demuxed instanceof MultiStream);
  const muxed = demuxed.mux();
  assert.ok(muxed instanceof DataStream);
  assert.deepEqual((await muxed.toArray()).sort((a, b) => a - b), [0, 1, 2, 3]);
});

test("NumberStream supports valueOf aggregation and WindowStream projections", async () => {
  const weighted = NumberStream.from([{ value: 2 }, { value: 4 }], { valueOf: (item) => item.value * 2 });
  assert.ok(weighted instanceof NumberStream);
  assert.equal(await weighted.sum(), 12);
  assert.equal(await NumberStream.from([{ value: 2 }, { value: 4 }], { valueOf: (item) => item.value }).avg(), 3);

  const windows = WindowStream.from([[1, 2], [3, 4]]);
  const sums = windows.sum();
  assert.ok(sums instanceof NumberStream);
  assert.deepEqual(await sums.toArray(), [3, 7]);
  const averages = WindowStream.from([[1, 2], [3, 4]]).avg();
  assert.ok(averages instanceof NumberStream);
  assert.deepEqual(await averages.toArray(), [1.5, 3.5]);
});

async function collect(readable) {
  const chunks = [];
  for await (const chunk of readable) chunks.push(chunk);
  return chunks;
}

test("DataStream keep provides independent live object replay readers", async () => {
  const source = new DataStream({ objectMode: true });
  source.keep(4);
  const first = source.rewind();
  const second = source.rewind();
  source.write({ id: 1 });
  source.write({ id: 2 });
  source.end({ id: 3 });

  assert.deepEqual(await collect(first), [{ id: 1 }, { id: 2 }, { id: 3 }]);
  assert.deepEqual(await collect(second), [{ id: 1 }, { id: 2 }, { id: 3 }]);
});

test("DataStream tail starts from retained chunks and follows future chunks", async () => {
  const source = new DataStream({ objectMode: true });
  source.keep(4);
  source.write("first");
  source.write("second");
  await new Promise((resolve) => setImmediate(resolve));
  const replay = collect(source.tail(1));
  source.write("third");
  source.end("fourth");

  assert.deepEqual(await replay, ["second", "third", "fourth"]);
});

test("DataStream tail(0) excludes retained history and follows future chunks", async () => {
  const source = new DataStream({ objectMode: true });
  source.keep(4);
  source.write("first");
  source.write("second");
  await new Promise((resolve) => setImmediate(resolve));
  const replay = collect(source.tail(0));
  source.write("third");
  source.end("fourth");

  assert.deepEqual(await replay, ["third", "fourth"]);
});

test("keeping a mapped stream replays only transformed output", async () => {
  const transformed = DataStream.from([1, 2, 3]).map((value) => value * 10).keep(2);

  assert.deepEqual(await transformed.toArray(), [10, 20, 30]);
  assert.deepEqual(await collect(transformed.rewind()), [20, 30]);
});

test("DataStream keep replays binary chunks", async () => {
  const source = new DataStream({ objectMode: true });
  source.keep(4);
  source.write(Buffer.from("one"));
  source.end(Buffer.from("two"));

  assert.equal(Buffer.concat(await collect(source.rewind())).toString(), "onetwo");
});

test("DataStream keep supports zero length and source drop events", async () => {
  const source = new DataStream({ objectMode: true });
  source.keep(0);
  const drops = [];
  source.on("drop", (count) => drops.push(count));
  source.write("first");
  source.end("second");

  assert.deepEqual(await collect(source.rewind()), []);
  assert.deepEqual(drops, [1, 1]);
});

test("DataStream keep rejects invalid bounded-history lengths", () => {
  for (const length of [1.5, Number.NaN, "2"]) {
    assert.throws(() => DataStream.from([]).keep(length), RangeError);
  }
});

test("DataStream keep reports drops to lagging readers", async () => {
  const source = new DataStream({ objectMode: true });
  source.keep(2);
  source.write(1);
  source.write(2);
  await new Promise((resolve) => setImmediate(resolve));
  const replay = source.rewind();
  const drops = [];
  replay.on("drop", (count) => drops.push(count));
  source.end(3);

  assert.deepEqual(await collect(replay), [2, 3]);
  assert.deepEqual(drops, [1]);
});

test("DataStream replay readers honor readable backpressure while following writes", async () => {
  const source = new DataStream({ objectMode: true, highWaterMark: 1 });
  source.keep(8);
  const replay = source.rewind();
  source.write(1);
  source.write(2);
  source.write(3);
  source.end(4);

  assert.equal(replay.readableHighWaterMark, 1);
  assert.deepEqual(await collect(replay), [1, 2, 3, 4]);
});

test("DataStream keep propagates source errors to active readers", async () => {
  const source = new DataStream({ objectMode: true });
  source.keep(2);
  const replay = source.rewind();
  const error = new Error("source failed");
  const observed = new Promise((resolve) => replay.once("error", resolve));
  source.destroy(error);

  assert.equal(await observed, error);
});
