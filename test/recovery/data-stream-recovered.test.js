import assert from "node:assert/strict";
import test from "node:test";
import { Readable, Writable } from "node:stream";
import { BufferStream, DataStream, MultiStream, StringStream, WindowStream } from "../../dist/index.js";

test("DataStream.window emits partial snapshots and retains Framework type", async () => {
  const windows = DataStream.from([1, 2, 3]).window(2);
  assert.ok(windows instanceof WindowStream);
  assert.deepEqual(await windows.toArray(), [[1], [1, 2], [2, 3]]);
  for (const length of [0, -1, Number.NaN]) assert.throws(() => DataStream.from([]).window(length), /positive integer/);
});

test("DataStream.separateInto awaits caller targets without ending them", async () => {
  const targets = {
    even: { values: [], async whenWrote(value) { await new Promise((resolve) => setTimeout(resolve, 1)); this.values.push(value); } },
    odd: { values: [], async whenWrote(value) { this.values.push(value); } },
  };
  const source = DataStream.from([1, 2, 3, 4]);
  assert.equal(source.separateInto(targets, (value) => value % 2 === 0 ? "even" : "odd"), source);
  await source.toArray();
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.deepEqual(targets.even.values, [2, 4]);
  assert.deepEqual(targets.odd.values, [1, 3]);
});

test("DataStream.separateInto reports missing targets", async () => {
  const source = DataStream.from([1]);
  const error = new Promise((resolve) => source.once("error", resolve));
  source.separateInto({}, () => "missing");
  assert.match((await error).message, /Output for missing not found/);
});

test("DataStream pull and into preserve Framework streams and ordering", async () => {
  const target = DataStream.from([1, 2, 3]).into(
    (output, value) => output.pull(function* () { yield value; yield value + 10; }),
    new DataStream()
  );
  assert.ok(target instanceof DataStream);
  assert.deepEqual(await target.toArray(), [1, 2, 3, 11, 12, 13]);
  assert.deepEqual(await DataStream.from(Readable.from([4, 5])).toArray(), [4, 5]);
});

test("DataStream reduceNow returns the captured accumulator immediately", async () => {
  const source = DataStream.from([1, 2, 3]);
  const accumulator = [];
  assert.equal(source.reduceNow((out, value) => {
    out.push(value);
    return out;
  }, accumulator), accumulator);
  await source.toArray();
  assert.deepEqual(accumulator, [1, 2, 3]);
});

test("DataStream shift removes a prefix and separate creates Framework siblings", async () => {
  let shifted;
  const remainder = DataStream.from([0, 1, 2, 3]).shift(2, (values) => { shifted = values; });
  assert.ok(remainder instanceof DataStream);
  assert.deepEqual(await remainder.toArray(), [2, 3]);
  assert.deepEqual(shifted, [0, 1]);

  const separated = DataStream.from([0, 1, 2, 3]).separate((value) => `group-${value % 2}`);
  assert.ok(separated instanceof MultiStream);
  const streams = await new Promise((resolve) => setImmediate(() => resolve(separated.streams)));
  assert.deepEqual((await Promise.all(streams.map((stream) => stream.toArray()))).sort((a, b) => a[0] - b[0]), [[0, 2], [1, 3]]);

  const delayed = DataStream.from([0, 1, 2, 3]).separate(async (value) => {
    await new Promise((resolve) => setTimeout(resolve, 5));
    return `delayed-${value % 2}`;
  });
  await new Promise((resolve) => {
    const waitForBranches = () => delayed.streams.length === 2 ? resolve() : setImmediate(waitForBranches);
    waitForBranches();
  });
  assert.deepEqual((await Promise.all(delayed.streams.map((stream) => stream.toArray()))).sort((a, b) => a[0] - b[0]), [[0, 2], [1, 3]]);
});

test("DataStream stack emits newest buffered chunks first", async () => {
  const stacked = DataStream.from([0, 1, 2, 3]).stack();
  assert.deepEqual(await stacked.toArray(), [3, 2, 1, 0]);
});

test("Core DataStream methods retain Framework return and stream behavior", async () => {
  const source = DataStream.from([1, 2]);
  assert.equal(source.tee(() => {}), source);
  assert.deepEqual(await DataStream.fromIterator([1, 2][Symbol.iterator]()).toArray(), [1, 2]);
  assert.deepEqual(await DataStream.from([1, 2]).all([value => value + 1]).toArray(), [[2], [3]]);
  assert.deepEqual(await DataStream.from([1, 2, 3]).until(value => value === 2).toArray(), [1]);
  assert.deepEqual(await DataStream.from([1, 2, 3]).while(value => value < 3).toArray(), [1, 2]);
  assert.deepEqual(await DataStream.from([1, 2]).reduce((sum, value) => sum + value, 0), 3);
  assert.deepEqual(await DataStream.from([1, 2]).toBufferStream((value) => Buffer.from(String(value))).toArray(), [Buffer.from("1"), Buffer.from("2")]);
  assert.ok(DataStream.from([1]).toStringStream() instanceof StringStream);
  assert.ok(DataStream.from([1]).toBufferStream((value) => Buffer.from(String(value))) instanceof BufferStream);
  assert.equal(typeof DataStream.from([1]).raise, "function");
  assert.equal((await DataStream.from([{ value: 1 }]).JSONStringify().toArray()).join(""), '{"value":1}\n');
  const written = [];
  const writable = new Writable({ objectMode: true, write(value, _encoding, done) { written.push(value); done(); } });
  const teeSource = DataStream.from([7, 8]);
  assert.equal(teeSource.tee(writable), teeSource);
  await new Promise((resolve) => writable.once("finish", resolve));
  assert.deepEqual(written, [7, 8]);
  const captured = new DataStream();
  const branch = captured.map((value) => value * 2);
  captured.end(3);
  assert.deepEqual(await branch.toArray(), [6]);
});

test("DataStream race returns the fastest OFCA-composed operation", async () => {
  const captured = DataStream.from([1, 2]);
  const raced = captured.race([
    async (value) => { await new Promise((resolve) => setTimeout(resolve, 10)); return value + 10; },
    async (value) => { await new Promise((resolve) => setTimeout(resolve, 1)); return value + 20; }
  ]);
  assert.ok(raced instanceof DataStream);
  assert.deepEqual(await raced.toArray(), [21, 22]);
});

test("DataStream raise propagates an explicit stream error", async () => {
  const source = new DataStream();
  const error = new Error("raised");
  const raised = new Promise((resolve) => source.once("error", resolve));
  source.raise(error);
  assert.equal(await raised, error);
});

test("DataStream tap preserves the captured Framework stream", async () => {
  const source = DataStream.from([4, 5]);
  const tapped = source.tap();
  assert.equal(tapped, source);
  assert.deepEqual(await tapped.toArray(), [4, 5]);
});

test("DataStream lifecycle promises resolve on writes, reads, and end", async () => {
  const source = new DataStream();
  source.resume();
  const wrote = source.whenWrote(6);
  const read = source.whenRead();
  const finished = source.whenFinished();
  source.write(6);
  assert.equal(await wrote, undefined);
  assert.equal(await read, 6);
  source.end();
  await finished;
  const endedSource = DataStream.from([7]);
  const ended = endedSource.whenEnd();
  endedSource.resume();
  await ended;
});

test("DataStream catch and lifecycle promises remain available", async () => {
  let caught;
  const source = DataStream.from([1]).map(() => { throw new Error("recovered"); });
  const recovered = source.catch((error) => { caught = error; });
  recovered.resume();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(caught?.message, "recovered");
});
