import assert from "node:assert/strict";
import test from "node:test";
import { resolve } from "node:path";
import { DataStream, StringStream } from "../../dist/index.js";
import { forkTransform } from "../../dist/execution.js";

const fixture = (name) => resolve("test/fixtures", name);
const nodeJson = `${process.execPath} -e "process.stdin.on('data', d => d.toString().split('\\n').filter(Boolean).forEach(line => process.stdout.write(JSON.stringify(JSON.parse(line) + 10) + '\\n')))"`;

test("DataStream.exec exchanges JSON Lines with a command", async () => {
  assert.deepEqual(await DataStream.from([1, 2]).exec(nodeJson).toArray(), [11, 12]);
});

test("StringStream.exec exchanges raw strings with a command", async () => {
  assert.deepEqual((await StringStream.from("a").exec("tr a-z A-Z").toArray()).join(""), "A");
});

test("distribute emits forked results in completion order and accepts CJS", async () => {
  const values = await DataStream.from([1, 2, 3, 4]).distribute(fixture("execution-transform.cjs"), { concurrency: 2 }).toArray();
  assert.deepEqual(values.map((item) => item.value).sort((a, b) => a - b), [2, 4, 6, 8]);
});

test("delegate preserves input order and accepts ESM", async () => {
  const values = await DataStream.from([1, 2, 3, 4]).delegate(fixture("execution-transform.mjs"), { concurrency: 3 }).toArray();
  assert.deepEqual(values, [2, 3, 4, 5].map((value, index) => ({ value, ref: index + 1 })));
});

test("forked execution propagates child errors and aborts", async () => {
  await assert.rejects(DataStream.from([1, 2]).delegate(fixture("execution-error.cjs")).toArray(), /fixture failure/);
  const controller = new AbortController();
  const aborted = DataStream.from([1]).delegate(fixture("execution-transform.cjs"), { signal: controller.signal }).toArray();
  setImmediate(() => controller.abort(new Error("cancelled")));
  await assert.rejects(aborted, /cancelled/);
});

test("ForkOptions rejects invalid concurrency and honors an already-aborted signal", async () => {
  for (const concurrency of [Infinity, Number.NaN, 1.5, 0, -1]) {
    assert.throws(() => DataStream.from([1]).delegate(fixture("execution-transform.cjs"), { concurrency }), /finite positive safe integer/);
  }
  const controller = new AbortController();
  controller.abort(new Error("already cancelled"));
  await assert.rejects(DataStream.from([1]).exec(nodeJson, { signal: controller.signal }).toArray(), /already cancelled/);
  await assert.rejects(DataStream.from([1]).delegate(fixture("execution-transform.cjs"), { signal: controller.signal }).toArray(), /already cancelled/);
});

test("delegate rolls refs over without colliding with reserved results", async () => {
  const output = forkTransform(
    DataStream.from([1, 2, 3]),
    fixture("execution-transform.mjs"),
    () => new DataStream(),
    true,
    { concurrency: 2 },
    0xfffffffe,
  );
  assert.deepEqual(await output.toArray(), [
    { value: 2, ref: 0xffffffff },
    { value: 3, ref: 0 },
    { value: 4, ref: 1 },
  ]);
});

test("fork finalization rejects abrupt exits and residual NDJSON", async () => {
  await assert.rejects(DataStream.from([1]).delegate(fixture("execution-abrupt.cjs")).toArray(), /assigned task|exited/);
  await assert.rejects(DataStream.from([1]).delegate(fixture("execution-partial.cjs")).toArray(), /partial NDJSON|protocol/);
});

test("fork finalization waits through output backpressure and ends once", async () => {
  const output = forkTransform(
    DataStream.from([1, 2, 3, 4]),
    fixture("execution-transform.mjs"),
    () => new DataStream({ highWaterMark: 1 }),
    false,
    { concurrency: 2 },
  );
  let ends = 0;
  output.on("end", () => { ends += 1; });
  assert.equal((await output.toArray()).length, 4);
  assert.equal(ends, 1);
});

test("ordered delegate flushes buffered results after output drain at source end", async () => {
  const output = forkTransform(
    DataStream.from([1, 2]),
    fixture("execution-ordered-backpressure.mjs"),
    () => {
      const stream = new DataStream();
      const write = stream.write.bind(stream);
      let blocked = true;
      stream.write = ((value) => {
        const accepted = write(value);
        if (blocked) {
          blocked = false;
          setImmediate(() => stream.emit("drain"));
          return false;
        }
        return accepted;
      });
      return stream;
    },
    true,
    { concurrency: 2 },
  );
  assert.deepEqual(await output.toArray(), [{ value: 1, ref: 1 }, { value: 2, ref: 2 }]);
});
