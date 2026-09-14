import assert from "node:assert/strict";
import test from "node:test";
import { DataStream } from "../../dist/index.js";

test("supported inherited chain methods retain Framework DataStream identity", () => {
  const source = () => DataStream.from([1, 2]);
  const chains = [
    source().map((value) => value * 2),
    source().filter((value) => value > 0),
    source().do((value) => value),
    source().all([(value) => value]),
    source().race([(value) => value]),
    source().unorder((value) => value),
    source().copy(),
    source().while((value) => value < 3),
    source().until((value) => value > 3),
  ];

  for (const chain of chains) assert.ok(chain instanceof DataStream);
});
