import { expect, test } from "bun:test";
import { DataStream } from "../src/index.ts";

test("Bun loads the shared TypeScript source and runs a Framework pipeline", async () => {
  expect(await DataStream.from([1, 2]).map((value) => value + 1).toArray()).toEqual([2, 3]);
});
