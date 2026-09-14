import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import framework, {
  BufferStream,
  DataStream,
  MultiStream,
  NumberStream,
  StringStream,
  WindowStream,
} from "../../dist/index.js";

test("Framework classes live in dedicated modules and the root only composes exports", async () => {
  const source = await readFile(new URL("../../src/index.ts", import.meta.url), "utf8");
  assert.doesNotMatch(source, /export class (DataStream|StringStream|BufferStream|MultiStream|NumberStream|WindowStream)/);
  for (const module of ["data-stream", "string-stream", "buffer-stream", "multi-stream", "number-stream", "window-stream"]) {
    const implementation = await readFile(new URL(`../../src/${module}.ts`, import.meta.url), "utf8");
    assert.match(implementation, /export class/);
  }
  assert.match(source, /export default scramjet/);
});

test("root named exports and default-object classes remain identical", () => {
  for (const name of ["DataStream", "StringStream", "BufferStream", "MultiStream", "NumberStream", "WindowStream"]) {
    assert.equal(framework[name], { DataStream, StringStream, BufferStream, MultiStream, NumberStream, WindowStream }[name]);
  }
});
