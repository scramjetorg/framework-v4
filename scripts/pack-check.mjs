import { access, mkdir, rm, writeFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";

const run = promisify(execFile);
const root = resolve(".");
const checkRoot = resolve(".pack-check");
const consumer = resolve(checkRoot, "consumer");
const tarDir = resolve(checkRoot, "tar");
await rm(checkRoot, { force: true, recursive: true });
await mkdir(consumer, { recursive: true });
await mkdir(tarDir, { recursive: true });

const pack = async (path) => {
  const { stdout } = await run("npm", ["pack", path, "--pack-destination", tarDir], { cwd: root });
  return resolve(tarDir, stdout.trim().split(/\s+/).at(-1));
};
const archive = await pack("./dist");
const coreArchive = await pack("../scramjet-core");
const ofcaArchive = await pack("../ofca/dist");
const packageJson = {
  name: "scramjet-packed-consumer",
  private: true,
  type: "module",
};
await writeFile(resolve(consumer, "package.json"), `${JSON.stringify(packageJson, null, 2)}\n`);

await run("npm", [
  "install", "--ignore-scripts", "--no-package-lock", "--no-save",
  archive,
  coreArchive,
  ofcaArchive,
], { cwd: consumer, stdio: "inherit" });

try {
  await access(resolve(consumer, "node_modules/rereadable-stream"));
  throw new Error("Packed consumer unexpectedly installed rereadable-stream");
} catch (error) {
  if (error.code !== "ENOENT") throw error;
}

const nodeFixture = [
  'import assert from "node:assert/strict";',
  'import { DataStream } from "scramjet";',
  'const resolved = import.meta.resolve("scramjet");',
  'assert.match(resolved, /scramjet[\\/]index\\.js$/);',
  'assert.deepEqual(await DataStream.from([1, 2]).map((value) => value * 2).toArray(), [2, 4]);',
  'await assert.rejects(() => import("scramjet/src/index.ts"));',
].join("\n");
await writeFile(resolve(consumer, "node-consumer.mjs"), `${nodeFixture}\n`);
await run(process.execPath, ["node-consumer.mjs"], { cwd: consumer, stdio: "inherit" });

const bunFixture = [
  'import { DataStream } from "scramjet";',
  'if (!import.meta.resolve("scramjet").endsWith("/src/index.ts")) throw new Error("Bun did not select the TypeScript export");',
  'const values = await DataStream.from([2, 3]).map((value) => value + 1).toArray();',
  'if (values.join(",") !== "3,4") throw new Error("Unexpected packed Bun pipeline result");',
].join("\n");
await writeFile(resolve(consumer, "bun-consumer.ts"), `${bunFixture}\n`);

try {
  await run("bun", ["run", "bun-consumer.ts"], { cwd: consumer, stdio: "inherit" });
} catch (error) {
  if (error.code === "ENOENT") console.warn("Bun unavailable; packed Bun consumer validation skipped.");
  else throw error;
}

console.log("Packed Node/Bun consumer validation complete.");
await rm(checkRoot, { force: true, recursive: true });
