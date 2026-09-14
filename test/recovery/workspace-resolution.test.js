import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { access } from "node:fs/promises";
import test from "node:test";

test("workspace dependencies resolve through their declared package exports", async () => {
  // Core is currently published as a CommonJS package; use the same resolver
  // its runtime consumer uses rather than requiring an ESM export condition.
  const core = createRequire(import.meta.url).resolve("scramjet-core");
  const ofca = createRequire(core).resolve("@scramjet/ofca");

  assert.match(core, /\/scramjet-core\/lib\/index\.js$/);
  assert.match(ofca, /\/ofca\/dist\/index\.js$/);
  await access(core);
  await access(ofca);
});
