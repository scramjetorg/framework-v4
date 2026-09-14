import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("migration guide maps every required removed v4 surface", async () => {
  const migration = await readFile(new URL("../../MIGRATION.md", import.meta.url), "utf8");
  for (const surface of [
    "`plugin`",
    "`API`",
    "`createTransformModule`",
    "`createReadModule`",
    "String `use`",
    "String `from` module-loader overload",
    "String `pipeline` module-loader path",
    "`ReReadable`",
    "`exec`, `distribute`, `cluster`, worker/thread execution",
  ]) {
    assert.match(migration, new RegExp(surface.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  }
});
