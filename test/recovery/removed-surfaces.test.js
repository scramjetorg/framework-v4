import assert from "node:assert/strict";
import test from "node:test";
import framework from "../../dist/index.js";

test("REQ-005 removed plugin and string-module surfaces stay unavailable", () => {
  assert.equal("plugin" in framework, false);
  assert.equal("API" in framework, false);
  assert.equal("createTransformModule" in framework, false);
  assert.equal("createReadModule" in framework, false);
});
