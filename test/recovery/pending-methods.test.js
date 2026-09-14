import test from "node:test";

// These are retained v4 contracts outside the approved DataStream recovery wave.
const pending = [
  "StringStream.get",
];

for (const method of pending) test.skip(`${method}: retained, pending implementation (Phase 1)`, () => {});

// String/worker execution remains intentionally outside the DataStream wave.
for (const method of ["StringStream.exec", "cluster/worker execution"])
  test.skip(`${method}: REQ-009 throw-placeholder coverage`, () => {});
