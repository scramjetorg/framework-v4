import test from "node:test";

// These are retained v4 contracts outside the approved DataStream recovery wave.
const pending = [
  "StringStream.get",
];

for (const method of pending) test.skip(`${method}: retained, pending implementation (Phase 1)`, () => {});

// Legacy cluster/worker execution remains outside the approved Node fork wave.
for (const method of ["cluster/worker execution"])
  test.skip(`${method}: REQ-009 throw-placeholder coverage`, () => {});
