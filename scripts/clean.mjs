import { rm } from "node:fs/promises";

await Promise.all([
  rm(".build", { force: true, recursive: true }),
  rm("dist", { force: true, recursive: true }),
]);
