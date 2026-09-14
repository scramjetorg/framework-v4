import { access } from "node:fs/promises";
import { spawn } from "node:child_process";

try {
  await access("dist/src/index.ts");
} catch {
  console.error("Bun check requires a current build with dist/src/index.ts.");
  process.exitCode = 1;
}

const result = spawn("bun", ["test", "test/package-bun.test.ts"], { stdio: "inherit" });
let unavailable = false;
result.on("error", (error) => {
  if (error.code === "ENOENT") {
    unavailable = true;
    console.warn("Bun unavailable; representative Bun test skipped.");
    return;
  }
  process.exitCode = 1;
});
result.on("exit", (code) => {
  if (!unavailable && code !== null && code !== 0) process.exitCode = code;
});
