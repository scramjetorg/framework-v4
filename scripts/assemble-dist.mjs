import { cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";

const sourceManifest = JSON.parse(await readFile("package.json", "utf8"));
const distManifest = {
  name: sourceManifest.name,
  version: sourceManifest.version,
  description: sourceManifest.description,
  type: "module",
  exports: {
    ".": {
      types: "./index.d.ts",
      import: "./index.js",
    },
  },
  types: "./index.d.ts",
  engines: sourceManifest.engines,
  dependencies: sourceManifest.dependencies,
  author: sourceManifest.author,
  license: sourceManifest.license,
  repository: sourceManifest.repository,
};

await rm("dist", { force: true, recursive: true });
await mkdir("dist", { recursive: true });
await Promise.all([
  cp(".build/index.js", "dist/index.js"),
  cp(".build/index.d.ts", "dist/index.d.ts"),
  cp("README.md", "dist/README.md"),
  cp("LICENSE", "dist/LICENSE"),
  writeFile("dist/package.json", `${JSON.stringify(distManifest, null, 2)}\n`),
]);
