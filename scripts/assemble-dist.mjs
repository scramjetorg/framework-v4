import { cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";

const sourceManifest = JSON.parse(await readFile("package.json", "utf8"));
const distManifest = {
  name: sourceManifest.name,
  version: sourceManifest.version,
  description: sourceManifest.description,
  type: "module",
  exports: {
    ".": {
      bun: {
        types: "./src/index.ts",
        import: "./src/index.ts",
      },
      node: {
        types: "./index.d.ts",
        import: "./index.js",
      },
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
  cp(".build", "dist", { recursive: true }),
  cp("src", "dist/src", { recursive: true }),
  cp("README.md", "dist/README.md"),
  cp("MIGRATION.md", "dist/MIGRATION.md"),
  cp("LICENSE", "dist/LICENSE"),
  writeFile("dist/package.json", `${JSON.stringify(distManifest, null, 2)}\n`),
]);
