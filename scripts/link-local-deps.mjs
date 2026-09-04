import { access, mkdir, rm, symlink } from "node:fs/promises";

const dependencies = [
  {
    name: "scramjet-core",
    artifactPath: "../scramjet-core/dist",
    linkTarget: "../../scramjet-core/dist",
  },
  {
    name: "rereadable-stream",
    artifactPath: "../rereadable-stream/dist",
    linkTarget: "../../rereadable-stream/dist",
  },
];

for (const { artifactPath, name } of dependencies) {
  for (const file of ["index.js", "index.d.ts"]) {
    try {
      await access(`${artifactPath}/${file}`);
    } catch {
      throw new Error(
        `Missing ${name} generated artifact ${artifactPath}/${file}. Run "npm run build" in the sibling ${name} package before building scramjet.`,
      );
    }
  }
}

await mkdir("node_modules", { recursive: true });
for (const { linkTarget, name } of dependencies) {
  const path = `node_modules/${name}`;
  await rm(path, { force: true, recursive: true });
  await symlink(linkTarget, path, "dir");
}
