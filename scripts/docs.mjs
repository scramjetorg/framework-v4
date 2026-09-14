import { execFileSync } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { mkdir, readFile, writeFile } from "node:fs/promises";

const require = createRequire(import.meta.url);
const root = dirname(fileURLToPath(import.meta.url));
const packageRoot = dirname(root);
const docsRoot = join(packageRoot, "docs");
const jsdocMain = join(dirname(require.resolve("jsdoc/package.json")), "jsdoc.js");
const files = [
  "index.js",
  "data-stream.js",
  "string-stream.js",
  "buffer-stream.js",
  "multi-stream.js",
  "number-stream.js",
  "window-stream.js",
].map((file) => join(packageRoot, ".build", file));
const check = process.argv.includes("--check");
const json = execFileSync(process.execPath, [jsdocMain, "-X", ...files], { cwd: packageRoot, encoding: "utf8" });
const doclets = JSON.parse(json).filter((doclet) => doclet.access !== "private" && !doclet.tags?.some((tag) => tag.title === "internal"));

const text = (value = "") => String(value).trim();
const typeText = (type) => type?.names?.join(" | ") ?? "";
const params = (doclet) => (doclet.params ?? []).map((param) => `${param.optional ? "[" : ""}${param.name}${param.optional ? "]" : ""}`).join(", ");
const methodMarkdown = (doclet) => {
  const signature = `${doclet.scope === "static" ? "static " : ""}${doclet.name}(${params(doclet)})`;
  const lines = [`### \`${signature}\``, "", text(doclet.description || doclet.longdescription) || "No description provided.", ""];
  if (doclet.params?.length) {
    lines.push("**Parameters**", "");
    for (const param of doclet.params) lines.push(`- \`${param.name}\` — ${typeText(param.type) || "unknown"}${param.description ? `: ${text(param.description)}` : ""}`);
    lines.push("");
  }
  if (doclet.returns?.length) lines.push(`**Returns:** \`${typeText(doclet.returns[0].type) || "unknown"}\` — ${text(doclet.returns[0].description)}`, "");
  return lines.join("\n");
};
const classMarkdown = (name) => {
  const classDoc = doclets.find((doclet) => doclet.kind === "class" && doclet.name === name);
  const members = doclets.filter((doclet) => doclet.memberof === name && ["function", "method"].includes(doclet.kind));
  const lines = [`# ${name}`, "", text(classDoc?.description || classDoc?.classdesc || classDoc?.longdescription) || "Framework stream class.", ""];
  if (classDoc?.params?.length) {
    lines.push("## Construction", "");
    for (const param of classDoc.params) lines.push(`- \`${param.name}\` — ${typeText(param.type) || "unknown"}${param.description ? `: ${text(param.description)}` : ""}`);
    lines.push("");
  }
  for (const member of members) lines.push(methodMarkdown(member));
  return `${lines.join("\n").replace(/\n{3,}/g, "\n\n").trim()}\n`;
};
const rootDoclets = doclets.filter((doclet) => doclet.meta?.filename === "index.js" && ["function", "member", "constant"].includes(doclet.kind) && doclet.description);
const rootLines = ["# Scramjet Framework API", "", "Public root exports for the Node.js 22+ Framework package.", ""];
for (const doclet of rootDoclets) {
  if (doclet.kind === "constant") rootLines.push(`### \`${doclet.name}\``, "", text(doclet.description), "");
  else rootLines.push(methodMarkdown(doclet));
}
const rootCompatibility = [
  ["PromiseTransformStream", "Core promise-aware Transform constructor exposed for advanced pipelines."],
  ["StreamError", "Framework stream error constructor."],
  ["errors", "Core error namespace retained for compatibility."],
  ["default", "Default object containing the complete public root API."],
];
rootLines.push("## Additional root exports", "");
for (const [name, description] of rootCompatibility) rootLines.push(`### \`${name}\``, "", description, "");
rootLines.push("## Stream classes", "", "See the class references for DataStream, StringStream, BufferStream, MultiStream, NumberStream, and WindowStream.", "");
const outputs = new Map([
  ["index.md", rootLines.join("\n").replace(/\n{3,}/g, "\n\n").trim() + "\n"],
  ...["DataStream", "StringStream", "BufferStream", "MultiStream", "NumberStream", "WindowStream"].map((name) => [`${name}.md`, classMarkdown(name)]),
]);
await mkdir(docsRoot, { recursive: true });
for (const [file, content] of outputs) {
  const destination = join(docsRoot, file);
  if (check) {
    const existing = await readFile(destination, "utf8");
    if (existing !== content) throw new Error(`Generated documentation is stale: ${file}`);
  } else await writeFile(destination, content);
}
if (!check) {
  const distDocs = join(packageRoot, "dist", "docs");
  await mkdir(distDocs, { recursive: true });
  for (const [file, content] of outputs) await writeFile(join(distDocs, file), content);
}
