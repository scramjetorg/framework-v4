import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";

const modulePath = process.argv[2];
const send = (message: unknown): void => { process.stdout.write(`${JSON.stringify(message)}\n`); };
const dynamicImport = new Function("specifier", "return import(specifier)") as (specifier: string) => Promise<Record<string, any>>;
const main = async (): Promise<void> => {
  const loaded = await dynamicImport(pathToFileURL(modulePath).href);
  const transform = loaded.default ?? loaded;
  send({ type: "ready" });
  for await (const line of createInterface({ input: process.stdin })) {
    if (!line.trim()) continue;
    try {
      const message = JSON.parse(line);
      if (message.type === "end") break;
      if (message.type !== "input" || typeof message.ref !== "number") throw new Error("Invalid fork transform input");
      const value = await transform(message.value, { ref: message.ref });
      send({ type: "output", ref: message.ref, value });
    } catch (error) {
      send({ type: "error", error: { message: error instanceof Error ? error.message : String(error), stack: error instanceof Error ? error.stack : undefined } });
      process.exitCode = 1;
      break;
    }
  }
};
void main().catch((error) => { send({ type: "error", error: { message: String(error) } }); process.exitCode = 1; });
