import { fork, spawn, type ChildProcess } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import type { DataStream } from "./data-stream.js";
import type { StringStream } from "./string-stream.js";

export interface ExecOptions {
  cwd?: string;
  env?: NodeJS.ProcessEnv;
  signal?: AbortSignal;
  shell?: boolean | string;
  [key: string]: unknown;
}

export interface ForkOptions {
  concurrency?: number;
  signal?: AbortSignal;
}

type OutputFactory<T> = () => T;

const packageRequire = createRequire(pathToFileURL(`${process.cwd()}/package.json`));

function outputError(output: NodeJS.EventEmitter & { destroy(error?: Error): unknown }, error: unknown): void {
  output.destroy(error instanceof Error ? error : new Error(String(error)));
}

export function execJson<Chunk, Output>(source: DataStream<Chunk>, command: string, createOutput: OutputFactory<Output>, options: ExecOptions = {}): Output {
  const output = createOutput();
  if (options.signal?.aborted) {
    queueMicrotask(() => outputError(output as any, options.signal?.reason ?? new Error("Execution aborted")));
    return output;
  }
  const child = spawn(command, { ...options, shell: options.shell ?? true, stdio: ["pipe", "pipe", "pipe"] } as any);
  let inputPending = Promise.resolve();
  let stdout = "";
  let failed = false;
  const fail = (error: unknown): void => {
    if (failed) return;
    failed = true;
    child.kill();
    outputError(output as any, error);
  };
  child.stdout?.setEncoding("utf8");
  child.stdout?.on("data", (part: string) => {
    stdout += part;
    const lines = stdout.split(/\r?\n/);
    stdout = lines.pop() ?? "";
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        if (!(output as any).write(JSON.parse(line))) (output as any).once("drain", () => undefined);
      } catch (error) { fail(error); }
    }
  });
  child.stderr?.resume();
  child.on("error", fail);
  child.on("close", (code, signal) => {
    if (failed) return;
    if (stdout.trim()) {
      try { (output as any).write(JSON.parse(stdout)); } catch (error) { fail(error); return; }
    }
    if (code !== 0) { fail(new Error(`Command exited with ${signal ?? `code ${code}`}`)); return; }
    (output as any).end();
  });
  options.signal?.addEventListener("abort", () => { fail(options.signal?.reason ?? new Error("Execution aborted")); }, { once: true });
  source.on("data", (chunk: Chunk) => {
    source.pause();
    inputPending = inputPending.then(async () => {
      if (failed) return;
      const line = `${JSON.stringify(chunk)}\n`;
      if (!child.stdin.write(line)) await new Promise<void>((resolve) => child.stdin.once("drain", resolve));
      source.resume();
    }).catch(fail);
  });
  source.once("end", () => inputPending.then(() => child.stdin.end()).catch(fail));
  source.once("error", fail);
  return output;
}

export function execRaw(source: StringStream, command: string, createOutput: OutputFactory<StringStream>, options: ExecOptions = {}): StringStream {
  const output = createOutput();
  if (options.signal?.aborted) {
    queueMicrotask(() => outputError(output, options.signal?.reason ?? new Error("Execution aborted")));
    return output;
  }
  const child = spawn(command, { ...options, shell: options.shell ?? true, stdio: ["pipe", "pipe", "pipe"] } as any);
  let failed = false;
  const fail = (error: unknown): void => { if (!failed) { failed = true; child.kill(); outputError(output, error); } };
  child.stdout?.setEncoding("utf8");
  child.stdout?.on("data", (part: string) => { if (!failed) (output as any).write(part); });
  child.stderr?.resume();
  child.on("error", fail);
  child.on("close", (code, signal) => {
    if (failed) return;
    if (code !== 0) { fail(new Error(`Command exited with ${signal ?? `code ${code}`}`)); return; }
    output.end();
  });
  options.signal?.addEventListener("abort", () => fail(options.signal?.reason ?? new Error("Execution aborted")), { once: true });
  let pending = Promise.resolve();
  source.on("data", (chunk: string) => {
    source.pause();
    pending = pending.then(async () => {
      if (failed) return;
      if (!child.stdin.write(chunk)) await new Promise<void>((resolve) => child.stdin.once("drain", resolve));
      source.resume();
    }).catch(fail);
  });
  source.once("end", () => pending.then(() => child.stdin.end()).catch(fail));
  source.once("error", fail);
  return output;
}

interface WorkerMessage { type: "ready" | "output" | "error"; ref?: number; value?: unknown; error?: { message?: string; stack?: string }; }
interface Task { ref?: number; value: unknown; worker?: Worker; }
interface Worker { child: ChildProcess; task?: Task; ready: boolean; endSent: boolean; closed: boolean; buffer: string; }

function runnerPath(): string {
  const entry = packageRequire.resolve("scramjet");
  const root = dirname(entry);
  return entry.endsWith(".cjs") ? join(root, "cjs", "fork-runner.js") : join(root, "fork-runner.js");
}

export function forkTransform<Chunk, Output>(source: DataStream<Chunk>, modulePath: string, createOutput: OutputFactory<Output>, ordered: boolean, options: ForkOptions = {}, refSeed = 0): Output {
  const output = createOutput();
  const requestedConcurrency = options.concurrency ?? 1;
  if (!Number.isSafeInteger(requestedConcurrency) || requestedConcurrency <= 0) {
    throw new RangeError("Fork concurrency must be a finite positive safe integer");
  }
  if (options.signal?.aborted) {
    queueMicrotask(() => outputError(output as any, options.signal?.reason ?? new Error("Execution aborted")));
    return output;
  }
  if (!Number.isSafeInteger(refSeed) || refSeed < 0 || refSeed > 0xffffffff) throw new RangeError("Invalid fork ref seed");
  const concurrency = requestedConcurrency;
  const workers: Worker[] = [];
  const queued: Task[] = [];
  const results = new Map<number, unknown>();
  const reserved = new Set<number>();
  let nextRef = refSeed >>> 0;
  let expected = (refSeed + 1) >>> 0;
  let ended = false;
  let failed = false;
  let ending = false;
  let finalized = false;
  let blocked = false;
  const fail = (error: unknown): void => {
    if (failed || finalized) return;
    failed = true;
    for (const worker of workers) worker.child.kill();
    outputError(output as any, error);
  };
  const finish = (): void => {
    if (finalized || failed || blocked || !ending || workers.some((worker) => !worker.closed)) return;
    finalized = true;
    (output as any).end();
  };
  const writeResult = (ref: number, value: unknown): void => {
    if (failed) return;
    reserved.delete(ref);
    if (!(output as any).write(value)) {
      blocked = true;
      (output as any).once("drain", () => { if (!finalized && !failed) { blocked = false; pump(); } });
    }
  };
  const flushOrdered = (): void => {
    if (!ordered || blocked) return;
    while (results.has(expected) && !blocked) {
      const result = results.get(expected);
      results.delete(expected);
      writeResult(expected, result);
      expected = (expected + 1) >>> 0;
    }
  };
  const complete = (worker: Worker, task: Task, value: unknown): void => {
    if (task.ref === undefined || !reserved.has(task.ref)) { fail(new Error("Invalid or duplicate fork transform output")); return; }
    worker.task = undefined;
    if (ordered) {
      if (results.has(task.ref)) { fail(new Error("Duplicate fork transform output ref")); return; }
      results.set(task.ref, value);
      flushOrdered();
    } else writeResult(task.ref, value);
    pump();
  };
  const dispatch = (worker: Worker, task: Task): void => {
    const candidate = (nextRef + 1) >>> 0;
    if (reserved.has(candidate)) return;
    nextRef = candidate;
    task.ref = candidate;
    reserved.add(candidate);
    worker.task = task;
    task.worker = worker;
    const stdin = worker.child.stdin;
    if (!stdin) { fail(new Error("Forked transform stdin is unavailable")); return; }
    if (!stdin.write(`${JSON.stringify({ type: "input", ref: task.ref, value: task.value })}\n`)) stdin.once("drain", pump);
  };
  const pump = (): void => {
    if (failed || finalized) return;
    flushOrdered();
    if (blocked) return;
    for (const worker of workers) {
      if (!worker.ready || worker.task || !queued.length) continue;
      const task = queued[0];
      const candidate = (nextRef + 1) >>> 0;
      if (reserved.has(candidate)) break;
      queued.shift();
      dispatch(worker, task);
    }
    if (ended && !queued.length && workers.every((worker) => !worker.task) && (!ordered || !results.size) && !ending) {
      ending = true;
      for (const worker of workers) {
        if (worker.endSent) continue;
        worker.endSent = true;
        worker.child.stdin?.write('{"type":"end"}\n');
        worker.child.stdin?.end();
      }
      finish();
    } else if (!queued.length && workers.some((worker) => !worker.task && worker.ready)) source.resume();
  };
  const handle = (worker: Worker, message: WorkerMessage): void => {
    if (finalized || worker.closed) { fail(new Error("Forked transform message after finalization")); return; }
    if (message.type === "ready") { if (worker.ready) { fail(new Error("Duplicate fork transform ready message")); return; } worker.ready = true; pump(); return; }
    if (message.type === "error") { fail(new Error(message.error?.message ?? "Forked transform failed")); return; }
    if (message.type !== "output" || !worker.task || message.ref !== worker.task.ref) { fail(new Error("Invalid fork transform protocol message")); return; }
    complete(worker, worker.task, message.value);
  };
  for (let index = 0; index < concurrency; index += 1) {
    const child = fork(runnerPath(), [modulePath], { silent: true });
    const worker: Worker = { child, ready: false, endSent: false, closed: false, buffer: "" };
    workers.push(worker);
    child.stdout?.setEncoding("utf8");
    child.stdout?.on("data", (part: string) => {
      if (worker.closed) return;
      worker.buffer += part;
      const lines = worker.buffer.split(/\r?\n/); worker.buffer = lines.pop() ?? "";
      for (const line of lines) if (line.trim()) { try { handle(worker, JSON.parse(line)); } catch (error) { fail(error); } }
    });
    child.stderr?.resume();
    child.on("error", fail);
    child.on("close", (code) => {
      worker.closed = true;
      if (worker.buffer.trim()) { fail(new Error("Forked transform closed with partial NDJSON")); return; }
      if (worker.task) { fail(new Error("Forked transform exited with an assigned task")); return; }
      if (!worker.endSent || code !== 0) { fail(new Error(`Forked transform exited with code ${code}`)); return; }
      finish();
    });
  }
  source.on("data", (value: Chunk) => {
    source.pause();
    queued.push({ value });
    pump();
  });
  source.once("end", () => { ended = true; pump(); });
  source.once("error", fail);
  options.signal?.addEventListener("abort", () => fail(options.signal?.reason ?? new Error("Execution aborted")), { once: true });
  return output;
}
