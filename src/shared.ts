import { Readable } from "node:stream";
import type { ReadableOptions, Transform, TransformOptions } from "node:stream";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

export type CoreModule = typeof import("scramjet-core");
export type CoreDataStreamClass = CoreModule["DataStream"];
export type CoreBufferStreamClass = CoreModule["BufferStream"];
export type CoreMultiStreamClass = CoreModule["MultiStream"];
export type CoreDataStreamInstance = InstanceType<CoreDataStreamClass>;
export type CoreDataStreamOptions = NonNullable<ConstructorParameters<CoreDataStreamClass>[0]>;
/** Node stream construction options accepted by Framework stream factories. */
export type DataStreamOptions<Chunk = unknown, Output = Chunk> = TransformOptions & Record<string, unknown>;
export type CorePipeline = (readable: Parameters<CoreDataStreamClass["from"]>[0], ...transforms: unknown[]) => CoreDataStreamInstance;

const coreRequire = createRequire(typeof __filename === "string" ? __filename : pathToFileURL(`${process.cwd()}/package.json`));
const core = coreRequire("scramjet-core") as CoreModule;
export const CoreDataStream: CoreDataStreamClass = core.DataStream;
export const CoreBufferStream: CoreBufferStreamClass = core.BufferStream;
export const CoreMultiStream: CoreMultiStreamClass = core.MultiStream;
export const PromiseTransformStream: typeof Transform = core.PromiseTransformStream as typeof Transform;
export const errors: { StreamError: typeof Error; [key: string]: unknown } = core.errors as unknown as {
  StreamError: typeof Error;
  [key: string]: unknown;
};
export const StreamError: typeof Error = errors.StreamError;

export const corePipeline = (CoreDataStream as typeof CoreDataStream & { pipeline: CorePipeline }).pipeline;

/** @internal */
const frameworkClasses = new Map<string, unknown>();

/** @internal Register Framework sibling constructors for cross-kind operations. */
export function registerFrameworkClasses(classes: Record<string, unknown>): void {
  for (const [name, value] of Object.entries(classes)) frameworkClasses.set(name, value);
}

/** @internal Resolve a registered Framework sibling constructor. */
export function getFrameworkClass<T>(name: string): T {
  const value = frameworkClasses.get(name);
  if (!value) throw new Error(`Framework class ${name} is not initialized`);
  return value as T;
}

type ReplayChunk = { chunk: unknown };

interface ReplaySource {
  readonly readableObjectMode?: boolean;
  readonly readableHighWaterMark?: number;
  on(event: string, listener: (...args: any[]) => void): this;
  once(event: string, listener: (...args: any[]) => void): this;
  emit(event: string, ...args: any[]): boolean;
}

/** @internal Bounded replay state used by DataStream.keep(). */
export class ReplayState {
  readonly buffer: ReplayChunk[] = [];
  readonly readers = new Set<ReplayReadable>();
  readonly length: number;
  firstAvailable = 0;
  ended = false;
  error: Error | null = null;

  constructor(readonly source: ReplaySource, length: number) {
    this.length = length;
    source.on("data", (chunk) => this.store(chunk));
    source.once("end", () => this.finish());
    source.once("error", (error) => this.fail(error));
    source.once("close", () => this.finish());
  }

  get nextAvailable(): number {
    return this.firstAvailable + this.buffer.length;
  }

  addReader(count: number): ReplayReadable {
    const cursor = count === 0
      ? this.nextAvailable
      : count > 0 && count < this.buffer.length
      ? this.nextAvailable - count
      : this.firstAvailable;
    const reader = new ReplayReadable(this, cursor, {
      objectMode: this.source.readableObjectMode ?? true,
      highWaterMark: this.source.readableHighWaterMark,
    });
    this.readers.add(reader);
    return reader;
  }

  detach(reader: ReplayReadable): void {
    this.readers.delete(reader);
  }

  chunkAt(position: number): ReplayChunk {
    const chunk = this.buffer[position - this.firstAvailable];
    if (!chunk) throw new RangeError("Replay position is no longer retained");
    return chunk;
  }

  private store(chunk: unknown): void {
    this.buffer.push({ chunk });
    this.trim();
    this.notifyReaders();
  }

  private trim(): void {
    const dropped = this.buffer.length - this.length;
    if (dropped <= 0) return;
    this.buffer.splice(0, dropped);
    this.firstAvailable += dropped;
    this.source.emit("drop", dropped);
    for (const reader of this.readers) reader.skipTo(this.firstAvailable);
  }

  private finish(): void {
    if (this.ended) return;
    this.ended = true;
    this.notifyReaders();
  }

  private fail(error: Error): void {
    if (this.ended) return;
    this.error = error;
    this.ended = true;
    for (const reader of this.readers) reader.fail(error);
    this.readers.clear();
  }

  private notifyReaders(): void {
    for (const reader of this.readers) reader.notify();
  }
}

class ReplayReadable extends Readable {
  private cursor: number;
  private waitingForData = false;
  private closedReplay = false;

  constructor(private readonly replay: ReplayState, cursor: number, options: ReadableOptions) {
    super(options);
    this.cursor = cursor;
    this.once("close", () => {
      this.closedReplay = true;
      this.replay.detach(this);
    });
  }

  override _read(): void {
    this.waitingForData = true;
    this.pump();
  }

  notify(): void {
    if (this.waitingForData) this.pump();
  }

  skipTo(firstAvailable: number): void {
    if (this.closedReplay || this.cursor >= firstAvailable) return;
    const dropped = firstAvailable - this.cursor;
    this.cursor = firstAvailable;
    this.emit("drop", dropped);
  }

  fail(error: Error): void {
    if (!this.closedReplay) this.destroy(error);
  }

  private pump(): void {
    if (this.closedReplay || this.destroyed) return;
    this.skipTo(this.replay.firstAvailable);
    while (this.cursor < this.replay.nextAvailable) {
      const chunk = this.replay.chunkAt(this.cursor);
      this.cursor += 1;
      if (!this.push(chunk.chunk)) {
        this.waitingForData = false;
        return;
      }
    }
    if (this.replay.ended) {
      this.closedReplay = true;
      if (this.replay.error) this.destroy(this.replay.error);
      else this.push(null);
    }
  }
}

export type CsvOptions = {
  delimiter?: string;
  header?: boolean;
  newline?: string;
};

export type StringSource<Chunk = unknown> =
  | string
  | Iterable<Chunk>
  | AsyncIterable<Chunk>
  | Readable
  | PromiseLike<Iterable<Chunk> | AsyncIterable<Chunk> | Readable>;

function responseError(url: URL, response: Response): Error {
  return new Error(`HTTP ${response.status} ${response.statusText} while fetching ${url.href}`);
}

function assertHttpURL(url: URL): void {
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new TypeError(`Unsupported URL protocol: ${url.protocol}`);
  }
}

export async function fetchURL(url: URL): Promise<Response> {
  assertHttpURL(url);
  const response = await fetch(url);
  if (!response.ok) throw responseError(url, response);
  return response;
}

function parseCsvRow(line: string, delimiter: string): string[] {
  const cells: string[] = [];
  let cell = "";
  let quoted = false;
  for (let index = 0; index < line.length; index += 1) {
    const character = line[index];
    if (character === '"') {
      if (quoted && line[index + 1] === '"') {
        cell += '"';
        index += 1;
      } else {
        quoted = !quoted;
      }
    } else if (character === delimiter && !quoted) {
      cells.push(cell);
      cell = "";
    } else {
      cell += character;
    }
  }
  if (quoted) throw new StreamError("Unterminated quoted CSV cell");
  cells.push(cell);
  return cells;
}

export function stringifyCsvCell(value: unknown, delimiter: string): string {
  const cell = String(value ?? "");
  return cell.includes(delimiter) || cell.includes('"') || /\r|\n/.test(cell)
    ? `"${cell.replaceAll('"', '""')}"`
    : cell;
}

/**
 * Framework stream base. Composition is in-place: a captured reference sees
 * later writes/composition, rather than becoming a runtime snapshot. Use the
 * returned sibling stream for a new branch; ordinary chaining does not require
 * `tap()`.
 */
