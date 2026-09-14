import { CoreDataStream, fetchURL, getFrameworkClass } from "./shared.js";
import type { CoreDataStreamOptions, CsvOptions, DataStreamOptions, StringSource } from "./shared.js";
import { DataStream } from "./data-stream.js";
import type { BufferStream } from "./buffer-stream.js";
import { execRaw } from "./execution.js";
import type { ExecOptions } from "./execution.js";

/** Text stream with line, parser, encoding, and raw Node command helpers. */
export class StringStream extends DataStream<string> {

  /** Run a trusted shell command with raw string stdin and stdout.
   * @param command Trusted shell command line.
   * @param options Child-process and abort options.
   */
  exec(command: string, options?: ExecOptions): StringStream {
    return execRaw(this, command, () => new (this.constructor as any)(), options);
  }

  static get SPLIT_LINE(): RegExp {
    return /\r\n?|\n/;
  }

  /** Split text into lines while preserving stream backpressure. */
  lines(): DataStream<string> {
    return this.split(StringStream.SPLIT_LINE);
  }

  /** Remove a character prefix and report it before passing the remainder. */
  shift(characters: number, callback: (chunk: string) => unknown): StringStream;
  shift(characters: number, callback: (chunks: string[]) => unknown): DataStream<string>;
  /** Remove a character prefix and report it before passing the remainder. */
  shift(characters: number, callback: ((chunk: string) => unknown) | ((chunks: string[]) => unknown)): StringStream {
    if (!Number.isInteger(characters) || characters < 0) throw new RangeError("shift count must be a non-negative integer");
    const output = new StringStream();
    let shifted = "";
    let detached = false;
    const detach = (): void => {
      if (detached) return;
      detached = true;
      this.off("data", onData);
      this.off("end", onEnd);
      this.off("error", onError);
    };
    const finish = (remainder?: string): void => {
      Promise.resolve((callback as (chunk: string) => unknown)(shifted)).then(() => {
        if (remainder !== undefined) output.write(remainder);
        this.pipe(output);
      }).catch((error) => output.destroy(error));
    };
    const onData = (chunk: string): void => {
      const needed = characters - shifted.length;
      if (needed <= 0) return;
      const take = Math.min(needed, chunk.length);
      shifted += chunk.slice(0, take);
      if (shifted.length === characters) {
        this.pause();
        detach();
        finish(chunk.slice(take));
      }
    };
    const onEnd = (): void => {
      detach();
      Promise.resolve((callback as (chunk: string) => unknown)(shifted)).then(() => output.end()).catch((error) => output.destroy(error));
    };
    const onError = (error: Error): void => { detach(); output.destroy(error); };
    if (characters === 0) {
      Promise.resolve((callback as (chunk: string) => unknown)("")).then(() => this.pipe(output)).catch((error) => output.destroy(error));
      return output;
    }
    this.on("data", onData);
    this.once("end", onEnd);
    this.once("error", onError);
    return output;
  }

  /** Alias for shifting characters from the front of the stream. */
  pop(characters: number, callback: (chunk: string) => unknown): StringStream {
    return this.shift(characters, callback);
  }

  /** Split text by a string or regular expression. */
  split(splitter: string | RegExp): StringStream {
    if (!(typeof splitter === "string" || splitter instanceof RegExp)) throw new TypeError("Splitter must be a string or RegExp");
    const output = new StringStream();
    let remainder = "";
    let pending = Promise.resolve();
    const write = async (value: string): Promise<void> => {
      if (output.write(value)) return;
      await new Promise<void>((resolve) => output.once("drain", resolve));
    };
    const splitChunk = async (chunk: string): Promise<void> => {
      remainder += chunk;
      // Keep a trailing CR until the next chunk so CRLF fragmentation remains a single delimiter.
      if (splitter instanceof RegExp && splitter.source === StringStream.SPLIT_LINE.source && remainder.endsWith("\r")) return;
      const pieces = remainder.split(splitter);
      remainder = pieces.pop() ?? "";
      for (const piece of pieces) await write(piece);
    };
    this.on("data", (chunk: string) => {
      this.pause();
      pending = pending.then(() => splitChunk(chunk)).then(() => { this.resume(); }).catch((error) => { output.destroy(error); });
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => {
      pending.then(async () => {
        if (remainder.endsWith("\r") && splitter instanceof RegExp && splitter.source === StringStream.SPLIT_LINE.source) {
          remainder = remainder.slice(0, -1);
        }
        if (remainder.length > 0) await write(remainder);
        output.end();
      }).catch((error) => output.destroy(error));
    });
    return output;
  }

  /** Emit text matching a regular expression. */
  match(matcher: RegExp): StringStream {
    if (!(matcher instanceof RegExp)) throw new TypeError("Matcher must be a RegExp");
    const flags = matcher.flags.includes("g") ? matcher.flags : `${matcher.flags}g`;
    const regex = new RegExp(matcher.source, flags);
    const output = new StringStream();
    let buffer = "";
    let pending = Promise.resolve();
    const write = async (value: string): Promise<void> => {
      if (output.write(value)) return;
      await new Promise<void>((resolve) => output.once("drain", resolve));
    };
    const matchChunk = async (chunk: string): Promise<void> => {
      buffer += chunk;
      regex.lastIndex = 0;
      let consumed = 0;
      let found = regex.exec(buffer);
      while (found !== null) {
        const value = found.length > 1 ? found.slice(1).join("") : found[0];
        await write(value);
        consumed = regex.lastIndex;
        if (found[0].length === 0) regex.lastIndex += 1;
        found = regex.exec(buffer);
      }
      if (consumed > 0) buffer = buffer.slice(consumed);
    };
    this.on("data", (chunk: string) => {
      this.pause();
      pending = pending.then(() => matchChunk(chunk)).then(() => { this.resume(); }).catch((error) => { output.destroy(error); });
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => { pending.then(() => output.end()).catch((error) => output.destroy(error)); });
    return output;
  }

  /** Append fixed or computed text to each chunk. */
  append(value: string | ((chunk: string) => string | PromiseLike<string>)): StringStream {
    const output = new StringStream();
    this.map(async (chunk: string) => `${chunk}${typeof value === "function" ? await value(chunk) : value}`).pipe(output);
    return output;
  }

  /** Prepend fixed or computed text to each chunk. */
  prepend(value: string | ((chunk: string) => string | PromiseLike<string>)): StringStream {
    const output = new StringStream();
    this.map(async (chunk: string) => `${typeof value === "function" ? await value(chunk) : value}${chunk}`).pipe(output);
    return output;
  }

  /** Parse JSON values, one per line by default. */
  JSONParse(perLine = true): DataStream<unknown> {
    const source = perLine ? this.lines() : this;
    const output = new DataStream<unknown>();
    source.filter((line: unknown) => String(line).length > 0).map((line: unknown) => JSON.parse(String(line))).pipe(output);
    return output;
  }

  /** Parse CSV rows into arrays or header-keyed records. */
  CSVParse(options: CsvOptions = {}): DataStream<Record<string, string> | string[]> {
    const delimiter = options.delimiter ?? ",";
    const hasHeader = options.header ?? true;
    const output = new DataStream<Record<string, string> | string[]>();
    let header: string[] | undefined;
    let row: string[] = [];
    let cell = "";
    let quoted = false;
    let quotePending = false;
    let skipLF = false;
    let pending = Promise.resolve();
    // Keep this parser local: the supported v4 CSV contract needs quoted and
    // multiline records, while avoiding a runtime parser dependency keeps the
    // Framework package smaller and preserves explicit backpressure control.
    const parseChunk = (text: string, final = false): string[][] => {
      const rows: string[][] = [];
      const finishRow = (): void => { row.push(cell); rows.push(row); row = []; cell = ""; };
      for (let index = 0; index < text.length; index += 1) {
        const character = text[index];
        if (skipLF) { skipLF = false; if (character === "\n") continue; }
        if (quotePending) {
          quotePending = false;
          if (character === '"') { cell += '"'; quoted = true; continue; }
          quoted = false;
        }
        if (quoted) {
          if (character === '"') quotePending = true;
          else cell += character;
        } else if (character === '"' && cell.length === 0) {
          quoted = true;
        } else if (character === delimiter) {
          row.push(cell); cell = "";
        } else if (character === "\n" || character === "\r") {
          if (character === "\r") skipLF = true;
          finishRow();
        } else {
          cell += character;
        }
      }
      if (final) {
        if (quotePending) { quotePending = false; quoted = false; }
        if (quoted) throw new Error("Unterminated quoted CSV cell");
        if (cell.length || row.length) finishRow();
      }
      return rows;
    };
    const emit = async (rows: string[][]): Promise<void> => {
      for (const parsed of rows) {
        if (hasHeader && !header) header = parsed;
        else {
          const value = header
            ? Object.fromEntries(header.map((key, index) => [key, parsed[index] ?? ""]))
            : parsed;
          if (!output.write(value)) await new Promise<void>((resolve) => output.once("drain", resolve));
        }
      }
    };
    this.on("data", (chunk: string) => {
      this.pause();
      pending = pending.then(() => emit(parseChunk(chunk))).then(() => { this.resume(); }).catch((error) => { output.destroy(error); });
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => {
      pending.then(() => emit(parseChunk("", true))).then(() => output.end()).catch((error) => output.destroy(error));
    });
    return output;
  }

  /** Parse each string chunk into a typed DataStream value. */
  parse<T>(parser: (chunk: string) => T | PromiseLike<T>): DataStream<T> {
    const output = new DataStream<T>();
    this.map(parser as any).pipe(output);
    return output;
  }

  /** Alias for parse(). */
  toDataStream<T>(parser: (chunk: string) => T | PromiseLike<T>): DataStream<T> {
    return this.parse(parser);
  }

  /** Encode each string chunk as UTF-8 bytes. */
  toBufferStream(): BufferStream {
    return (this as any).map((chunk: string) => Buffer.from(chunk), getFrameworkClass("BufferStream")) as BufferStream;
  }

  /** Return this StringStream unchanged. */
  toStringStream(): StringStream {
    return this;
  }

  /** Create a StringStream from literal text or a string source. */
  static from(source: any, options?: DataStreamOptions): StringStream {
    const output = new this(options);
    CoreDataStream.from(normalizeStringSource(source), options as CoreDataStreamOptions).map((chunk) => String(chunk)).pipe(output);
    return output;
  }

  /** Create a StringStream from one literal string. */
  static fromString(value: string): StringStream {
    return this.from(value);
  }

  /** Fetch a URL and decode its body into a StringStream. */
  static async fromURL(url: URL, options: { encoding?: string } = {}): Promise<StringStream> {
    const response = await fetchURL(url);
    const bytes = new Uint8Array(await response.arrayBuffer());
    return this.from(new TextDecoder(options.encoding ?? "utf-8").decode(bytes));
  }
}

function normalizeStringSource<Chunk>(source: StringSource<Chunk>): Parameters<typeof CoreDataStream.from<Chunk>>[0] {
  if (typeof source === "string") return [source] as unknown as Parameters<typeof CoreDataStream.from<Chunk>>[0];
  if (source && typeof source === "object" && "then" in source) {
    return Promise.resolve(source).then((resolved) => normalizeStringSource(resolved));
  }
  return source as Parameters<typeof CoreDataStream.from<Chunk>>[0];
}
