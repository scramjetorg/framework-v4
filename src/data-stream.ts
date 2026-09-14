import { Readable } from "node:stream";
import { CoreDataStream, fetchURL, getFrameworkClass, ReplayState, stringifyCsvCell } from "./shared.js";
import type { CoreDataStreamOptions, CsvOptions, DataStreamOptions } from "./shared.js";
import type { BufferStream } from "./buffer-stream.js";
import type { MultiStream } from "./multi-stream.js";
import type { StringStream } from "./string-stream.js";
import { execJson, forkTransform } from "./execution.js";
import type { ExecOptions, ForkOptions } from "./execution.js";

/**
 * Object-mode stream with Framework factories and format, replay, and execution helpers.
 * @template Chunk The value carried by the stream.
 */
export class DataStream<Chunk = unknown> extends CoreDataStream<Chunk> {
  /** Create a Framework stream from an iterable, async iterable, promise, or Node readable.
   * @param source The input source.
   * @param options Node stream options.
   */
  static from(source: Parameters<typeof CoreDataStream.from>[0], options?: DataStreamOptions): DataStream<any> {
    // biome-ignore lint/complexity/noThisInStatic: the inherited factory must construct this framework subclass.
    return super.from(source, options as CoreDataStreamOptions) as unknown as DataStream<any>;
  }

  /** Create a stream from a synchronous or asynchronous iterator.
   * @param iterator The iterator to consume.
   * @param options Node stream options.
   */
  static fromIterator<T>(iterator: Iterator<T> | AsyncIterator<T>, options?: DataStreamOptions): DataStream<T> {
    const output = new this(options) as DataStream<T>;
    CoreDataStream.fromIterator(iterator as any, options as CoreDataStreamOptions).pipe(output);
    return output;
  }

  // These delegates preserve Core's OFCA/in-place execution while replacing
  // only the public return declaration with the Framework sibling type.
  /** Apply a synchronous or asynchronous transform while retaining a Framework DataStream.
   * @param callback Transform applied to each value.
   */
  map<U>(callback: (chunk: Chunk) => U | PromiseLike<U>): DataStream<U> {
    const ClassType = (arguments as unknown as Array<unknown>)[1];
    return (super.map as any).call(this, callback, ClassType) as unknown as DataStream<U>;
  }

  /** Keep values for which the predicate resolves truthy. */
  filter(callback: (chunk: Chunk) => boolean | PromiseLike<boolean>): DataStream<Chunk> {
    return super.filter(callback as any) as unknown as DataStream<Chunk>;
  }

  /** Observe each value and retain the original values. */
  do(callback: (chunk: Chunk) => unknown | PromiseLike<unknown>): DataStream<Chunk> {
    return super.do(callback as any) as unknown as DataStream<Chunk>;
  }

  /** Run all transforms for each value and emit their results as an array. */
  all<U>(functions: Array<(value: Chunk) => U | PromiseLike<U>>): DataStream<U[]> {
    return super.all(functions as any) as unknown as DataStream<U[]>;
  }

  /** Run transforms concurrently and emit the fastest result for each value. */
  race<U>(functions: Array<(value: Chunk) => U | PromiseLike<U>>): DataStream<U> {
    return super.race(functions as any) as unknown as DataStream<U>;
  }

  /** Write transform output into a caller-provided Framework stream. */
  into<U>(callback: (stream: DataStream<U>, value: Chunk) => unknown | PromiseLike<unknown>, into: DataStream<U>): DataStream<U> {
    return super.into(callback as any, into as any) as unknown as DataStream<U>;
  }

  /** Observe or tee values without changing this stream. */
  tee(callback: ((stream: DataStream<Chunk>) => unknown) | NodeJS.WritableStream): this {
    return super.tee(callback as any) as this;
  }

  /** Apply a transform concurrently and emit completions without ordering guarantees. */
  unorder<U>(callback: (chunk: Chunk) => U | PromiseLike<U>): DataStream<U> {
    return super.unorder(callback as any) as unknown as DataStream<U>;
  }

  /** Return a Framework stream copy that receives the same values. */
  copy(): DataStream<Chunk> {
    return super.copy() as unknown as DataStream<Chunk>;
  }

  /** Pass values while the predicate remains true. */
  while(callback: (chunk: Chunk) => boolean | PromiseLike<boolean>): DataStream<Chunk> {
    return this.takeWhile(callback, false);
  }

  /** Pass values until the predicate becomes true. */
  until(callback: (chunk: Chunk) => boolean | PromiseLike<boolean>): DataStream<Chunk> {
    return this.takeWhile(callback, true);
  }

  /** Fetch an HTTP(S) URL and expose its response body as a Framework stream.
   * @param url An HTTP or HTTPS URL.
   */
  static async fromURL(url: URL): Promise<DataStream<any>> {
    const response = await fetchURL(url);
    const output = new DataStream<Uint8Array>();
    if (!response.body) return output.end() as DataStream<any>;
    Readable.fromWeb(response.body as any).pipe(output);
    return output;
  }

  /** Serialize each value as one JSON value followed by `newline`.
   * @param newline Line terminator appended to each serialized value.
   */
  JSONStringify(newline = "\n"): StringStream {
    const output = new (getFrameworkClass<typeof StringStream>("StringStream"))();
    this.map((chunk: unknown) => `${JSON.stringify(chunk)}${newline}`).pipe(output);
    return output;
  }

  /** Serialize record or array values as CSV, optionally including a header.
   * @param options CSV delimiter, newline, and header options.
   */
  CSVStringify(options: CsvOptions = {}): StringStream {
    const delimiter = options.delimiter ?? ",";
    const newline = options.newline ?? "\n";
    const includeHeader = options.header ?? true;
    let header: string[] | undefined;
    let wroteHeader = false;
    // Use Core's flow-controlled `into` path rather than a flowing-mode data
    // listener, so downstream StringStream backpressure reaches `this`.
    const output = new (getFrameworkClass<typeof StringStream>("StringStream"))();
    (this as any).into(async (out: any, chunk: Record<string, unknown>) => {
      header ??= Object.keys(chunk);
      const records: string[] = [];
      if (includeHeader && !wroteHeader) {
        records.push(`${header.map((key) => stringifyCsvCell(key, delimiter)).join(delimiter)}${newline}`);
        wroteHeader = true;
      }
      records.push(`${header.map((key) => stringifyCsvCell(chunk[key], delimiter)).join(delimiter)}${newline}`);
      await out.whenWrote(...records);
    }, output);
    return output;
  }

  /** Serialize each value into a Framework BufferStream. */
  bufferify(serializer: any): BufferStream {
    return (this as any).map(serializer, getFrameworkClass("BufferStream")) as BufferStream;
  }

  /** Convert each value to a Buffer stream. */
  toBufferStream(serializer: (chunk: Chunk) => Buffer): BufferStream {
    return this.bufferify(serializer);
  }

  /** Serialize each value into a Framework StringStream. */
  stringify(serializer: any = (chunk: Chunk) => String(chunk)): StringStream {
    return (this as any).map(serializer, getFrameworkClass("StringStream")) as StringStream;
  }

  /** Convert each value to a StringStream. */
  toStringStream(serializer?: (chunk: Chunk) => string): StringStream {
    return this.stringify(serializer);
  }

  /** Pull values from another source into this stream without ending it. */
  async pull(pullable: unknown, ...args: unknown[]): Promise<void> {
    const incoming = (this.constructor as any).from(pullable as any, {} as DataStreamOptions, ...args as any) as DataStream<any>;
    incoming.pipe(this, { end: false });
    await new Promise<void>((resolve, reject) => {
      incoming.once("end", resolve);
      incoming.once("error", reject);
    });
  }

  /** Add fixed or computed object properties to each value. */
  assign(values: Record<string, unknown> | ((chunk: Chunk) => Record<string, unknown>)): DataStream<Chunk & Record<string, unknown>> {
    return this.map((chunk) => ({ ...((chunk as object) ?? {}), ...(typeof values === "function" ? values(chunk) : values) })) as DataStream<Chunk & Record<string, unknown>>;
  }

  /** Concatenate this stream with subsequent readable sources. */
  concat(...streams: Array<Readable | DataStream<unknown>>): DataStream<Chunk> {
    const output = new DataStream<Chunk>();
    const sources: Readable[] = [this as unknown as Readable, ...streams as Readable[]];
    let index = 0;
    const pipeNext = (): void => {
      const source = sources[index++];
      if (!source) { output.end(); return; }
      source.once("end", pipeNext).pipe(output, { end: false });
    };
    pipeNext();
    return output;
  }

  /** Reduce values into a caller-owned accumulator. */
  async accumulate<Accumulator>(reducer: (accumulator: Accumulator, chunk: Chunk) => unknown, accumulator: Accumulator): Promise<Accumulator> {
    for await (const chunk of this) await reducer(accumulator, chunk);
    return accumulator;
  }

  /** Consume all values with an asynchronous consumer. */
  async consume(consumer: (chunk: Chunk) => unknown): Promise<void> {
    for await (const chunk of this) await consumer(chunk);
  }

  /** Start a reduction and return the accumulator immediately. */
  reduceNow<Accumulator>(reducer: (accumulator: Accumulator, chunk: Chunk) => unknown, accumulator: Accumulator): Accumulator {
    const pending = this.reduce(reducer as any, accumulator);
    if (accumulator && typeof (accumulator as any).on === "function") {
      pending.catch((error) => (accumulator as any).emit("error", error));
    }
    return accumulator;
  }

  /** Insert a fixed or computed separator between values. */
  join(separator: unknown | ((previous: Chunk, next: Chunk, ...args: unknown[]) => unknown), ...args: unknown[]): DataStream<unknown> {
    const output = new DataStream<unknown>();
    let previous: Chunk | undefined;
    let hasPrevious = false;
    let pending = Promise.resolve();
    this.on("data", (chunk: Chunk) => {
      pending = pending.then(async () => {
        if (hasPrevious) {
          const inserted = typeof separator === "function" ? separator(previous as Chunk, chunk, ...args) : separator;
          if (inserted && typeof (inserted as any)[Symbol.asyncIterator] === "function") {
            for await (const value of inserted as AsyncIterable<unknown>) output.write(value);
          } else if (inserted && typeof (inserted as any)[Symbol.iterator] === "function" && typeof inserted !== "string") {
            for (const value of inserted as Iterable<unknown>) output.write(value);
          } else if (inserted !== undefined) output.write(await inserted);
        }
        output.write(chunk);
        previous = chunk;
        hasPrevious = true;
      }).catch((error) => { output.destroy(error); });
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => { pending.then(() => output.end()).catch((error) => output.destroy(error)); });
    return output;
  }

  /** Emit a bounded slice of the stream. */
  slice(start = 0, length?: number): DataStream<Chunk> {
    const output = new DataStream<Chunk>();
    let index = 0;
    this.on("data", (chunk: Chunk) => {
      const inRange = index++ >= start && (length === undefined || index - 1 < start + length);
      if (inRange) output.write(chunk);
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => output.end());
    return output;
  }

  /** Append values after the source ends. */
  endWith(...items: Chunk[]): DataStream<Chunk> {
    const output = new DataStream<Chunk>();
    this.pipe(output, { end: false });
    this.once("end", () => {
      for (const item of items) output.write(item);
      output.end();
    });
    return output;
  }

  unshift(chunk: any, encoding?: BufferEncoding): void;
  unshift(...items: Chunk[]): DataStream<Chunk>;
  /** Prepend values or use Node's writable unshift form. */
  unshift(...items: any[]): DataStream<Chunk> | void {
    if (items.length === 2 && (typeof items[1] === "string" || items[1] === undefined)) {
      return super.unshift(items[0], items[1] as BufferEncoding);
    }
    const output = new DataStream<Chunk>();
    for (const item of items) output.write(item);
    this.pipe(output);
    return output;
  }

  /** Remove and report a prefix of values before passing the remainder. */
  shift(count: number, callback: (chunks: Chunk[]) => unknown): DataStream<Chunk> {
    if (!Number.isInteger(count) || count < 0) throw new RangeError("shift count must be a non-negative integer");
    const output = new DataStream<Chunk>();
    const shifted: Chunk[] = [];
    let detached = false;
    const detach = (): void => {
      if (detached) return;
      detached = true;
      this.off("data", onData);
      this.off("end", onEnd);
      this.off("error", onError);
    };
    const continueWithRemainder = (): void => {
      Promise.resolve(callback(shifted)).then(() => this.pipe(output)).catch((error) => output.destroy(error));
    };
    const onData = (chunk: Chunk): void => {
      shifted.push(chunk);
      if (shifted.length >= count) {
        this.pause();
        detach();
        continueWithRemainder();
      }
    };
    const onEnd = (): void => { detach(); Promise.resolve(callback(shifted)).then(() => output.end()).catch((error) => output.destroy(error)); };
    const onError = (error: Error): void => { detach(); output.destroy(error); };
    if (count === 0) {
      Promise.resolve(callback([])).then(() => this.pipe(output)).catch((error) => output.destroy(error));
      return output;
    }
    this.on("data", onData);
    this.once("end", onEnd);
    this.once("error", onError);
    return output;
  }

  /** Observe a prefix without removing it from the stream. */
  peek(count: number, callback: (chunks: Chunk[]) => void): this {
    const seen: Chunk[] = [];
    const inspect = (chunk: Chunk): void => {
      if (seen.length < count) seen.push(chunk);
      if (seen.length === count) {
        this.off("data", inspect);
        callback(seen);
      }
    };
    this.on("data", inspect);
    this.once("end", () => {
      if (seen.length < count) callback(seen);
    });
    return this;
  }

  /** Invoke a callback if the stream emits no values. */
  empty(callback: () => void): this {
    let hasData = false;
    this.once("data", () => { hasData = true; });
    this.once("end", () => { if (!hasData) callback(); });
    return this;
  }

  /** Group values into fixed-size batches, including a final partial batch. */
  batch(size: number): DataStream<Chunk[]> {
    if (!Number.isInteger(size) || size < 1) throw new RangeError("batch size must be a positive integer");
    const output = new DataStream<Chunk[]>();
    let batch: Chunk[] = [];
    this.on("data", (chunk: Chunk) => {
      batch.push(chunk);
      if (batch.length === size) { output.write(batch); batch = []; }
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => { if (batch.length) output.write(batch); output.end(); });
    return output;
  }

  /** Group values by elapsed time and optional maximum size. */
  timeBatch(milliseconds: number, size?: number): DataStream<Chunk[]> {
    const output = new DataStream<Chunk[]>();
    let batch: Chunk[] = [];
    let timer: NodeJS.Timeout | undefined;
    const flush = (): void => { if (batch.length) { output.write(batch); batch = []; } };
    const arm = (): void => { if (!timer) timer = setTimeout(() => { timer = undefined; flush(); if (batch.length) arm(); }, milliseconds); };
    this.on("data", (chunk: Chunk) => { batch.push(chunk); if (size && batch.length >= size) flush(); else arm(); });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => { if (timer) clearTimeout(timer); flush(); output.end(); });
    return output;
  }

  /** Limit emission to the requested rate. */
  rate(chunksPerSecond: number): DataStream<Chunk> {
    if (!(chunksPerSecond > 0)) throw new RangeError("chunksPerSecond must be positive");
    const delay = 1000 / chunksPerSecond;
    return this.flatMap(async (chunk) => { await new Promise((resolve) => setTimeout(resolve, delay)); return [chunk]; });
  }

  /** Observe values for diagnostics while passing them through unchanged. */
  debug(observer: (chunk: Chunk) => unknown): DataStream<Chunk> {
    return this.map((chunk) => { observer(chunk); return chunk; });
  }

  /** Emit rolling arrays, including partial windows at the beginning. */
  window(length: number): import("./window-stream.js").WindowStream {
    if (!(+length > 0)) throw new Error("Length argument must be a positive integer!");
    const WindowClass = getFrameworkClass<typeof import("./window-stream.js").WindowStream>("WindowStream");
    const output = new WindowClass();
    const values: Chunk[] = [];
    let pending = Promise.resolve();
    this.on("data", (chunk: Chunk) => {
      this.pause();
      pending = pending.then(async () => {
        values.push(chunk);
        if (values.length > length) values.shift();
        if (!output.write(values.slice())) await new Promise<void>((resolve) => output.once("drain", resolve));
        void this.resume();
      }).catch((error) => { output.destroy(error); });
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => {
      pending.then(() => { output.end(); }).catch((error) => { output.destroy(error); });
    });
    return output;
  }

  /** Route values into caller-provided targets without ending those targets. */
  separateInto(
    streams: Record<string | symbol, { write(chunk: Chunk): boolean; whenWrote?: (chunk: Chunk) => PromiseLike<unknown>; once?: (event: string, listener: () => void) => unknown }>,
    affinity: (chunk: Chunk) => string | symbol | PromiseLike<string | symbol>,
  ): this {
    let pending = Promise.resolve();
    const fail = (error: unknown): void => { this.destroy(error as Error); };
    const writeToTarget = async (chunk: Chunk): Promise<void> => {
      const key = await affinity(chunk);
      const target = streams[key];
      if (!target) throw new Error(`Output for ${String(key)} not found in ${JSON.stringify(chunk)}`);
      if (target.whenWrote) {
        await target.whenWrote(chunk);
      } else if (!target.write(chunk)) {
        await new Promise<void>((resolve) => target.once?.("drain", resolve));
      }
    };
    this.on("data", (chunk: Chunk) => {
      this.pause();
      pending = pending.then(() => writeToTarget(chunk)).then(() => { this.resume(); }).catch(fail);
    });
    return this;
  }

  /** Retain a bounded live replay history for rewind and tail readers. */
  keep(length = -1): this {
    const normalizedLength = length < 0 ? Number.POSITIVE_INFINITY : length;
    if (!(normalizedLength === Number.POSITIVE_INFINITY || Number.isSafeInteger(normalizedLength)) || normalizedLength < 0) {
      throw new RangeError("keep length must be a non-negative safe integer");
    }
    const replay = new ReplayState(this, normalizedLength);
    Object.defineProperty(this, "__frameworkReplay", { configurable: true, value: replay, writable: true });
    this.resume();
    return this;
  }

  /** Read retained history from its beginning and follow future values. */
  rewind(count = -1): Readable {
    const replay = (this as this & { __frameworkReplay?: ReplayState }).__frameworkReplay;
    if (!replay) throw new Error("Stream not buffered, cannot rewind.");
    return replay.addReader(count);
  }

  /** Read the retained suffix and follow future values. */
  tail(count: number): Readable {
    return this.rewind(count);
  }

  /** Flatten iterable or async iterable values by one level. */
  flatten(): DataStream<unknown> {
    return this.flatMap((value) => value as Iterable<unknown> | AsyncIterable<unknown>);
  }

  /** Map values to iterables and flatten their results. */
  flatMap<U>(mapper: (chunk: Chunk) => Iterable<U> | AsyncIterable<U> | PromiseLike<Iterable<U> | AsyncIterable<U>>): DataStream<U> {
    const output = new DataStream<U>();
    let pending = Promise.resolve();
    this.on("data", (chunk: Chunk) => {
      pending = pending.then(async () => {
        const values = await mapper(chunk);
        for await (const value of values) output.write(value);
      }).catch((error) => { output.destroy(error); });
    });
    this.once("end", () => { pending.then(() => output.end()).catch((error) => output.destroy(error)); });
    this.once("error", (error) => output.destroy(error));
    return output;
  }

  /** Let a mapper emit zero or more output values per input. */
  remap<U>(mapper: (emit: (value: U) => void, chunk: Chunk) => unknown): DataStream<U> {
    const output = new DataStream<U>();
    let pending = Promise.resolve();
    this.on("data", (chunk: Chunk) => {
      pending = pending.then(() => mapper((value) => output.write(value), chunk) as any).catch((error) => { output.destroy(error); });
    });
    this.once("end", () => { pending.then(() => output.end()).catch((error) => output.destroy(error)); });
    this.once("error", (error) => output.destroy(error));
    return output;
  }

  /** @private */
  private takeWhile(callback: (chunk: Chunk) => boolean | PromiseLike<boolean>, until: boolean): DataStream<Chunk> {
    const output = new DataStream<Chunk>();
    let stopped = false;
    let pending = Promise.resolve();
    const stop = (): void => { if (!stopped) { stopped = true; this.pause(); output.end(); } };
    this.on("data", (chunk: Chunk) => {
      if (stopped) return;
      pending = pending.then(async () => {
        if (stopped) return;
        const matches = await callback(chunk);
        if ((until && matches) || (!until && !matches)) { stop(); return; }
        output.write(chunk);
      }).catch((error) => { output.destroy(error); });
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => { pending.then(() => { if (!stopped) output.end(); }).catch((error) => output.destroy(error)); });
    return output;
  }

  /** Create a MultiStream with one lazily created branch per affinity key. */
  separate(affinity: (chunk: Chunk) => unknown | PromiseLike<unknown>, createOptions: DataStreamOptions = {}, ClassType: typeof DataStream = this.constructor as typeof DataStream): MultiStream {
    const result = new (getFrameworkClass<typeof MultiStream>("MultiStream"))();
    const streams = new Map<string, DataStream<Chunk>>();
    let pendingRoutes = 0;
    let sourceEnded = false;
    const finish = (): void => {
      if (sourceEnded && pendingRoutes === 0) {
        for (const target of streams.values()) target.end();
      }
    };
    const route = async (hash: unknown, chunk: Chunk): Promise<void> => {
      const key = String(hash);
      let target = streams.get(key);
      if (!target) {
        target = new ClassType(createOptions) as DataStream<Chunk>;
        (target as DataStream<Chunk> & { _separateId?: string })._separateId = key;
        streams.set(key, target);
        result.add(target);
      }
      if (!target.write(chunk)) await new Promise<void>((resolve) => target!.once("drain", resolve));
    };
    this.on("data", (chunk: Chunk) => {
      pendingRoutes += 1;
      Promise.resolve(affinity(chunk)).then((hash): Promise<void> => Array.isArray(hash)
        ? Promise.all(hash.map((item) => route(item, chunk))).then(() => undefined)
        : route(hash, chunk).then(() => undefined)).then(() => undefined).catch((error) => result.emit("error", error)).finally(() => {
        pendingRoutes -= 1;
        finish();
      });
    });
    this.once("error", (error) => result.emit("error", error));
    this.once("end", () => { sourceEnded = true; finish(); });
    return result;
  }

  /** Emit buffered values newest-first, optionally dropping older overflow. */
  stack(count = 1000, drop: (chunks: Chunk[]) => unknown = () => undefined): DataStream<Chunk> {
    const stack: Chunk[] = [];
    const waiting: Array<(chunks: Chunk[]) => void> = [];
    let ended = false;
    const StreamClass = this.constructor as typeof DataStream;
    const output = new StreamClass({
      promiseRead: (): Chunk[] | Promise<Chunk[]> => {
        if (stack.length) return [stack.pop() as Chunk];
        if (ended) return [];
        return new Promise((resolve) => waiting.push(resolve));
      }
    } as CoreDataStreamOptions) as unknown as DataStream<Chunk>;
    this.on("data", (chunk: Chunk) => {
      const waiter = waiting.shift();
      if (waiter) waiter([chunk]);
      else {
        stack.push(chunk);
        if (stack.length > count) Promise.resolve(drop([stack.shift() as Chunk])).catch((error) => output.destroy(error));
      }
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => { ended = true; while (waiting.length) waiting.shift()!([]); });
    return output;
  }

  /** Serialize the stream as a JSON array string. */
  toJSONArray(enclosure: Iterable<string> = ["[", "]"]): StringStream {
    const [open = "[", close = "]"] = Array.from(enclosure);
    const output = new (getFrameworkClass<typeof StringStream>("StringStream"))();
    let first = true;
    output.write(open);
    this.on("data", (chunk: Chunk) => { output.write(`${first ? "" : ","}${JSON.stringify(chunk)}`); first = false; });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => { output.end(close); });
    return output;
  }

  /** Serialize the stream as a JSON object keyed by a function. */
  toJSONObject(key: (chunk: Chunk) => string = (chunk) => String(chunk), enclosure: Iterable<string> = ["{", "}"]): StringStream {
    const [open = "{", close = "}"] = Array.from(enclosure);
    const output = new (getFrameworkClass<typeof StringStream>("StringStream"))();
    let first = true;
    output.write(open);
    this.on("data", (chunk: Chunk) => { output.write(`${first ? "" : ","}${JSON.stringify(key(chunk))}:${JSON.stringify(chunk)}`); first = false; });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => output.end(close));
    return output;
  }

  /**
   * Run a trusted shell command using JSON Lines on stdin/stdout.
   * Each input value is encoded as JSON plus a newline; each nonblank output line is parsed as JSON.
   * This is Node-only and does not impose a command, environment, or path policy.
   * @param command Trusted shell command line.
   * @param options Child-process and abort options.
   */
  exec(command: string, options?: ExecOptions): DataStream<any> {
    return execJson(this, command, () => new (this.constructor as any)() as DataStream<any>, options) as DataStream<any>;
  }

  /**
   * Apply a trusted ESM or CommonJS fork module with bounded concurrency.
   * Results are emitted in worker completion order; this method does not create MultiStream fanout.
   * @param modulePath Path to a module exporting `(value, { ref }) => result`.
   * @param options Concurrency and abort options.
   */
  distribute<Output = unknown>(modulePath: string, options?: ForkOptions): DataStream<Output> {
    return forkTransform(this, modulePath, () => new (this.constructor as any)() as DataStream<Output>, false, options) as DataStream<Output>;
  }

  /**
   * Apply a trusted ESM or CommonJS fork module with bounded concurrency.
   * Results are emitted in input order; this method does not create MultiStream fanout.
   * @param modulePath Path to a module exporting `(value, { ref }) => result`.
   * @param options Concurrency and abort options.
   */
  delegate<Output = unknown>(modulePath: string, options?: ForkOptions): DataStream<Output> {
    return forkTransform(this, modulePath, () => new (this.constructor as any)() as DataStream<Output>, true, options) as DataStream<Output>;
  }
}
