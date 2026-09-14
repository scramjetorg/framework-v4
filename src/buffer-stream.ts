import { StringDecoder } from "node:string_decoder";
import { CoreBufferStream, getFrameworkClass } from "./shared.js";
import { DataStream } from "./data-stream.js";
import type { StringStream } from "./string-stream.js";

/** Byte-oriented Framework stream with safe splitting, decoding, and parsing helpers. */
export class BufferStream extends CoreBufferStream {
  // Core implements the byte-oriented operation; these covariant wrappers keep
  // the Framework sibling type and the v4 `pop` alias visible to consumers.
  /** Remove a byte prefix and report it before passing the remainder. */
  shift(bytes: number, callback: (chunk: Buffer) => unknown): BufferStream {
    if (!Number.isInteger(bytes) || bytes < 0) throw new RangeError("shift size must be a non-negative integer");
    const output = new BufferStream();
    let shifted = Buffer.alloc(0);
    let detached = false;
    const detach = (): void => {
      if (detached) return;
      detached = true;
      this.off("data", onData);
      this.off("end", onEnd);
      this.off("error", onError);
    };
    const finish = (remainder?: Buffer): void => {
      Promise.resolve(callback(shifted)).then(() => {
        if (remainder?.length) output.write(remainder);
        this.pipe(output);
      }).catch((error) => output.destroy(error));
    };
    const onData = (chunk: Buffer): void => {
      const needed = bytes - shifted.length;
      const take = Math.min(needed, chunk.length);
      shifted = Buffer.concat([shifted, chunk.subarray(0, take)]);
      if (shifted.length === bytes) {
        this.pause();
        detach();
        finish(chunk.subarray(take));
      }
    };
    const onEnd = (): void => { detach(); Promise.resolve(callback(shifted)).then(() => output.end()).catch((error) => output.destroy(error)); };
    const onError = (error: Error): void => { detach(); output.destroy(error); };
    if (bytes === 0) {
      Promise.resolve(callback(Buffer.alloc(0))).then(() => this.pipe(output)).catch((error) => output.destroy(error));
      return output;
    }
    this.on("data", onData);
    this.once("end", onEnd);
    this.once("error", onError);
    return output;
  }

  /** Alias for shifting bytes from the front of the stream. */
  pop(bytes: number, callback: (chunk: Buffer) => unknown): BufferStream {
    return this.shift(bytes, callback);
  }

  /** Split buffered bytes on a non-empty string or Buffer delimiter. */
  split(splitter: string | Buffer): BufferStream {
    const needle = Buffer.from(splitter);
    if (needle.length === 0) throw new RangeError("splitter must not be empty");
    const output = new BufferStream();
    let remainder = Buffer.alloc(0);
    let pending = Promise.resolve();
    const write = async (value: Buffer): Promise<void> => {
      if (!value.length || output.write(value)) return;
      await new Promise<void>((resolve) => output.once("drain", resolve));
    };
    const splitChunk = async (chunk: Buffer): Promise<void> => {
      remainder = Buffer.concat([remainder, chunk]);
      let position = remainder.indexOf(needle);
      while (position !== -1) {
        await write(remainder.subarray(0, position));
        remainder = remainder.subarray(position + needle.length);
        position = remainder.indexOf(needle);
      }
    };
    this.on("data", (chunk: Buffer) => {
      this.pause();
      pending = pending.then(() => splitChunk(chunk)).then(() => { this.resume(); }).catch((error) => { output.destroy(error); });
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => { pending.then(async () => { await write(remainder); output.end(); }).catch((error) => output.destroy(error)); });
    return output;
  }

  /** Break each input into chunks no larger than `size`. */
  breakup(size: number): BufferStream {
    if (!Number.isInteger(size) || size < 1) throw new RangeError("breakup size must be a positive integer");
    const output = new BufferStream();
    let remainder = Buffer.alloc(0);
    let pending = Promise.resolve();
    const write = async (value: Buffer): Promise<void> => {
      if (output.write(value)) return;
      await new Promise<void>((resolve) => output.once("drain", resolve));
    };
    const breakChunk = async (chunk: Buffer): Promise<void> => {
      remainder = Buffer.concat([remainder, chunk]);
      while (remainder.length >= size) {
        await write(remainder.subarray(0, size));
        remainder = remainder.subarray(size);
      }
    };
    this.on("data", (chunk: Buffer) => {
      this.pause();
      pending = pending.then(() => breakChunk(chunk)).then(() => { this.resume(); }).catch((error) => { output.destroy(error); });
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => { pending.then(async () => { if (remainder.length) await write(remainder); output.end(); }).catch((error) => output.destroy(error)); });
    return output;
  }

  /** Decode bytes with a stateful Node StringDecoder. */
  stringify(encoding?: any): StringStream {
    encoding ??= "utf-8";
    const output = new (getFrameworkClass<typeof StringStream>("StringStream"))();
    const decoder = new StringDecoder(encoding);
    let pending = Promise.resolve();
    const write = async (chunk: Buffer): Promise<void> => {
      const value = decoder.write(chunk);
      if (value.length === 0) return;
      if (output.write(value)) return;
      await new Promise<void>((resolve) => output.once("drain", resolve));
    };
    this.on("data", (chunk: Buffer) => {
      this.pause();
      pending = pending.then(() => write(chunk)).then(() => { this.resume(); }).catch((error) => { output.destroy(error); });
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => {
      pending.then(() => {
        const tail = decoder.end();
        if (tail.length) output.end(tail);
        else output.end();
      }).catch((error) => output.destroy(error));
    });
    return output;
  }

  /** Decode bytes into a Framework StringStream. */
  toStringStream(encoding = "utf-8"): StringStream {
    return this.stringify(encoding);
  }

  /** Parse each Buffer through a synchronous or asynchronous parser. */
  parse<T>(parser: any): DataStream<T> {
    return (this as any).map(parser, DataStream) as DataStream<T>;
  }

  /** Alias for parse(). */
  toDataStream<T>(parser: (chunk: Buffer) => T | PromiseLike<T>): DataStream<T> {
    return this.parse(parser);
  }
}
