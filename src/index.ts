import { DataStream } from "./data-stream.js";
import { StringStream } from "./string-stream.js";
import { BufferStream } from "./buffer-stream.js";
import { MultiStream } from "./multi-stream.js";
import { NumberStream } from "./number-stream.js";
import { WindowStream } from "./window-stream.js";
import { corePipeline, errors, PromiseTransformStream, registerFrameworkClasses, StreamError } from "./shared.js";
import type { DataStreamOptions } from "./shared.js";
export type { ExecOptions, ForkOptions } from "./execution.js";

export type { CsvOptions, DataStreamOptions, StringSource } from "./shared.js";

registerFrameworkClasses({ DataStream, StringStream, BufferStream, MultiStream, NumberStream, WindowStream });

/** Compose a readable source with Core-compatible transforms. */
export const pipeline = corePipeline.bind(DataStream) as (
  readable: Parameters<typeof DataStream.from>[0],
  ...transforms: unknown[]
) => DataStream;

/** Create a typed DataStream from a general source. */
export const from = <Chunk>(source: Parameters<typeof DataStream.from>[0], options?: DataStreamOptions): DataStream<Chunk> =>
  DataStream.from(source, options) as DataStream<Chunk>;
/** Create a typed DataStream from an array. */
export const fromArray = <Chunk>(source: readonly Chunk[], options?: DataStreamOptions): DataStream<Chunk> =>
  DataStream.from(source, options) as DataStream<Chunk>;

export {
  BufferStream,
  DataStream,
  MultiStream,
  NumberStream,
  StringStream,
  WindowStream,
};

/** Core promise-aware Transform constructor exposed for advanced pipelines. */
export { PromiseTransformStream };
/** Framework stream error constructor. */
export { StreamError };
/** Core error namespace retained for compatibility. */
export { errors };

/** Default object containing the complete public root API. */
const scramjet = {
  BufferStream,
  DataStream,
  MultiStream,
  NumberStream,
  PromiseTransformStream,
  StringStream,
  StreamError,
  WindowStream,
  errors,
  from,
  fromArray,
  pipeline,
};

export default scramjet;
