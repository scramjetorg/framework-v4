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

export const pipeline = corePipeline.bind(DataStream) as (
  readable: Parameters<typeof DataStream.from>[0],
  ...transforms: unknown[]
) => DataStream;

export const from = <Chunk>(source: Parameters<typeof DataStream.from>[0], options?: DataStreamOptions): DataStream<Chunk> =>
  DataStream.from(source, options) as DataStream<Chunk>;
export const fromArray = <Chunk>(source: readonly Chunk[], options?: DataStreamOptions): DataStream<Chunk> =>
  DataStream.from(source, options) as DataStream<Chunk>;

export {
  BufferStream,
  DataStream,
  MultiStream,
  NumberStream,
  PromiseTransformStream,
  StringStream,
  StreamError,
  WindowStream,
  errors,
};

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
