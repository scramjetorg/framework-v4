import { CoreDataStream } from "./shared.js";
import { DataStream } from "./data-stream.js";
import { NumberStream } from "./number-stream.js";
import type { CoreDataStreamOptions, DataStreamOptions } from "./shared.js";

/** DataStream whose values are rolling windows, with numeric projections. */
export class WindowStream extends DataStream<unknown[]> {
  private readonly windowValueOf: (value: unknown) => number | PromiseLike<number>;

  constructor(options: DataStreamOptions = {}) {
    super(options);
    this.windowValueOf = Object.prototype.hasOwnProperty.call(options, "valueOf") && typeof options.valueOf === "function" ? options.valueOf as unknown as (value: unknown) => number | PromiseLike<number> : (value) => Number(value);
  }

  /** Create a WindowStream from a source of window arrays. */
  static from(source: Parameters<typeof CoreDataStream.from>[0], options?: DataStreamOptions): WindowStream {
    return super.from(source, options as CoreDataStreamOptions) as WindowStream;
  }

  /** Sum each window using an optional asynchronous projection. */
  sum(project: (value: unknown) => number | PromiseLike<number> = this.windowValueOf): NumberStream {
    return (this as any).map(async (window: unknown[]) => {
      let total = 0;
      for (const value of window) total += await project(value);
      return total;
    }, NumberStream) as NumberStream;
  }

  /** Average each window using an optional asynchronous projection. */
  avg(project: (value: unknown) => number | PromiseLike<number> = this.windowValueOf): NumberStream {
    return (this as any).map(async (window: unknown[]) => {
      if (window.length === 0) return 0;
      let total = 0;
      for (const value of window) total += await project(value);
      return total / window.length;
    }, NumberStream) as NumberStream;
  }
}
