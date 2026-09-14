import { CoreDataStream } from "./shared.js";
import { DataStream } from "./data-stream.js";
import type { CoreDataStreamOptions, DataStreamOptions } from "./shared.js";

/** Numeric DataStream with asynchronous sum and average reductions. */
export class NumberStream extends DataStream<number> {
  private readonly numberValueOf: (value: number) => number | PromiseLike<number>;

  constructor(options: DataStreamOptions = {}) {
    super(options);
    this.numberValueOf = Object.prototype.hasOwnProperty.call(options, "valueOf") && typeof options.valueOf === "function" ? options.valueOf as unknown as (value: number) => number | PromiseLike<number> : (value) => Number(value);
  }

  /** Create a NumberStream from a source. */
  static from(source: Parameters<typeof CoreDataStream.from>[0], options?: DataStreamOptions): NumberStream {
    return super.from(source, options as CoreDataStreamOptions) as NumberStream;
  }
  /** Sum projected numeric values. */
  async sum(): Promise<number> {
    return this.reduce(async (total, value) => total + await this.numberValueOf(value), 0);
  }

  /** Calculate the average of projected numeric values. */
  async avg(): Promise<number> {
    let count = 0;
    return this.reduce(async (total, value) => (total * count + await this.numberValueOf(value)) / ++count, 0);
  }
}
