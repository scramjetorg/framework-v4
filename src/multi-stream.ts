import type { Readable } from "node:stream";
import { CoreMultiStream } from "./shared.js";
import { DataStream } from "./data-stream.js";

/** Collection of Framework streams supporting map, filter, dedupe, and mux operations. */
export class MultiStream extends CoreMultiStream {
  /** Create a MultiStream from stream-like values. */
  static from(streams: any[]): MultiStream {
    return super.from(streams, DataStream as any) as unknown as MultiStream;
  }

  /** Add a stream reference. */
  add(stream: Readable): this {
    super.add(stream);
    return this;
  }

  /** Remove a stream reference. */
  remove(stream: Readable): this {
    super.remove(stream);
    return this;
  }

  /** Transform each member stream into a new MultiStream. */
  async map(callback: (stream: DataStream<any>) => unknown, onRemove?: (stream: DataStream<any>) => unknown): Promise<MultiStream> {
    return await (super.map as any).call(this, callback, onRemove) as MultiStream;
  }

  /** Retain member streams selected by a predicate. */
  async filter(callback: (stream: DataStream<any>) => unknown): Promise<MultiStream> {
    return await (super.filter as any).call(this, callback) as MultiStream;
  }

  /** Return a Framework MultiStream containing each stream reference once. */
  dedupe(): MultiStream {
    const seen = new Set<Readable>();
    return new MultiStream(this.streams.filter((stream) => {
      if (seen.has(stream)) return false;
      seen.add(stream);
      return true;
    }));
  }

  /** Observe each member stream and optional removals. */
  async each(callback: (stream: DataStream<any>) => unknown, onRemove?: (stream: DataStream<any>) => unknown): Promise<this> {
    await (super.each as any).call(this, callback, onRemove);
    return this;
  }

  /** Merge member streams into one Framework DataStream. */
  mux(comparator?: any): DataStream<unknown> {
    return (super.mux as any).call(this, comparator, DataStream) as unknown as DataStream<unknown>;
  }
}
