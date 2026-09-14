import type { Readable } from "node:stream";
import { CoreMultiStream } from "./shared.js";
import { DataStream } from "./data-stream.js";

export class MultiStream extends CoreMultiStream {
  static from(streams: any[]): MultiStream {
    return super.from(streams, DataStream as any) as unknown as MultiStream;
  }

  add(stream: Readable): this {
    super.add(stream);
    return this;
  }

  remove(stream: Readable): this {
    super.remove(stream);
    return this;
  }

  async map(callback: (stream: DataStream<any>) => unknown, onRemove?: (stream: DataStream<any>) => unknown): Promise<MultiStream> {
    return await (super.map as any).call(this, callback, onRemove) as MultiStream;
  }

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

  async each(callback: (stream: DataStream<any>) => unknown, onRemove?: (stream: DataStream<any>) => unknown): Promise<this> {
    await (super.each as any).call(this, callback, onRemove);
    return this;
  }

  mux(comparator?: any): DataStream<unknown> {
    return (super.mux as any).call(this, comparator, DataStream) as unknown as DataStream<unknown>;
  }
}

