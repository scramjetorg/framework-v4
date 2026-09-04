import type { Readable } from "node:stream";
import { ReReadable } from "rereadable-stream";
import {
  BufferStream,
  DataStream as CoreDataStream,
  type DataStreamOptions,
  errors,
  MultiStream,
  PromiseTransformStream,
  pipeline,
  StreamError,
} from "scramjet-core";

export type CsvOptions = {
  delimiter?: string;
  header?: boolean;
  newline?: string;
};

export type StringSource<Chunk = unknown> =
  | Iterable<Chunk>
  | AsyncIterable<Chunk>
  | Readable
  | PromiseLike<Iterable<Chunk> | AsyncIterable<Chunk> | Readable>;

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

function stringifyCsvCell(value: unknown, delimiter: string): string {
  const cell = String(value ?? "");
  return cell.includes(delimiter) || cell.includes('"') || /\r|\n/.test(cell)
    ? `"${cell.replaceAll('"', '""')}"`
    : cell;
}

export class DataStream<Chunk = unknown> extends CoreDataStream<Chunk> {
  static override from<Chunk>(source: Parameters<typeof CoreDataStream.from<Chunk>>[0], options?: DataStreamOptions<Chunk, Chunk>): DataStream<Chunk> {
    // biome-ignore lint/complexity/noThisInStatic: the inherited factory must construct this framework subclass.
    return super.from(source, options) as DataStream<Chunk>;
  }

  JSONStringify(newline = "\n"): StringStream {
    const output = new StringStream();
    this.map((chunk) => `${JSON.stringify(chunk)}${newline}`).pipe(output);
    return output;
  }

  CSVStringify(options: CsvOptions = {}): StringStream {
    const delimiter = options.delimiter ?? ",";
    const newline = options.newline ?? "\n";
    const includeHeader = options.header ?? true;
    const output = new StringStream();
    let header: string[] | undefined;
    let wroteHeader = false;

    this.on("data", (chunk: Record<string, unknown>) => {
      try {
        header ??= Object.keys(chunk);
        if (includeHeader && !wroteHeader) {
          output.write(`${header.map((key) => stringifyCsvCell(key, delimiter)).join(delimiter)}${newline}`);
          wroteHeader = true;
        }
        output.write(`${header.map((key) => stringifyCsvCell(chunk[key], delimiter)).join(delimiter)}${newline}`);
      } catch (error) {
        output.destroy(error as Error);
      }
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => output.end());
    return output;
  }
}

class FrameworkStringStream extends DataStream<string> {

  static get SPLIT_LINE(): RegExp {
    return /\r\n?|\n/;
  }

  lines(): CoreDataStream<string> {
    const output = new CoreDataStream<string>();
    let remainder = "";
    this.on("data", (chunk: string) => {
      const parts = `${remainder}${chunk}`.split(FrameworkStringStream.SPLIT_LINE);
      remainder = parts.pop() ?? "";
      for (const part of parts) output.write(part);
    });
    this.once("error", (error) => output.destroy(error));
    this.once("end", () => {
      if (remainder.length > 0) output.write(remainder);
      output.end();
    });
    return output;
  }

  append(value: string | ((chunk: string) => string | PromiseLike<string>)): CoreDataStream<string> {
    return this.map(async (chunk) => `${chunk}${typeof value === "function" ? await value(chunk) : value}`);
  }

  prepend(value: string | ((chunk: string) => string | PromiseLike<string>)): CoreDataStream<string> {
    return this.map(async (chunk) => `${typeof value === "function" ? await value(chunk) : value}${chunk}`);
  }

  JSONParse(perLine = true): CoreDataStream<unknown> {
    const source = perLine ? this.lines() : this;
    return source.filter((line) => line.length > 0).map((line) => JSON.parse(line));
  }

  CSVParse(options: CsvOptions = {}): CoreDataStream<Record<string, string> | string[]> {
    const delimiter = options.delimiter ?? ",";
    const hasHeader = options.header ?? true;
    const output = new CoreDataStream<Record<string, string> | string[]>();
    let header: string[] | undefined;
    const lines = this.lines();

    lines.on("data", (line: string) => {
      try {
        if (line.length === 0) return;
        const row = parseCsvRow(line, delimiter);
        if (hasHeader && !header) {
          header = row;
        } else if (header) {
          output.write(Object.fromEntries(header.map((key, index) => [key, row[index] ?? ""])));
        } else {
          output.write(row);
        }
      } catch (error) {
        output.destroy(error as Error);
      }
    });
    lines.once("error", (error) => output.destroy(error));
    lines.once("end", () => output.end());
    return output;
  }
}

export type StringStream = FrameworkStringStream;
export type StringStreamConstructor = (new (
  options?: DataStreamOptions<string, string>,
) => FrameworkStringStream) & {
  readonly SPLIT_LINE: RegExp;
  from<Chunk>(
    source: StringSource<Chunk>,
    options?: DataStreamOptions<Chunk, Chunk>,
  ): FrameworkStringStream;
  fromArray<Chunk>(
    source: readonly Chunk[],
    options?: DataStreamOptions<Chunk, Chunk>,
  ): FrameworkStringStream;
  fromString(value: string): FrameworkStringStream;
};

function stringFrom<Chunk>(
  source: StringSource<Chunk>,
  options?: DataStreamOptions<Chunk, Chunk>,
): FrameworkStringStream {
  const output = new FrameworkStringStream();
  CoreDataStream.from(source, options).map((chunk) => String(chunk)).pipe(output);
  return output;
}

export const StringStream: StringStreamConstructor = Object.assign(
  FrameworkStringStream,
  {
    from: stringFrom,
    fromArray: <Chunk>(source: readonly Chunk[], options?: DataStreamOptions<Chunk, Chunk>) =>
      stringFrom(source, options),
    fromString: (value: string) => stringFrom(value),
  },
);

export class NumberStream extends DataStream<number> {
  async sum(): Promise<number> {
    return this.reduce((total, value) => total + Number(value), 0);
  }

  async avg(): Promise<number> {
    let count = 0;
    return this.reduce((total, value) => (total * count + Number(value)) / ++count, 0);
  }
}

export class WindowStream extends DataStream<unknown[]> {
  sum(project: (value: unknown) => number = Number): CoreDataStream<number> {
    return this.map((window: unknown[]) => window.reduce<number>((total, value) => total + project(value), 0)) as CoreDataStream<number>;
  }
}

export {
  BufferStream,
  errors,
  MultiStream,
  PromiseTransformStream,
  pipeline,
  ReReadable,
  StreamError,
};

export const from = DataStream.from.bind(DataStream);
export const fromArray = DataStream.fromArray.bind(DataStream);

const scramjet = {
  BufferStream,
  DataStream,
  MultiStream,
  NumberStream,
  PromiseTransformStream,
  ReReadable,
  StringStream,
  StreamError,
  WindowStream,
  errors,
  from,
  fromArray,
  pipeline,
};

export default scramjet;
