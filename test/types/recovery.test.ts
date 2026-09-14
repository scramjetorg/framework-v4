import { Readable } from "node:stream";
import { BufferStream, DataStream, MultiStream, NumberStream, StringStream, WindowStream } from "scramjet";
import type { ExecOptions, ForkOptions } from "scramjet";

const data: DataStream<number> = DataStream.from([1, 2, 3]);
const mapped: Promise<number[]> = data.map((value) => value * 2).toArray();
const strings = StringStream.from(["1", "2"]);
const parsed: Promise<unknown[]> = strings.CSVParse().toArray();
const numbers: NumberStream = NumberStream.from([1, 2, 3]) as NumberStream;
const csvOutput: StringStream = data.CSVStringify();
const parsedOutput: DataStream<unknown> = strings.CSVParse() as DataStream<unknown>;
const bufferOutput: BufferStream = data.toBufferStream((value) => Buffer.from(String(value)));
const stringOutput: StringStream = data.toStringStream();
const parsedBuffer: DataStream<number> = bufferOutput.parse((value: Buffer) => value.length);
const parsedString: DataStream<number> = strings.parse((value) => Number(value));
const stringSplit: StringStream = strings.split("|");
const stringMatch: StringStream = strings.match(/x/g);
const stringShift: StringStream = strings.shift(1, (_value: string) => undefined);
const stringPop: StringStream = strings.pop(1, (_value: string) => undefined);
const bufferSplit: BufferStream = bufferOutput.split(Buffer.from("|"));
const bufferBroken: BufferStream = bufferOutput.breakup(2);
const bufferShift: BufferStream = bufferOutput.shift(1, (_value: Buffer) => undefined);
const bufferPop: BufferStream = bufferOutput.pop(1, (_value: Buffer) => undefined);
const bufferString: StringStream = bufferOutput.toStringStream("utf8");
const muxed: DataStream<unknown> = MultiStream.from([[1], [2]]).mux();
const multi: MultiStream = MultiStream.from([[1], [2]]);
const multiMapped: Promise<MultiStream> = multi.map((stream) => stream.map((value) => value));
const multiFiltered: Promise<MultiStream> = multi.filter(() => true);
const multiDeduped: MultiStream = multi.dedupe();
const multiAdded: MultiStream = multi.add(new DataStream<number>());
const multiRemoved: MultiStream = multi.remove(new DataStream<number>());
const windowSums: NumberStream = WindowStream.from([[1, 2]]).sum();
const windowAverages: NumberStream = WindowStream.from([[1, 2]]).avg();
const windows: WindowStream = data.window(2);
const separatedInto: DataStream<number> = data.separateInto({ sink: new DataStream<number>() }, () => "sink");
const execOutput: DataStream<unknown> = data.exec("node command");
const stringExecOutput: StringStream = strings.exec("node command", {} satisfies ExecOptions);
const distributeOutput: DataStream<unknown> = data.distribute("./transform.cjs", { concurrency: 2 } satisfies ForkOptions);
const delegateOutput: DataStream<unknown> = data.delegate("./transform.mjs");
const replayable: DataStream<number> = DataStream.from([1, 2]).keep(2);
const replayReader: Readable = replayable.rewind();
const replayTail: Readable = replayable.tail(1);

// These assertions are the composition/type-transition scaffold for REQ-007.
// They intentionally constrain only the transitions already exposed by the
// current declarations; stronger OFCA composition states belong to Core's type
// wave and must not be guessed here.
const transformed: Promise<unknown[]> = data.filter((value) => value > 1).toArray();
const mappedChain: DataStream<number> = data.map((value: number) => value * 2);
const filteredChain: DataStream<number> = data.filter((value: number) => value > 1);
const doneChain: DataStream<number> = data.do((value: number) => value);
const allChain: DataStream<number[]> = data.all([(value: number) => value]);
const raceChain: DataStream<number> = data.race([(value: number) => value]);
const tappedChain: DataStream<number> = data.tap();
const unorderChain: DataStream<string> = data.unorder((value: number) => String(value));
const copyChain: DataStream<number> = data.copy();
const whileChain: DataStream<number> = data.while((value: number) => value < 3);
const untilChain: DataStream<number> = data.until((value: number) => value > 3);
const intoChain: DataStream<number> = data.into((_out, value) => value, new DataStream<number>());
const pulled: Promise<void> = data.pull([4, 5]);
const immediate: number[] = data.reduceNow((acc, value) => { acc.push(value); return acc; }, [] as number[]);
const shifted: DataStream<number> = data.shift(1, (values) => values.length);
const separated: MultiStream = data.separate((value) => value % 2);
const stacked: DataStream<number> = data.stack();
const teed: DataStream<number> = data.tee(() => undefined);
const iterated: DataStream<number> = DataStream.fromIterator([1, 2][Symbol.iterator]());

// Local type-only state markers document the REQ-007 composition distinction
// without claiming that the unfinished Core composition types already exist.
type ComposableState<T> = { readonly kind: "composable"; readonly output: T };
type BreakingState<T> = { readonly kind: "breaking"; readonly output: T };
const mappedState: ComposableState<number> = { kind: "composable", output: 2 };
const csvState: BreakingState<string> = { kind: "breaking", output: "csv" };
// @ts-expect-error composition-breaking output cannot be used as composable
const invalidComposition: ComposableState<string> = csvState;

// @ts-expect-error CSV delimiter must be a string
strings.CSVParse({ delimiter: 1 });
// @ts-expect-error replay-like options are not accepted by DataStream.from
DataStream.from([1], { objectMode: "yes" });

void mapped;
void parsed;
void numbers;
void csvOutput;
void parsedOutput;
void bufferOutput;
void stringOutput;
void parsedBuffer;
void parsedString;
void stringSplit;
void stringMatch;
void stringShift;
void stringPop;
void bufferSplit;
void bufferBroken;
void bufferShift;
void bufferPop;
void bufferString;
void muxed;
void multi;
void multiMapped;
void multiFiltered;
void multiDeduped;
void multiAdded;
void multiRemoved;
void windowSums;
void windowAverages;
void windows;
void separatedInto;
void execOutput;
void stringExecOutput;
void distributeOutput;
void delegateOutput;
void replayable;
void replayReader;
void replayTail;
void transformed;
void mappedChain;
void filteredChain;
void doneChain;
void allChain;
void raceChain;
void tappedChain;
void unorderChain;
void copyChain;
void whileChain;
void untilChain;
void intoChain;
void pulled;
void immediate;
void shifted;
void separated;
void stacked;
void teed;
void iterated;
void mappedState;
void invalidComposition;
