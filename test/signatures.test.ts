import { DataStream, NumberStream, ReReadable, StringStream } from "scramjet";

const rows = StringStream.from("name,age\nAda,36\n").CSVParse();
const records: Promise<Array<Record<string, string> | string[]>> = rows.toArray();
const numbers: NumberStream = NumberStream.from([1, 2]) as NumberStream;
const history = new ReReadable({ length: 2, objectMode: true });
const values: Promise<number[]> = DataStream.from([1, 2]).map((value) => value * 2).toArray();

// @ts-expect-error CSV delimiter must be a string
rows.CSVParse({ delimiter: 1 });
// @ts-expect-error replay length must be numeric
new ReReadable({ length: "2" });

void records;
void numbers;
void history;
void values;
