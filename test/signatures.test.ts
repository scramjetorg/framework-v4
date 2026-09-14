import { DataStream, NumberStream, StringStream } from "scramjet";

const rows = StringStream.from("name,age\nAda,36\n").CSVParse();
const records: Promise<unknown[]> = rows.toArray();
const numbers: NumberStream = NumberStream.from([1, 2]) as NumberStream;
const history: DataStream<number> = DataStream.from([1, 2]).keep(2);
const values: Promise<number[]> = DataStream.from([1, 2]).map((value) => value * 2).toArray();

// @ts-expect-error CSV delimiter must be a string
rows.CSVParse({ delimiter: 1 });
// @ts-expect-error replay length must be numeric
DataStream.from([1]).keep("2");
void records;
void numbers;
void history;
void values;
