# DataStream

Object-mode stream with Framework factories and format, replay, and execution helpers.

### `static from(source, options)`

Create a Framework stream from an iterable, async iterable, promise, or Node readable.

**Parameters**

- `source` — unknown: The input source.
- `options` — unknown: Node stream options.

### `static fromIterator(iterator, options)`

Create a stream from a synchronous or asynchronous iterator.

**Parameters**

- `iterator` — unknown: The iterator to consume.
- `options` — unknown: Node stream options.

### `map(callback)`

Apply a synchronous or asynchronous transform while retaining a Framework DataStream.

**Parameters**

- `callback` — unknown: Transform applied to each value.

### `filter()`

Keep values for which the predicate resolves truthy.

### `do()`

Observe each value and retain the original values.

### `all()`

Run all transforms for each value and emit their results as an array.

### `race()`

Run transforms concurrently and emit the fastest result for each value.

### `into()`

Write transform output into a caller-provided Framework stream.

### `tee()`

Observe or tee values without changing this stream.

### `unorder()`

Apply a transform concurrently and emit completions without ordering guarantees.

### `copy()`

Return a Framework stream copy that receives the same values.

### `while()`

Pass values while the predicate remains true.

### `until()`

Pass values until the predicate becomes true.

### `static fromURL(url)`

Fetch an HTTP(S) URL and expose its response body as a Framework stream.

**Parameters**

- `url` — unknown: An HTTP or HTTPS URL.

### `JSONStringify(newline)`

Serialize each value as one JSON value followed by `newline`.

**Parameters**

- `newline` — unknown: Line terminator appended to each serialized value.

### `CSVStringify(options)`

Serialize record or array values as CSV, optionally including a header.

**Parameters**

- `options` — unknown: CSV delimiter, newline, and header options.

### `bufferify()`

Serialize each value into a Framework BufferStream.

### `toBufferStream()`

Convert each value to a Buffer stream.

### `stringify()`

Serialize each value into a Framework StringStream.

### `toStringStream()`

Convert each value to a StringStream.

### `pull()`

Pull values from another source into this stream without ending it.

### `assign()`

Add fixed or computed object properties to each value.

### `concat()`

Concatenate this stream with subsequent readable sources.

### `accumulate()`

Reduce values into a caller-owned accumulator.

### `consume()`

Consume all values with an asynchronous consumer.

### `reduceNow()`

Start a reduction and return the accumulator immediately.

### `join()`

Insert a fixed or computed separator between values.

### `slice()`

Emit a bounded slice of the stream.

### `endWith()`

Append values after the source ends.

### `unshift()`

Prepend values or use Node's writable unshift form.

### `shift()`

Remove and report a prefix of values before passing the remainder.

### `peek()`

Observe a prefix without removing it from the stream.

### `empty()`

Invoke a callback if the stream emits no values.

### `batch()`

Group values into fixed-size batches, including a final partial batch.

### `timeBatch()`

Group values by elapsed time and optional maximum size.

### `rate()`

Limit emission to the requested rate.

### `debug()`

Observe values for diagnostics while passing them through unchanged.

### `window()`

Emit rolling arrays, including partial windows at the beginning.

### `separateInto()`

Route values into caller-provided targets without ending those targets.

### `keep()`

Retain a bounded live replay history for rewind and tail readers.

### `rewind()`

Read retained history from its beginning and follow future values.

### `tail()`

Read the retained suffix and follow future values.

### `flatten()`

Flatten iterable or async iterable values by one level.

### `flatMap()`

Map values to iterables and flatten their results.

### `remap()`

Let a mapper emit zero or more output values per input.

### `separate()`

Create a MultiStream with one lazily created branch per affinity key.

### `stack()`

Emit buffered values newest-first, optionally dropping older overflow.

### `toJSONArray()`

Serialize the stream as a JSON array string.

### `toJSONObject()`

Serialize the stream as a JSON object keyed by a function.

### `exec(command, options)`

Run a trusted shell command using JSON Lines on stdin/stdout.
Each input value is encoded as JSON plus a newline; each nonblank output line is parsed as JSON.
This is Node-only and does not impose a command, environment, or path policy.

**Parameters**

- `command` — unknown: Trusted shell command line.
- `options` — unknown: Child-process and abort options.

### `distribute(modulePath, options)`

Apply a trusted ESM or CommonJS fork module with bounded concurrency.
Results are emitted in worker completion order; this method does not create MultiStream fanout.

**Parameters**

- `modulePath` — ref: Path to a module exporting `(value, ) => result`.
- `options` — unknown: Concurrency and abort options.

### `delegate(modulePath, options)`

Apply a trusted ESM or CommonJS fork module with bounded concurrency.
Results are emitted in input order; this method does not create MultiStream fanout.

**Parameters**

- `modulePath` — ref: Path to a module exporting `(value, ) => result`.
- `options` — unknown: Concurrency and abort options.
