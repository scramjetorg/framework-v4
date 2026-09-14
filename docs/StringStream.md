# StringStream

Text stream with line, parser, encoding, and raw Node command helpers.

### `exec(command, options)`

Run a trusted shell command with raw string stdin and stdout.

**Parameters**

- `command` — unknown: Trusted shell command line.
- `options` — unknown: Child-process and abort options.

### `lines()`

Split text into lines while preserving stream backpressure.

### `shift()`

Remove a character prefix and report it before passing the remainder.

### `pop()`

Alias for shifting characters from the front of the stream.

### `split()`

Split text by a string or regular expression.

### `match()`

Emit text matching a regular expression.

### `append()`

Append fixed or computed text to each chunk.

### `prepend()`

Prepend fixed or computed text to each chunk.

### `JSONParse()`

Parse JSON values, one per line by default.

### `CSVParse()`

Parse CSV rows into arrays or header-keyed records.

### `parse()`

Parse each string chunk into a typed DataStream value.

### `toDataStream()`

Alias for parse().

### `toBufferStream()`

Encode each string chunk as UTF-8 bytes.

### `toStringStream()`

Return this StringStream unchanged.

### `static from()`

Create a StringStream from literal text or a string source.

### `static fromString()`

Create a StringStream from one literal string.

### `static fromURL()`

Fetch a URL and decode its body into a StringStream.
