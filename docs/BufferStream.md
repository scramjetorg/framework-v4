# BufferStream

Byte-oriented Framework stream with safe splitting, decoding, and parsing helpers.

### `shift()`

Remove a byte prefix and report it before passing the remainder.

### `pop()`

Alias for shifting bytes from the front of the stream.

### `split()`

Split buffered bytes on a non-empty string or Buffer delimiter.

### `breakup()`

Break each input into chunks no larger than `size`.

### `stringify()`

Decode bytes with a stateful Node StringDecoder.

### `toStringStream()`

Decode bytes into a Framework StringStream.

### `parse()`

Parse each Buffer through a synchronous or asynchronous parser.

### `toDataStream()`

Alias for parse().
