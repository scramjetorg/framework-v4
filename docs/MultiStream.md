# MultiStream

Collection of Framework streams supporting map, filter, dedupe, and mux operations.

### `static from()`

Create a MultiStream from stream-like values.

### `add()`

Add a stream reference.

### `remove()`

Remove a stream reference.

### `map()`

Transform each member stream into a new MultiStream.

### `filter()`

Retain member streams selected by a predicate.

### `dedupe()`

Return a Framework MultiStream containing each stream reference once.

### `each()`

Observe each member stream and optional removals.

### `mux()`

Merge member streams into one Framework DataStream.
