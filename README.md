# Scramjet Framework

Scramjet v5 is a Node.js 22+ ESM framework that layers practical format helpers and replay integration over `scramjet-core`.

```js
import { DataStream, StringStream } from "scramjet";

const records = await StringStream
  .from("name,age\nAda,36\n")
  .CSVParse()
  .map((row) => ({ ...row, age: Number(row.age) }))
  .toArray();
```

## Public API

- Core-compatible `DataStream`, `StringStream`, `BufferStream`, `MultiStream`, and `PromiseTransformStream` exports.
- `NumberStream` and `WindowStream` numeric helpers.
- `StringStream.lines`, `append`, `prepend`, `JSONParse`, and `CSVParse`.
- `DataStream.JSONStringify` and `CSVStringify`.
- `DataStream.keep(length)`, `rewind()`, and `tail(count)` for bounded live replay.
- Node-only `DataStream.exec`, `StringStream.exec`, `DataStream.distribute`, and
  `DataStream.delegate` execution helpers.

Generated API references are checked in under [`docs/`](docs/) and are included
in the package distribution.

CSV support is implemented in this package to retain the documented framework feature without a parser runtime dependency. It supports delimiter-separated rows, quoted cells, escaped quotes, and optional header records.

### String and Buffer conversions

`BufferStream.toStringStream()` uses UTF-8 by default; pass a Node encoding to
select another decoding. It returns a Framework `StringStream` and uses a
stateful decoder, so UTF-8 characters split across incoming Buffer chunks are
reassembled before emission. `StringStream.toBufferStream()` encodes each
string chunk as UTF-8 and returns a Framework `BufferStream`. Both conversions
remain ordinary stream compositions: captured references continue to observe
later writes, and Node pipe backpressure is preserved.

### Live replay

`DataStream.keep(length)` is chainable and retains at most `length` chunks,
including zero-length and binary histories. `rewind()` replays all retained
chunks; `tail(count)` starts at the requested retained suffix. Both return
independent Node-readable readers that continue with future source chunks,
honor reader backpressure, emit `drop` counts when history advances past a
reader, and finish or fail with the source.

## Packaging and migration

v5 replaces the CommonJS plugin, worker_threads/WASM backend, dynamic-module, legacy runner, Gulp, and generated-doc surfaces. It retains the stream transformation model and the documented JSON/CSV convenience features. OFCA is intentionally not a runtime dependency. See [MIGRATION.md](MIGRATION.md) for execution, removed APIs, URL sources, composition semantics, and Node/Bun use.

The package has root-only conditional exports: Node resolves built ESM JavaScript
and declarations, while Bun resolves the shipped TypeScript source. Deep imports
are unsupported.

## Development

The root npm workspace must be reinstalled after this major version is adopted so its lockfile resolves `scramjet-core ^5` locally. Framework development uses normal declared dependency resolution; it does not rewrite dependencies or link generated sibling artifacts.

```sh
npm run build
npm test
npm run coverage
npm run typecheck
npm run check
npm run pack:check
npm run test:bun
```

`dist/` contains compiled ESM, generated declarations, the shared TypeScript
source for Bun, a sanitized manifest, package documentation, and the MIT license.

## License

MIT. See [LICENSE](LICENSE).
