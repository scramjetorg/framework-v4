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
- `ReReadable` from `rereadable-stream` for bounded replayable writable histories.

CSV support is implemented in this package to retain the documented framework feature without a parser runtime dependency. It supports delimiter-separated rows, quoted cells, escaped quotes, and optional header records.

## v5 migration

v5 replaces the CommonJS plugin, worker, dynamic-module, legacy runner, Gulp, and generated-doc surfaces. It retains the stream transformation model and the documented JSON/CSV convenience features. OFCA is intentionally not a runtime dependency.

## Development

The root npm workspace must be reinstalled after this major version is adopted so its lockfile resolves `scramjet-core ^5` and `rereadable-stream ^2` locally. Until then, the build links the sibling packages' generated `dist/` directories only for replacement-package validation.

```sh
npm run build
npm test
npm run coverage
npm run typecheck
npm run check
npm run pack:check
```

`dist/` contains only compiled ESM, generated declarations, a sanitized manifest, this README, and the MIT license.

## License

MIT. See [LICENSE](LICENSE).
