# Framework v5 recovery inventory

Baseline: `v4.37.0` (`e8d0c14`). This is the Phase 1 compatibility ledger;
it is intentionally local to Framework and does not describe Core recovery.

## Disposition totals

The baseline contains **76 conformance/transition test files**: 75 files in
`test/methods/` plus `test/v1/cluster.js`. The disposition is:

| Disposition | Count | Meaning |
| --- | ---: | --- |
| Retained/runnable | 69 | Ported to independently runnable `node:test` recovery waves |
| Retained/pending | 1 | Public v4 surface retained, but implementation remains outside completed waves; explicit skip in `test/recovery/pending-methods.test.js` |
| REQ-005 removal | 2 | Plugin/mixin and string module loading are intentionally unavailable |
| REQ-009 placeholder | 4 | Execution/worker entry points are tracked as explicit throw-placeholder work |
| **Total** | **76** | Every historical conformance file has one disposition |

`framework.test.js`, the retained waves under `test/recovery/`, and
`removed-surfaces.test.js` are runnable independently after `npm run build`.
Pending tests never pass silently: each is a named skipped Node test with the
corresponding historical filename/method below.

## Historical test disposition ledger

| Historical file | Class/surface | Disposition and Phase 1 link |
| --- | --- | --- |
| `buffer-stream-breakup.js` | BufferStream.breakup | Retained/runnable — `string-buffer-waves.test.js` |
| `buffer-stream-constructor.js` | BufferStream constructor | Retained/runnable — `string-buffer-waves.test.js` |
| `buffer-stream-parse.js` | BufferStream.parse | Retained/runnable — `string-buffer-waves.test.js` |
| `buffer-stream-shift.js` | BufferStream.shift/pop | Retained/runnable — `string-buffer-waves.test.js` |
| `buffer-stream-split.js` | BufferStream.split | Retained/runnable — `string-buffer-waves.test.js` |
| `buffer-stream-tostringstream.js` | BufferStream.toStringStream | Retained/runnable — `string-buffer-waves.test.js` |
| `data-stream-accumulate.js` | DataStream.accumulate | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-all.js` | DataStream.all | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-assign.js` | DataStream.assign | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-batch.js` | DataStream.batch | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-catch.js` | DataStream.catch | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-concat.js` | DataStream.concat | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-constructor.js` | DataStream constructor | Retained/runnable — `framework.test.js` exports/factory wave |
| `data-stream-consume.js` | DataStream.consume | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-csv.js` | DataStream.CSVStringify | Retained/runnable — `data-stream-csv.test.js` |
| `data-stream-debug.js` | DataStream.debug | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-distribute.js` | DataStream.distribute | REQ-009 placeholder — `data-stream-waves.test.js` |
| `data-stream-empty.js` | DataStream.empty | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-endwith.js` | DataStream.endWith | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-exec.js` | DataStream.exec | REQ-009 placeholder — `data-stream-waves.test.js` |
| `data-stream-filter.js` | DataStream.filter | Retained/runnable — `data-stream-filter.test.js` |
| `data-stream-flatmap.js` | DataStream.flatMap | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-flatten.js` | DataStream.flatten | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-fromarray.js` | DataStream.fromArray | Retained/runnable — `data-stream-factories.test.js` |
| `data-stream-fromiterator.js` | DataStream.fromIterator | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-into.js` | DataStream.into | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-join.js` | DataStream.join | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-jsonparse.js` | StringStream.JSONParse | Retained/runnable — `string-stream-transform.test.js` |
| `data-stream-jsonstringify.js` | DataStream.JSONStringify | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-keep.js` | DataStream.keep | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-map.js` | DataStream.map | Retained/runnable — `data-stream-map.test.js` |
| `data-stream-peek.js` | DataStream.peek | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-pull.js` | DataStream.pull | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-race.js` | DataStream.race | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-raise.js` | DataStream.raise | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-rate.js` | DataStream.rate | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-reduce.js` | DataStream.reduce | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-reduceNow.js` | DataStream.reduceNow | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-remap.js` | DataStream.remap | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-selfinstance.js` | DataStream self-instance transition | Retained/runnable — Framework class/type wave |
| `data-stream-separate.js` | DataStream.separate | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-shift.js` | DataStream.shift | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-slice.js` | DataStream.slice | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-stack.js` | DataStream.stack | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-tap.js` | DataStream.tap | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-tee.js` | DataStream.tee | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-timebatch.js` | DataStream.timeBatch | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-toarray.js` | DataStream.toArray | Retained/runnable — `data-stream-factories.test.js` |
| `data-stream-tobufferstream.js` | DataStream.toBufferStream | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-tojsonarray.js` | DataStream.toJSONArray | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-tojsonobject.js` | DataStream.toJSONObject | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-tostringstream.js` | DataStream.toStringStream | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-unshift.js` | DataStream.unshift | Retained/runnable — `data-stream-waves.test.js` |
| `data-stream-until.js` | DataStream.until | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-use.js` | DataStream.use / module loading | REQ-005 removal — `removed-surfaces.test.js` |
| `data-stream-whens.js` | DataStream lifecycle promises | Retained/runnable — `data-stream-recovered.test.js` |
| `data-stream-while.js` | DataStream.while | Retained/runnable — `data-stream-recovered.test.js` |
| `multi-stream-add.js` | MultiStream.add | Retained/runnable — `multi-numeric-window-replay.test.js` |
| `multi-stream-constructor.js` | MultiStream constructor | Retained/runnable — `multi-numeric-window-replay.test.js` |
| `multi-stream-dedupe.js` | MultiStream.dedupe | Retained/runnable — `multi-numeric-window-replay.test.js` |
| `multi-stream-filter.js` | MultiStream.filter | Retained/runnable — `multi-numeric-window-replay.test.js` |
| `multi-stream-map.js` | MultiStream.map | Retained/runnable — `multi-numeric-window-replay.test.js` |
| `multi-stream-mux.js` | MultiStream.mux | Retained/runnable — `multi-numeric-window-replay.test.js` |
| `multi-stream-remove.js` | MultiStream.remove | Retained/runnable — `multi-numeric-window-replay.test.js` |
| `scramjet-plugin.js` | plugin/mixin registry | REQ-005 removal — `removed-surfaces.test.js` |
| `string-stream-append.js` | StringStream.append | Retained/runnable — `string-stream-transform.test.js` |
| `string-stream-constructor.js` | StringStream constructor/factory | Retained/runnable — `framework.test.js` and `string-stream-lines.test.js` |
| `string-stream-exec.js` | StringStream.exec | REQ-009 placeholder — `pending-methods.test.js` execution group |
| `string-stream-get.js` | StringStream.get | Retained/pending — StringStream group |
| `string-stream-match.js` | StringStream.match | Retained/runnable — `string-buffer-waves.test.js` |
| `string-stream-parse.js` | StringStream.parse | Retained/runnable — `string-buffer-waves.test.js` |
| `string-stream-prepend.js` | StringStream.prepend | Retained/runnable — `string-stream-transform.test.js` |
| `string-stream-shift.js` | StringStream.shift/pop | Retained/runnable — `string-buffer-waves.test.js` |
| `string-stream-split.js` | StringStream.split | Retained/runnable — `string-buffer-waves.test.js` |
| `string-stream-tobufferstream.js` | StringStream.toBufferStream | Retained/runnable — `string-buffer-waves.test.js` |
| `v1/cluster.js` | worker/cluster execution | REQ-009 placeholder — `pending-methods.test.js` execution group |

Benchmark files and `test/methods/data/**` fixtures are not conformance files;
helper programs under `test/methods/lib/**` support the historical tests and
are not public API surfaces.

## v4 exports, aliases, options, and transitions

| Baseline surface | v4 contract / alias | Phase 1 evidence |
| --- | --- | --- |
| Root classes | `DataStream`, `StringStream`, `BufferStream`, `MultiStream`, `NumberStream`, `WindowStream` | `framework.test.js`; pending transitions are explicit above |
| Root functions | `from`, `fromArray`, `pipeline` | `signatures.test.ts`; Framework `pipeline` is the Phase 3-bound adapter |
| Root utility exports | `PromiseTransformStream`, `StreamError`, `errors` | `framework.test.js` export wave; no new Core exports |
| Removed root exports | `plugin`, `API`, `createTransformModule`, `createReadModule` | `removed-surfaces.test.js` (REQ-005) |
| DataStream → StringStream | `JSONStringify`, `CSVStringify`, `toStringStream`, `toJSONArray`, `toJSONObject` | `data-stream-recovered.test.js` and CSV/JSON waves |
| StringStream → DataStream | `JSONParse`, `CSVParse`, `parse`, `toDataStream` | JSON/CSV and `string-buffer-waves.test.js` |
| DataStream → BufferStream | `toBufferStream`, `bufferify` | `data-stream-recovered.test.js`; UTF-8/default conversion and captured-reference coverage in `string-buffer-waves.test.js` |
| StringStream → BufferStream | `toBufferStream` | Framework Buffer sibling; UTF-8 conversion coverage in `string-buffer-waves.test.js` |
| BufferStream → StringStream | `toStringStream`, `stringify` | UTF-8 default, explicit encoding, and fragmented-byte coverage in `string-buffer-waves.test.js` |
| DataStream → MultiStream/WindowStream | `separate`, `window`, `separateInto` | `separate` and Multi/Window projections runnable; `window`/`separateInto` remain outside this wave |
| String input | `StringStream.from`, `fromString`, `StringSource` | `string-stream-lines.test.js`; raw strings are normalized locally |
| Data options | `DataStreamOptions`/`Options`: object mode, high water mark, encoding, OFCA transform options | `signatures.test.ts`, `test/types/recovery.test.ts` |
| CSV options | `delimiter?: string`, `header?: boolean`, `newline?: string` | `data-stream-csv.test.js`; negative delimiter assertion |
| Replay methods | `DataStream.keep(length)`, `rewind()`, `tail(count)` | `multi-numeric-window-replay.test.js`; live object/binary, drop, finish, error, and reader coverage |
| Class transitions | factories preserve Framework siblings; Multi mux/demux and Window projections return Framework DataStream/NumberStream | `multi-numeric-window-replay.test.js`, `number-stream.test.js`, `test/types/recovery.test.ts` |

## Type-test baseline

`test/signatures.test.ts` and `test/types/recovery.test.ts` are compiled by
`npm run typecheck`. They assert public factories, option failures, inferred
transform outputs, Framework class transitions, and composition-state
scaffolding. Composition-breaking versus composable operations are represented
by positive/negative placeholders without inventing a Core type contract; the
pending methods link to the exact retained rows above.

Phase 5 keeps JSON/CSV parsing local rather than adding an external parser:
the supported contract is narrow, and the local stateful parser directly
controls multiline quotes, chunk fragmentation, errors, and output backpressure.

## Phase 7 packaging and release disposition

| Surface | Disposition | Evidence |
| --- | --- | --- |
| Node package export | Built ESM/declarations from `dist/` | `package.json`, `assemble-dist.mjs`, packed Node consumer |
| Bun package export | Shipped `src/index.ts` under the root `bun` condition | `test/package-bun.test.ts`, packed Bun consumer |
| CI checks | Prepared build, lint, typecheck, Node test, coverage, pack, and Bun jobs | `.github/workflows/ci.yml` |
| Trusted publishing | Prepared manual OIDC workflow with `id-token: write`; no token or publish was run | `.github/workflows/release.yml` |
| Migration guidance | Documents removed APIs, URL sources, composition references, throws, and Node/Bun use | `MIGRATION.md` |

## Phase 8 source module map

| Module | Responsibility |
| --- | --- |
| `src/data-stream.ts` | Framework DataStream methods, conversions, URL sources, and local replay integration |
| `src/string-stream.ts` | StringStream parsing, transforms, and string sources |
| `src/buffer-stream.ts` | BufferStream splitting, framing, parsing, and decoding |
| `src/multi-stream.ts` | MultiStream Framework wrappers and deduplication |
| `src/number-stream.ts` | NumberStream aggregation |
| `src/window-stream.ts` | WindowStream numeric projections |
| `src/shared.ts` | Core adapter/types, CSV/URL helpers, class registry, and live replay reader state |
| `src/index.ts` | Root imports, registry initialization, named exports, and default object |
