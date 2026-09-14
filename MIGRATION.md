# v4 to v5 migration

Scramjet Framework v5 is a Node.js 22+ ESM package. Node consumers load built
JavaScript and declarations; Bun consumers use the same implementation's shipped
TypeScript source through the root `bun` export. Deep imports are not supported.

## Removed surfaces

- CommonJS entrypoints, plugins/mixins, string-module loading, dynamic module
  factories, and the legacy Gulp/tooling surface were removed. Checked-in API
  documentation is generated from the emitted Framework ESM build.
- `exec`, `distribute`, and `delegate` use the Node-only execution backend
  described below. Legacy cluster/worker APIs remain unavailable.
- Deep imports and undocumented legacy helpers are unavailable; use the root
  package exports and the documented stream classes.

| v4 surface | v5 disposition | Migration |
| --- | --- | --- |
| `plugin` / plugin and mixin registry | Removed (REQ-005) | Use documented root classes; there is no plugin registry or mixin API. |
| `API` compatibility export | Removed (REQ-005) | Import the documented root package exports. |
| `createTransformModule` | Removed (REQ-005) | Use ordinary functions with `DataStream.map`, `flatMap`, or `remap`. |
| `createReadModule` | Removed (REQ-005) | Use `DataStream.from`, `StringStream.from`, or explicit `fromURL(URL)`. |
| String `use` / string module loading | Removed (REQ-005) | String values remain literal stream data; no module resolution occurs. |
| String `from` module-loader overload | Removed (REQ-005) | Use literal strings with `StringStream.from("text")`; use `fromURL(new URL(...))` for HTTP(S). |
| String `pipeline` module-loader path | Removed (REQ-005) | Use the root `pipeline` with stream values and ordinary transform functions. |
| `ReReadable` | Replaced locally | Use chainable `DataStream.keep(length)`, then `rewind()` or `tail(count)`. |
| `exec`, `distribute`, `delegate` | Node child-process backend | Use JSON Lines for `DataStream.exec`; use trusted ESM/CJS modules for forked transforms. |
| `cluster`, worker/thread execution | Unavailable | Node-only child processes are used instead; no worker_threads or WASM backend is provided. |

## Sources and URLs

Use literal strings with `StringStream.from("text")`. URL sources must be explicit
`URL` instances, for example `StringStream.fromURL(new URL("https://example.test"))`.
HTTP and HTTPS redirects are followed by the platform fetch implementation, and
non-2xx responses reject.

## Composition and captured references

Composition follows Core's in-place model. A captured stream reference observes
later writes and composition; operations that create a branch return a Framework
family sibling stream. `tap()` is optional and is not required to keep a reference
live.

## Node and Bun

```js
import { DataStream } from "scramjet";

const values = await DataStream.from([1, 2])
  .map((value) => value * 2)
  .toArray();
```

Node resolves the built ESM/declaration branch. Bun resolves `src/index.ts` from
the same package tarball. Both runtimes use the same public root API and should
not rely on `dist` or `src` deep imports.

## Node execution

The historical surface was documented as `exec`, `distribute`, `cluster`, worker/thread execution;
the first two now use the Node backend below, while the legacy cluster/worker portion remains unavailable.

`DataStream.exec(command)` writes one JSON value plus a newline per input and
parses nonblank JSON Lines from stdout. `StringStream.exec(command)` forwards
raw string input and output. Commands are trusted shell command lines; stderr,
nonzero exits, malformed JSON, aborts, and stream backpressure fail the output
stream.

`DataStream.distribute(modulePath, { concurrency })` and
`DataStream.delegate(modulePath, { concurrency })` fork trusted ESM or CJS
modules exporting `(value, { ref }) => result`. `distribute` emits completion
order; `delegate` preserves input order. Module stdout is reserved for the
protocol and arbitrary module code runs with the caller's process privileges;
callers are responsible for command/module, environment, and path risks.
These APIs return a single `DataStream`; use `MultiStream.mux` or explicit
mapping when fanout is required.
