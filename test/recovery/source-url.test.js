import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";
import { DataStream, StringStream } from "../../dist/index.js";

test("fromURL supports HTTP, redirects, status rejection, and StringStream decoding", async (t) => {
  const server = createServer((request, response) => {
    if (request.url === "/redirect") {
      response.writeHead(302, { location: "/text" }).end();
    } else if (request.url === "/missing") {
      response.writeHead(404).end("missing");
    } else {
      response.writeHead(200, { "content-type": "text/plain; charset=utf-8" }).end("hello\nworld");
    }
  });
  await new Promise((resolve) => server.listen(0, resolve));
  t.after(() => server.close());
  const address = server.address();
  const url = new URL(`http://127.0.0.1:${address.port}`);

  assert.deepEqual(await StringStream.fromURL(new URL("/redirect", url)).then((stream) => stream.toArray()), ["hello\nworld"]);
  assert.deepEqual(await DataStream.fromURL(new URL("/text", url)).then((stream) => stream.toArray()), [Buffer.from("hello\nworld")]);
  await assert.rejects(() => DataStream.fromURL(new URL("/missing", url)), /HTTP 404/);
  await assert.rejects(() => DataStream.fromURL(new URL("file:///tmp/input")), /Unsupported URL protocol/);
});
