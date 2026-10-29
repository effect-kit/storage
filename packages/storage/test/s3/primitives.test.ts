import { assert, it } from "@effect/vitest";
import { Effect, Result, Stream } from "effect";

import { Storage } from "../../src/index.js";
import { drain, httpFixture, xml } from "./http-fixture.js";

it.effect("S3 key and metadata restrictions reject before a source is consumed", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let consumed = false;
      const fixture = yield* httpFixture((_incoming, response) => response.end());
      const source = Stream.fromEffect(
        Effect.sync(() => {
          consumed = true;
          return new Uint8Array();
        }),
      );
      for (const input of [
        { key: "é".repeat(513), body: source },
        { key: "metadata", body: source, customMetadata: { Label: "a", label: "b" } },
        { key: "metadata", body: source, customMetadata: { value: "a".repeat(2048) } },
      ]) {
        const result = yield* Effect.result(fixture.storage.put(input));
        assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "InvalidInput");
      }
      assert.isFalse(consumed);
    }),
  ),
);

it.effect(
  "S3 put preserves opaque keys, metadata, atomic conditions, version and checksum fields",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let path = "";
        let seen: Record<string, string | string[] | undefined> = {};
        const fixture = yield* httpFixture((incoming, response) => {
          path = incoming.url ?? "";
          seen = incoming.headers;
          void drain(incoming).then(() => {
            response.writeHead(200, {
              etag: '"validator"',
              "x-amz-version-id": "9007199254740993",
              "x-amz-checksum-crc32": "AAAAAA==",
              "x-amz-checksum-type": "FULL_OBJECT",
            });
            return response.end();
          });
        });
        const saved = yield* fixture.storage.put({
          key: "a//%2F/é",
          body: new Uint8Array(),
          condition: { _tag: "CreateOnly" },
          contentType: "text/plain",
          customMetadata: { label: "value" },
        });
        assert.include(path, "/a//%252F/%C3%A9");
        assert.strictEqual(seen["if-none-match"], "*");
        assert.strictEqual(seen["x-amz-meta-label"], "value");
        assert.isDefined(seen["x-amz-checksum-crc32"]);
        assert.strictEqual(saved.etag, '"validator"');
        assert.strictEqual(saved.ref.version?.id, "9007199254740993");
        assert.strictEqual(saved.checksums?.[0]?.scope, "full-object");
      }),
    ),
);

it.effect("S3 scoped get uses one response for metadata/body and preserves full range size", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let range: string | undefined;
      let path = "";
      const fixture = yield* httpFixture((incoming, response) => {
        range = incoming.headers.range;
        path = incoming.url ?? "";
        response.writeHead(206, {
          "content-length": "2",
          "content-range": "bytes 1-2/4",
          etag: '"same-response"',
          "x-amz-version-id": "native/version",
        });
        response.end(new Uint8Array([1, 2]));
      });
      const response = yield* fixture.storage.get({
        ref: {
          storeId: fixture.storage.storeId,
          key: "ranged",
          version: { _tag: "S3Version", id: "native/version" },
        },
        range: { _tag: "Bounded", start: 1, end: 2 },
      });
      assert.strictEqual(range, "bytes=1-2");
      assert.include(path, "versionId=native%2Fversion");
      assert.strictEqual(response.metadata.size, 4);
      assert.strictEqual(response.metadata.etag, '"same-response"');
      if (response._tag !== "Object") return assert.fail("Expected object");
      assert.strictEqual(response.contentLength, 2);
      assert.deepStrictEqual(
        Array.from(yield* Stream.runCollect(response.body)).flatMap((chunk) => Array.from(chunk)),
        [1, 2],
      );
    }),
  ),
);

it.effect("S3 distinguishes 304, ranges, denied, missing and ambiguous 404 responses", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture((incoming, response) => {
        const key = incoming.url?.split("?")[0]?.split("/").at(-1);
        if (key === "cached") {
          response.writeHead(304);
          return response.end();
        }
        if (key === "range") return xml(response, "<Error><Code>InvalidRange</Code></Error>", 416);
        if (key === "denied")
          return xml(
            response,
            "<Error><Code>AccessDenied</Code><Message>SECRET</Message></Error>",
            403,
          );
        if (key === "unknown")
          return xml(response, "<Error><Code>NoSuchBucket</Code></Error>", 404);
        xml(response, "<Error><Code>NoSuchKey</Code></Error>", 404);
      });
      const cached = yield* fixture.storage.get({
        key: "cached",
        condition: { _tag: "IfNoneMatch", etag: "*" },
      });
      assert.strictEqual(cached._tag, "NotModified");
      assert.notProperty(cached, "body");
      for (const [key, reason] of [
        ["range", "RangeNotSatisfiable"],
        ["denied", "AccessDenied"],
        ["missing", "NotFound"],
        ["unknown", "ProviderFailure"],
      ]) {
        const result = yield* Effect.result(fixture.storage.get({ key: key ?? "" }));
        assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === reason);
        assert.notInclude(JSON.stringify(result), "SECRET");
      }
    }),
  ),
);

it.effect("S3 rejects a mismatched range response and disposes its body", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture((_incoming, response) => {
        response.writeHead(206, { "content-length": "2", "content-range": "bytes 0-1/4" });
        response.end(new Uint8Array([0, 1]));
      });
      const result = yield* Effect.result(
        fixture.storage.get({ key: "range", range: { _tag: "From", start: 2 } }),
      );
      assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "ProviderFailure");
    }),
  ),
);

it.effect(
  "SDK checksum validation becomes an integrity failure without exposing native messages",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture((_incoming, response) => {
          response.writeHead(200, { "content-length": "3", "x-amz-checksum-crc32": "AAAAAA==" });
          response.end("bad");
        });
        const result = yield* Effect.result(
          Storage.getBytes({ key: "corrupt", maxBytes: 3 }).pipe(
            Effect.provideService(Storage, fixture.storage),
          ),
        );
        assert.isTrue(
          Result.isFailure(result) && result.failure.reason._tag === "IntegrityMismatch",
        );
      }),
    ),
);

it.effect("S3 listing retains empty continuation pages and binds cursors to the query", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let calls = 0;
      const fixture = yield* httpFixture((incoming, response) => {
        calls++;
        const next = new URL(incoming.url ?? "", "http://fixture").searchParams.get(
          "continuation-token",
        );
        xml(
          response,
          next
            ? "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>p/a</Key><Size>0</Size></Contents><CommonPrefixes><Prefix>p/folder/</Prefix></CommonPrefixes></ListBucketResult>"
            : "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>opaque/native</NextContinuationToken></ListBucketResult>",
        );
      });
      const first = yield* fixture.storage.listPage({ prefix: "p/", delimiter: "/", limit: 1 });
      assert.isEmpty(first.entries);
      assert.isDefined(first.cursor);
      const invalid = yield* Effect.result(
        fixture.storage.listPage({
          prefix: "other",
          delimiter: "/",
          limit: 1,
          cursor: first.cursor,
        }),
      );
      assert.isTrue(Result.isFailure(invalid) && invalid.failure.reason._tag === "InvalidInput");
      assert.strictEqual(calls, 1);
      const entries = yield* Stream.runCollect(
        Storage.list({ prefix: "p/", delimiter: "/", limit: 1 }).pipe(
          Stream.provideService(Storage, fixture.storage),
        ),
      );
      assert.strictEqual(entries.length, 2);
    }),
  ),
);

it.effect(
  "native put validates checksum options and rejects mismatched byte lengths before sending",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let calls = 0;
        const fixture = yield* httpFixture((_incoming, response) => {
          calls++;
          return response.end();
        });
        const result = yield* Effect.result(
          fixture.storage.putObject({ key: "short", body: new Uint8Array(1), contentLength: 2 }),
        );
        assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "LengthMismatch");
        const invalid = yield* Effect.result(
          fixture.storage.putObject(
            { key: "invalid", body: new Uint8Array() },
            { ChecksumSHA256: "SECRET\r\nheader" },
          ),
        );
        assert.isTrue(Result.isFailure(invalid) && invalid.failure.reason._tag === "InvalidInput");
        assert.strictEqual(calls, 0);
      }),
    ),
);
