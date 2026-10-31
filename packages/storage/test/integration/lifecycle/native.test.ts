import { assert, it } from "@effect/vitest";
import { Effect, Redacted, Result } from "effect";

import { AzureProtection } from "../../../src/providers/azure/governance.js";
import { AzureStorage } from "../../../src/providers/azure/index.js";
import {
  AzureMetadata,
  AzureProperties,
  AzureTags,
  AzureVersions,
} from "../../../src/providers/azure/lifecycle.js";
import { GcsAdministration } from "../../../src/providers/gcs/admin.js";
import { GcsStorage } from "../../../src/providers/gcs/index.js";
import { GcsMetadata, GcsVersions, GcsRecovery } from "../../../src/providers/gcs/lifecycle.js";
import { WasabiStorage } from "../../../src/providers/wasabi/index.js";
import { WasabiNative, WasabiCompliance } from "../../../src/providers/wasabi/native.js";
import { httpFixture, drain, xml, fixtureCredentials } from "../../fixtures/s3/http-fixture.js";

it.effect(
  "Wasabi rejects XML entity declarations and incomplete MOVE receipts without claiming completion",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let receipt =
          '<!DOCTYPE result [<!ENTITY secret SYSTEM "file:///unread">]><MoveObjectResult>&secret;</MoveObjectResult>';
        const fixture = yield* httpFixture((incoming, response) => {
          incoming.resume();
          xml(response, receipt);
        });
        const native = WasabiNative.make(
          yield* WasabiStorage.make({ ...fixture.config, credentials: fixtureCredentials() }),
        );
        const input = {
          sourceKey: "source",
          destinationKey: "destination",
          overwrite: false,
          intent: "rename-object-and-all-versions" as const,
        };
        const invalid = yield* Effect.result(native.move(input));
        assert.isTrue(Result.isFailure(invalid) && invalid.failure.mutationOutcome === "unknown");
        receipt = "<MoveObjectResult><MoveCount>0</MoveCount></MoveObjectResult>";
        const missing = yield* Effect.result(native.move(input));
        assert.isTrue(Result.isFailure(missing) && missing.failure.mutationOutcome === "unknown");
      }),
    ),
);

it.effect(
  "GCS metadata uses lossless generation and metageneration CAS; native recovery stays create-only",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const writes: Array<{ url: URL; body: unknown }> = [];
        const meta = {
          name: "a%2Fb",
          generation: "9007199254740993",
          metageneration: "7",
          size: "3",
          etag: "opaque",
          metadata: { keep: "yes", remove: "old" },
          contentType: "application/pdf",
        };
        const fixture = yield* httpFixture((incoming, response) => {
          void drain(incoming).then((bytes) => {
            const url = new URL(incoming.url ?? "/", "http://localhost");
            if (incoming.method !== "GET")
              writes.push({
                url,
                body: bytes.length
                  ? (JSON.parse(Buffer.from(bytes).toString()) as unknown)
                  : undefined,
              });
            response.setHeader("content-type", "application/json");
            response.end(JSON.stringify(url.pathname.endsWith("/o") ? { items: [meta] } : meta));
            return undefined;
          });
        });
        const storage = yield* GcsStorage.make({
          bucket: "test-bucket",
          apiEndpoint: fixture.endpoint,
          useAuthWithCustomEndpoint: false,
        });
        const ref = {
          storeId: storage.storeId,
          key: "a%2Fb",
          version: { _tag: "GcsGeneration" as const, id: meta.generation, metageneration: "7" },
        };
        yield* GcsMetadata.make(storage).merge({
          ref,
          patch: { customMetadata: { remove: null, added: "new" } },
        });
        assert.strictEqual(writes[0]?.url.searchParams.get("ifGenerationMatch"), meta.generation);
        assert.strictEqual(writes[0]?.url.searchParams.get("ifMetagenerationMatch"), "7");
        assert.deepStrictEqual((writes[0]?.body as { metadata: unknown } | undefined)?.metadata, {
          keep: "yes",
          added: "new",
        });
        assert.strictEqual(
          (yield* GcsVersions.make(storage).listPage()).entries[0]?._tag,
          "Version",
        );
        yield* GcsRecovery.make(storage).restoreDeleted({ ref, restoreToken: "native-token" });
        assert.strictEqual(writes[1]?.url.searchParams.get("ifGenerationMatch"), "0");
        assert.strictEqual(writes[1]?.url.searchParams.get("restoreToken"), "native-token");
        const insecure = yield* Effect.result(
          GcsStorage.make({
            bucket: "test-bucket",
            apiEndpoint: fixture.endpoint,
            customerKey: Redacted.make(Buffer.alloc(32).toString("base64")),
          }),
        );
        assert.isTrue(
          Result.isFailure(insecure) && insecure.failure.reason._tag === "InvalidConfiguration",
        );
      }),
    ),
);

it.effect(
  "GCS configuration plans are nonmutating and apply native bucket metageneration conditions",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let changes = 0;
        let observed = "";
        const fixture = yield* httpFixture((incoming, response) => {
          incoming.resume();
          if (incoming.method === "PATCH") {
            changes++;
            observed = incoming.url ?? "";
          }
          response.setHeader("content-type", "application/json");
          response.end(
            JSON.stringify({
              name: "test-bucket",
              metageneration: "9007199254740993",
              versioning: { enabled: changes > 0 },
            }),
          );
        });
        const service = yield* GcsAdministration.make({
          apiEndpoint: fixture.endpoint,
          useAuthWithCustomEndpoint: false,
        });
        const plan = yield* service.configuration.plan({
          bucket: "test-bucket",
          desired: { versioning: { enabled: true } },
        });
        assert.strictEqual(changes, 0);
        yield* service.configuration.apply({ plan });
        assert.include(observed, "ifMetagenerationMatch=9007199254740993");
        assert.isTrue(
          Result.isFailure(yield* Effect.result(service.configuration.apply({ plan }))),
        );
        assert.strictEqual(changes, 1);
      }),
    ),
);

it.effect(
  "Azure metadata, tags and version deletion preserve native conditions and exact identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const requests: Array<{
          url: string;
          method: string;
          ifMatch?: string | undefined;
          lease?: string | undefined;
          metadata?: string | undefined;
        }> = [];
        const fixture = yield* httpFixture((incoming, response) => {
          incoming.resume();
          requests.push({
            url: incoming.url ?? "",
            method: incoming.method ?? "",
            ifMatch: incoming.headers["if-match"] as string | undefined,
            lease: incoming.headers["x-ms-lease-id"] as string | undefined,
            metadata: incoming.headers["x-ms-meta-added"] as string | undefined,
          });
          response.writeHead(
            incoming.method === "DELETE" ? 202 : incoming.url?.includes("comp=tags") ? 204 : 200,
            {
              etag: '"current"',
              "last-modified": "Fri, 30 Oct 2026 00:00:00 GMT",
              "x-ms-blob-type": "BlockBlob",
              "x-ms-meta-keep": "yes",
              "content-length": "0",
            },
          );
          response.end();
        });
        const storage = yield* AzureStorage.make({
          containerUrl: `${fixture.endpoint}/account/container`,
          leaseId: Redacted.make("lease-id"),
        });
        const ref = { storeId: storage.storeId, key: "file" };
        yield* AzureMetadata.make(storage).merge({
          ref,
          condition: { _tag: "IfMatch", etag: '"current"' },
          patch: { customMetadata: { added: "new" } },
        });
        assert.strictEqual(requests[1]?.ifMatch, '"current"');
        assert.strictEqual(requests[1]?.lease, "lease-id");
        assert.strictEqual(requests[1]?.metadata, "new");
        const properties = yield* AzureProperties.make(storage).setHeaders({
          ref,
          condition: { _tag: "IfMatch", etag: '"current"' },
          headers: { contentType: "text/plain" },
        });
        assert.strictEqual(properties.contentType, "text/plain");
        assert.deepStrictEqual(properties.customMetadata, { keep: "yes" });
        yield* AzureTags.make(storage).set({ ref, tags: { role: "owned" } });
        yield* AzureVersions.make(storage).deleteVersion({
          ref: { ...ref, version: { _tag: "AzureVersion", id: "2026-10-30T00:00:00.0000000Z" } },
        });
        assert.include(requests.at(-1)?.url, "versionid=2026-10-30T00%3A00%3A00.0000000Z");
        const before = requests.length;
        assert.isTrue(
          Result.isFailure(
            yield* Effect.result(
              AzureProtection.make(storage).setLegalHold({
                ref: { ...ref, version: { _tag: "AzureVersion", id: "version" } },
                enabled: false,
              }),
            ),
          ),
        );
        assert.strictEqual(requests.length, before);
      }),
    ),
);

it.effect(
  "Wasabi native MOVE reports partial per-version receipts and signs native extensions",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const requests: Array<{ url: string; method: string; signed: string; body: string }> = [];
        const fixture = yield* httpFixture((incoming, response) => {
          void drain(incoming).then((bytes) => {
            requests.push({
              url: incoming.url ?? "",
              method: incoming.method ?? "",
              signed: incoming.headers.authorization ?? "",
              body: Buffer.from(bytes).toString(),
            });
            if (incoming.method === "MOVE")
              xml(
                response,
                '<MoveObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><MoveCount>1</MoveCount><MoveObject><SourceKey>source</SourceKey><DestinationKey>destination</DestinationKey><VersionId>one</VersionId></MoveObject><MoveObject><SourceKey>source</SourceKey><DestinationKey>destination</DestinationKey><VersionId>two</VersionId><Error><Code>AccessDenied</Code></Error></MoveObject></MoveObjectResult>',
              );
            else {
              response.writeHead(200, { etag: '"native"' });
              response.end();
            }
            return undefined;
          });
        });
        const storage = yield* WasabiStorage.make({
          ...fixture.config,
          credentials: fixtureCredentials(),
        });
        const native = WasabiNative.make(storage);
        const moved = yield* native.move({
          sourceKey: "source",
          destinationKey: "destination",
          overwrite: false,
          intent: "rename-object-and-all-versions",
        });
        assert.strictEqual(moved._tag, "Partial");
        assert.deepStrictEqual(
          moved.outcomes.map((entry) => entry.status),
          ["moved", "failed"],
        );
        assert.include(requests[0]?.signed, "destination");
        yield* native.compose({
          sources: [
            {
              storeId: storage.storeId,
              key: "a&b",
              version: { _tag: "S3Version", id: "opaque-version" },
            },
          ],
          destinationKey: "composed",
          intent: "compose-native-object",
        });
        assert.include(requests[1]?.url, "compose");
        assert.include(requests[1]?.body, "a&amp;b");
        assert.include(requests[1]?.body, "opaque-version");
        assert.strictEqual(
          (yield* native.append({
            key: "append",
            body: new Uint8Array([1, 2]),
            intent: "append-without-native-cas",
          })).appendedBytes,
          2,
        );
      }),
    ),
);

it.effect("Wasabi Compliance stays separate and requires explicit irreversible lock intent", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const requests: string[] = [];
      const fixture = yield* httpFixture((incoming, response) => {
        incoming.resume();
        requests.push(incoming.url ?? "");
        if (incoming.method === "GET")
          xml(
            response,
            '<BucketComplianceConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>enabled</Status><LockTime>off</LockTime><IsLocked>false</IsLocked><RetentionDays>7</RetentionDays><ConditionalHold>false</ConditionalHold></BucketComplianceConfiguration>',
          );
        else {
          response.writeHead(200);
          response.end();
        }
      });
      const native = WasabiCompliance.make(
        yield* WasabiStorage.make({ ...fixture.config, credentials: fixtureCredentials() }),
      );
      assert.strictEqual((yield* native.inspect()).RetentionDays, "7");
      assert.match(requests[0] ?? "", /^\/storage-tests\/?\?compliance/u);
      const policy = {
        status: "enabled" as const,
        retentionDays: 7,
        conditionalHold: false,
        deleteAfterRetention: false,
        lockTime: "now",
      };
      assert.isTrue(
        Result.isFailure(
          yield* Effect.result(native.set({ policy, intent: "change-legacy-compliance" })),
        ),
      );
      assert.strictEqual(requests.length, 1);
      yield* native.set({
        policy: { ...policy, lockTime: "off" },
        intent: "change-legacy-compliance",
      });
      assert.strictEqual(requests.length, 2);
    }),
  ),
);
