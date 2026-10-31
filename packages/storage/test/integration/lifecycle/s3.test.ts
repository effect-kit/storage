import { assert, it } from "@effect/vitest";
import { DateTime, Effect, Result, Stream } from "effect";

import { R2Storage } from "../../../src/providers/r2/index.js";
import { makeAdministration } from "../../../src/providers/s3/admin.js";
import { S3Archive } from "../../../src/providers/s3/archive.js";
import { S3ObjectLock } from "../../../src/providers/s3/governance.js";
import { S3Versions, S3Metadata, S3Tags } from "../../../src/providers/s3/lifecycle.js";
import { fixtureCredentials, drain, httpFixture, xml } from "../../fixtures/s3/http-fixture.js";

const inventory =
  '<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>file</Key><VersionId>9007199254740993</VersionId><IsLatest>false</IsLatest><Size>3</Size><ETag>"old"</ETag></Version><DeleteMarker><Key>file</Key><VersionId>marker</VersionId><IsLatest>true</IsLatest></DeleteMarker></ListVersionsResult>';

it.effect("versions keep native IDs, distinguish markers and guard destructive operations", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const removed: string[] = [];
      const fixture = yield* httpFixture((incoming, response) => {
        if (incoming.method === "DELETE") {
          removed.push(incoming.url ?? "");
          response.writeHead(204);
          response.end();
        } else xml(response, inventory);
      });
      const versions = S3Versions.make(fixture.storage);
      const page = yield* versions.listPage();
      assert.strictEqual(page.entries.length, 2);
      const ref = {
        storeId: fixture.storage.storeId,
        key: "file",
        version: { _tag: "S3Version" as const, id: "9007199254740993" },
      };
      const marker = { ...ref, version: { _tag: "S3Version" as const, id: "marker" } };
      const wrong = yield* Effect.result(versions.removeDeleteMarker({ ref }));
      assert.isTrue(Result.isFailure(wrong) && wrong.failure.reason._tag === "InvalidInput");
      const wrongDelete = yield* Effect.result(versions.deleteVersion({ ref: marker }));
      assert.isTrue(
        Result.isFailure(wrongDelete) && wrongDelete.failure.reason._tag === "InvalidInput",
      );
      assert.strictEqual(removed.length, 0);
      yield* versions.deleteVersion({ ref });
      yield* versions.removeDeleteMarker({ ref: marker });
      assert.include(removed[0], "versionId=9007199254740993");
      assert.include(removed[1], "versionId=marker");
      assert.strictEqual((yield* Stream.runCollect(versions.list())).length, 2);
    }),
  ),
);

it.effect("metadata merge pins both copy source and destination and preserves protection", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let seen: Record<string, string | string[] | undefined> = {};
      const fixture = yield* httpFixture((incoming, response) => {
        if (incoming.method === "HEAD") {
          response.writeHead(200, {
            etag: '"current"',
            "content-length": "3",
            "content-type": "application/pdf",
            "cache-control": "private",
            "x-amz-meta-keep": "yes",
            "x-amz-meta-remove": "old",
            "x-amz-server-side-encryption": "aws:kms",
            "x-amz-server-side-encryption-aws-kms-key-id": "key-reference",
            "x-amz-object-lock-mode": "GOVERNANCE",
            "x-amz-object-lock-retain-until-date": "2030-01-01T00:00:00Z",
            "x-amz-object-lock-legal-hold": "ON",
          });
          response.end();
        } else {
          seen = incoming.headers;
          void drain(incoming).then(() => {
            response.setHeader("x-amz-version-id", "new-version");
            return xml(response, '<CopyObjectResult><ETag>"copy"</ETag></CopyObjectResult>');
          });
        }
      });
      const metadata = S3Metadata.make(fixture.storage);
      const updated = yield* metadata.merge({
        ref: { storeId: fixture.storage.storeId, key: "file" },
        patch: { customMetadata: { remove: null, add: "new" } },
        condition: { _tag: "IfMatch", etag: '"current"' },
      });
      assert.strictEqual(seen["if-match"], '"current"');
      assert.strictEqual(seen["x-amz-copy-source-if-match"], '"current"');
      assert.strictEqual(seen["x-amz-tagging-directive"], "COPY");
      assert.strictEqual(seen["x-amz-meta-keep"], "yes");
      assert.isUndefined(seen["x-amz-meta-remove"]);
      assert.strictEqual(seen["x-amz-meta-add"], "new");
      assert.strictEqual(seen["x-amz-object-lock-legal-hold"], "ON");
      assert.strictEqual(seen["x-amz-server-side-encryption"], "aws:kms");
      assert.strictEqual(updated.ref.version?.id, "new-version");
      assert.strictEqual(updated.contentType, "application/pdf");
    }),
  ),
);

it.effect(
  "metadata without an atomic condition and unsupported native profiles fail before requests",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let calls = 0;
        const fixture = yield* httpFixture((_incoming, response) => {
          calls++;
          response.end();
        });
        const result = yield* Effect.result(
          S3Metadata.make(fixture.storage).replace({
            ref: { storeId: fixture.storage.storeId, key: "file" },
            headers: {},
          }),
        );
        assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "Unsupported");
        const r2 = yield* R2Storage.make({
          accountId: "a".repeat(32),
          bucket: "storage-tests",
          endpoint: fixture.endpoint,
          forcePathStyle: true,
          credentials: fixtureCredentials(),
        });
        assert.isTrue(
          Result.isFailure(
            yield* Effect.result(
              S3Tags.make(r2).get({ ref: { storeId: r2.storeId, key: "file" } }),
            ),
          ),
        );
        assert.isTrue(Result.isFailure(yield* Effect.result(S3Versions.make(r2).listPage())));
        assert.strictEqual(calls, 0);
      }),
    ),
);

it.effect("tag replacement selects the exact version and rejects limits before mutation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let body = "";
      let path = "";
      const fixture = yield* httpFixture((incoming, response) => {
        path = incoming.url ?? "";
        if (incoming.method === "PUT")
          void drain(incoming).then((bytes) => {
            body = new TextDecoder().decode(bytes);
            response.writeHead(200);
            return response.end();
          });
        else
          xml(
            response,
            "<Tagging><TagSet><Tag><Key>category</Key><Value>invoice</Value></Tag></TagSet></Tagging>",
          );
      });
      const tags = S3Tags.make(fixture.storage);
      const ref = {
        storeId: fixture.storage.storeId,
        key: "file",
        version: { _tag: "S3Version" as const, id: "exact" },
      };
      yield* tags.set({ ref, tags: { category: "invoice" } });
      assert.include(path, "versionId=exact");
      assert.include(body, "<Key>category</Key>");
      assert.deepStrictEqual(yield* tags.get({ ref }), { category: "invoice" });
      const invalid = yield* Effect.result(
        tags.set({
          ref,
          tags: Object.fromEntries(
            Array.from({ length: 11 }, (_, index) => [String(index), "value"]),
          ),
        }),
      );
      assert.isTrue(Result.isFailure(invalid) && invalid.failure.reason._tag === "InvalidInput");
    }),
  ),
);

it.effect("governance bypass is explicit and never appears on an ordinary retention request", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const headers: Array<string | string[] | undefined> = [];
      const fixture = yield* httpFixture((incoming, response) => {
        headers.push(incoming.headers["x-amz-bypass-governance-retention"]);
        void drain(incoming).then(() => {
          response.writeHead(200);
          return response.end();
        });
      });
      const service = S3ObjectLock.make(fixture.storage);
      const ref = {
        storeId: fixture.storage.storeId,
        key: "file",
        version: { _tag: "S3Version" as const, id: "exact" },
      };
      const retention = {
        mode: "governance" as const,
        retainUntil: DateTime.makeUnsafe("2030-01-01T00:00:00Z"),
      };
      yield* service.setRetention({ ref, retention });
      yield* service.setRetention({
        ref,
        retention,
        bypassGovernance: { intent: "bypass-governance", reason: "owned fixture" },
      });
      assert.isUndefined(headers[0]);
      assert.strictEqual(headers[1], "true");
    }),
  ),
);

it.effect(
  "configuration planning is read-only and apply rejects forged, mutated and stale plans",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let puts = 0;
        let origin = "https://old.example.test";
        const fixture = yield* httpFixture((incoming, response) => {
          if (incoming.method === "PUT") {
            puts++;
            void drain(incoming).then(() => {
              response.writeHead(200);
              return response.end();
            });
          } else
            xml(
              response,
              `<CORSConfiguration><CORSRule><AllowedOrigin>${origin}</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>`,
            );
        });
        const admin = makeAdministration(fixture.storage);
        assert.strictEqual(puts, 0);
        const plan = yield* admin.configuration.plan({
          bucket: "storage-tests",
          desired: {
            cors: {
              CORSRules: [
                { AllowedOrigins: ["https://new.example.test"], AllowedMethods: ["GET"] },
              ],
            },
          },
        });
        assert.strictEqual(puts, 0);
        const forged = yield* Effect.result(
          admin.configuration.apply({ plan: { ...plan }, allowUnconditional: true }),
        );
        assert.isTrue(Result.isFailure(forged));
        const original = plan.changes[0];
        if (!original) return assert.fail("Expected a configuration change");
        Reflect.set(original, "after", "mutated");
        const mutated = yield* Effect.result(
          admin.configuration.apply({ plan, allowUnconditional: true }),
        );
        assert.isTrue(Result.isFailure(mutated));
        const fresh = yield* admin.configuration.plan({
          bucket: "storage-tests",
          desired: { cors: { CORSRules: [] } },
        });
        origin = "https://changed.example.test";
        const stale = yield* Effect.result(
          admin.configuration.apply({ plan: fresh, allowUnconditional: true }),
        );
        assert.isTrue(
          Result.isFailure(stale) && stale.failure.reason._tag === "PreconditionFailed",
        );
        assert.strictEqual(puts, 0);
      }),
    ),
);

it.effect("configuration apply preserves earlier mutations when a later step fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let applied = false;
      const fixture = yield* httpFixture((incoming, response) => {
        if (incoming.url?.includes("versioning")) {
          if (incoming.method === "PUT")
            void drain(incoming).then(() =>
              xml(response, "<Error><Code>AccessDenied</Code></Error>", 403),
            );
          else
            xml(
              response,
              "<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>",
            );
        } else if (incoming.method === "PUT")
          void drain(incoming).then(() => {
            applied = true;
            response.writeHead(200);
            return response.end();
          });
        else
          xml(
            response,
            `<CORSConfiguration><CORSRule><AllowedOrigin>https://${applied ? "new" : "old"}.example.test</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>`,
          );
      });
      const admin = makeAdministration(fixture.storage);
      const plan = yield* admin.configuration.plan({
        bucket: "storage-tests",
        desired: {
          cors: {
            CORSRules: [{ AllowedOrigins: ["https://new.example.test"], AllowedMethods: ["GET"] }],
          },
          versioning: { Status: "Enabled" },
        },
      });
      const result = yield* admin.configuration.apply({
        plan,
        allowUnconditional: true,
        protectionChange: { intent: "change-storage-protection", reason: "owned test" },
      });
      assert.isFalse(result.complete);
      assert.deepStrictEqual(
        result.steps.map((step) => step.status),
        ["applied", "failed"],
      );
      assert.isTrue(applied);
    }),
  ),
);

it.effect(
  "archive request acknowledgement remains pending until provider observation confirms readiness",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let restores = 0;
        const fixture = yield* httpFixture((incoming, response) => {
          if (incoming.method === "POST")
            void drain(incoming).then(() => {
              restores++;
              response.writeHead(202);
              return response.end();
            });
          else {
            response.writeHead(200, {
              "content-length": "0",
              "x-amz-storage-class": "GLACIER",
              "x-amz-restore": 'ongoing-request="true"',
            });
            response.end();
          }
        });
        const archive = S3Archive.make(fixture.storage);
        const ref = { storeId: fixture.storage.storeId, key: "file" };
        assert.strictEqual(
          (yield* archive.requestRestore({ ref, options: { days: 1, tier: "Standard" } }))._tag,
          "Pending",
        );
        assert.strictEqual((yield* archive.restoreStatus({ ref }))._tag, "Pending");
        assert.strictEqual(restores, 1);
      }),
    ),
);
