import { assert, it } from "@effect/vitest";
import { Effect, Redacted, Result } from "effect";

import { TigrisStorage } from "../../src/tigris.js";
import { TigrisEvents } from "../../src/tigris/events.js";
import { TigrisAdministration, TigrisRecovery } from "../../src/tigris/native.js";
import { httpFixture, drain, fixtureCredentials, xml } from "../s3/http-fixture.js";

it.effect(
  "Tigris admin readback redacts settings that can contain migration and webhook credentials",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture((incoming, response) => {
          incoming.resume();
          response.setHeader("content-type", "application/json");
          response.end(
            JSON.stringify({
              name: "owned-bucket",
              cors: { rules: [] },
              acl_settings: { allow_object_acl: false },
              soft_delete: { enabled: false },
            }),
          );
        });
        const service = yield* TigrisAdministration.make({
          endpoint: fixture.endpoint,
          credentials: async () => fixtureCredentials(),
        });
        const info = yield* service.head({ bucket: "owned-bucket" });
        assert.isTrue(Redacted.isRedacted(info.settings));
      }),
    ),
);

it.effect("Tigris archive sends its native Days request and rejects AWS-only restore tiers", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const bodies: string[] = [];
      const fixture = yield* httpFixture((incoming, response) => {
        void drain(incoming).then((bytes) => {
          bodies.push(Buffer.from(bytes).toString());
          response.writeHead(202);
          return response.end();
        });
      });
      const storage = yield* TigrisStorage.make({
        bucket: "owned-bucket",
        endpoint: fixture.endpoint,
        forcePathStyle: true,
        placement: "global",
        credentials: fixtureCredentials(),
      });
      const archive = TigrisRecovery.make(storage).archive;
      const ref = { storeId: storage.storeId, key: "archived" };
      assert.isTrue(
        Result.isFailure(
          yield* Effect.result(
            archive.requestRestore({ ref, options: { days: 1, tier: "Expedited" } }),
          ),
        ),
      );
      assert.strictEqual(bodies.length, 0);
      assert.strictEqual(
        (yield* archive.requestRestore({ ref, options: { days: 1, tier: "Standard" } }))._tag,
        "Pending",
      );
      assert.include(bodies[0], "<Days>1</Days>");
      assert.notInclude(bodies[0], "GlacierJobParameters");
    }),
  ),
);

it.effect(
  "Tigris snapshots and forks use signed single-attempt commands; native configuration resolves credentials",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const requests: Array<{
          method: string;
          snapshot?: string | undefined;
          fork?: string | undefined;
          source?: string | undefined;
          body: string;
        }> = [];
        let fail = false;
        let resolved = 0;
        const fixture = yield* httpFixture((incoming, response) => {
          void drain(incoming).then((bytes) => {
            requests.push({
              method: incoming.method ?? "",
              snapshot: incoming.headers["x-tigris-snapshot"] as string | undefined,
              fork: incoming.headers["x-tigris-fork-source-bucket-snapshot"] as string | undefined,
              source: incoming.headers["x-tigris-fork-source-bucket"] as string | undefined,
              body: Buffer.from(bytes).toString(),
            });
            if (fail) return xml(response, "<Error><Code>ServiceUnavailable</Code></Error>", 503);
            if (incoming.method === "PATCH") {
              response.setHeader("content-type", "application/json");
              response.end(JSON.stringify({ status: "ok" }));
            } else {
              response.setHeader("x-tigris-snapshot-version", "snapshot-opaque");
              response.writeHead(200);
              response.end();
            }
            return undefined;
          });
        });
        const credentials = fixtureCredentials();
        const service = yield* TigrisAdministration.make({
          endpoint: fixture.endpoint,
          credentials: async () => {
            resolved++;
            return credentials;
          },
        });
        assert.strictEqual(requests.length, 0);
        assert.strictEqual(resolved, 0);
        assert.strictEqual(
          (yield* service.snapshots.create({ bucket: "owned-bucket", name: "checkpoint" }))
            .snapshotVersion,
          "snapshot-opaque",
        );
        assert.strictEqual(requests[0]?.snapshot, "true; name=checkpoint");
        yield* service.forks.create({
          bucket: "owned-fork",
          sourceBucket: "owned-bucket",
          snapshot: "snapshot-opaque",
        });
        assert.strictEqual(requests[1]?.source, "owned-bucket");
        assert.strictEqual(requests[1]?.fork, "snapshot-opaque");
        yield* service.configuration.setCors({
          bucket: "owned-bucket",
          rules: [{ allowedOrigins: ["https://app.example"], allowedMethods: ["GET"] }],
        });
        assert.strictEqual(requests[2]?.method, "PATCH");
        assert.include(requests[2]?.body, "allowedOrigin");
        fail = true;
        assert.isTrue(
          Result.isFailure(
            yield* Effect.result(service.snapshots.create({ bucket: "owned-bucket" })),
          ),
        );
        assert.strictEqual(requests.length, 4);
      }),
    ),
);

it.effect(
  "Tigris webhook events preserve native keys and use the authenticated delivery identity",
  () =>
    Effect.gen(function* () {
      const payload = {
        events: [
          {
            eventVersion: "1.0",
            eventSource: "tigris",
            eventName: "OBJECT_CREATED_PUT",
            eventTime: "2026-10-30T00:00:00Z",
            bucket: "owned-bucket",
            object: { key: "a%2Fb+c", size: 3, eTag: "opaque" },
          },
        ],
      };
      const events = yield* TigrisEvents.decode(payload, {
        bucket: "owned-bucket",
        storeId: "store",
        messageId: "authenticated-delivery",
      });
      assert.strictEqual(events[0]?.ref.key, "a%2Fb+c");
      assert.strictEqual(events[0]?.id, "authenticated-delivery:0");
      assert.isTrue(
        Result.isFailure(
          yield* Effect.result(
            TigrisEvents.decode(payload, {
              bucket: "other",
              storeId: "store",
              messageId: "delivery",
            }),
          ),
        ),
      );
    }),
);
