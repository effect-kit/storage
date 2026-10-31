import { assert, it } from "@effect/vitest";
import { Effect, Redacted, Result } from "effect";
import { FetchHttpClient } from "effect/http";

import { AzureAccountAdministration } from "../../../src/providers/azure/account-admin.js";
import { B2Administration } from "../../../src/providers/b2/admin.js";
import { R2Administration } from "../../../src/providers/r2/admin.js";
import { httpFixture, drain } from "../../fixtures/s3/http-fixture.js";

it.effect(
  "R2 administration remains lazy, accepts native response fields and separates queue configuration",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const seen: Array<{ method: string; path: string; body: unknown }> = [];
        const fixture = yield* httpFixture((incoming, response) => {
          void drain(incoming).then((bytes) => {
            seen.push({
              method: incoming.method ?? "",
              path: incoming.url ?? "",
              body: bytes.length
                ? (JSON.parse(Buffer.from(bytes).toString()) as unknown)
                : undefined,
            });
            response.setHeader("content-type", "application/json");
            response.end(
              JSON.stringify({
                success: true,
                errors: [],
                messages: [],
                result: { name: "owned-bucket" },
              }),
            );
            return undefined;
          });
        });
        const service = yield* R2Administration.make({
          endpoint: fixture.endpoint,
          accountId: "a".repeat(32),
          token: Redacted.make("fixture-token"),
        });
        assert.strictEqual(seen.length, 0);
        yield* service.create({ bucket: "owned-bucket" });
        yield* service.configuration.setLocks({
          bucket: "owned-bucket",
          rules: [{ id: "rule", enabled: true, condition: { type: "Indefinite" } }],
          intent: "change-bucket-locks",
        });
        yield* service.notifications.createRules({
          bucket: "owned-bucket",
          queueId: "b".repeat(32),
          rules: [{ actions: ["PutObject"], prefix: "owned/" }],
        });
        assert.strictEqual(seen[0]?.method, "POST");
        assert.include(seen[1]?.path, "/r2/buckets/owned-bucket/locks");
        assert.include(
          seen[2]?.path,
          `/event_notifications/r2/owned-bucket/configuration/queues/${"b".repeat(32)}`,
        );
        assert.deepStrictEqual(seen[2]?.body, {
          rules: [{ actions: ["PutObject"], prefix: "owned/" }],
        });
      }),
    ).pipe(Effect.provide(FetchHttpClient.layer)),
);

it.effect("control-plane requests never forward credentials through redirects", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let targetCalls = 0;
      const target = yield* httpFixture((incoming, response) => {
        targetCalls++;
        incoming.resume();
        response.end("{}");
      });
      const source = yield* httpFixture((incoming, response) => {
        incoming.resume();
        response.writeHead(307, { location: `${target.endpoint}/credential-target` });
        response.end();
      });
      const service = yield* R2Administration.make({
        endpoint: source.endpoint,
        accountId: "a".repeat(32),
        token: Redacted.make("private-token"),
      });
      assert.isTrue(Result.isFailure(yield* Effect.result(service.list())));
      assert.strictEqual(targetCalls, 0);
    }),
  ).pipe(Effect.provide(FetchHttpClient.layer)),
);

it.effect(
  "B2 v4 authorization uses native storageApi authority and revision CAS without leaking intents",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const writes: unknown[] = [];
        let endpoint = "";
        const fixture = yield* httpFixture((incoming, response) => {
          void drain(incoming).then((bytes) => {
            response.setHeader("content-type", "application/json");
            if (incoming.url?.includes("b2_authorize_account"))
              response.end(
                JSON.stringify({
                  accountId: "account",
                  authorizationToken: "authorized-token",
                  apiInfo: {
                    storageApi: { apiUrl: endpoint, capabilities: ["writeBuckets"] },
                    groupsApi: {},
                  },
                  applicationKeyExpirationTimestamp: null,
                }),
              );
            else {
              writes.push(JSON.parse(Buffer.from(bytes).toString()) as unknown);
              response.end(JSON.stringify({ bucketId: "native-id", revision: 8 }));
            }
            return undefined;
          });
        });
        endpoint = fixture.endpoint;
        const service = yield* B2Administration.make({
          accountEndpoint: endpoint,
          applicationKeyId: "fixture-key",
          applicationKey: Redacted.make("private-key"),
        });
        assert.strictEqual(writes.length, 0);
        yield* service.updateBucket({
          bucketId: "native-id",
          expectedRevision: 7,
          configuration: { bucketType: "allPrivate" },
          intent: "change-storage-protection",
        });
        assert.deepStrictEqual(writes, [
          {
            accountId: "account",
            bucketId: "native-id",
            ifRevisionIs: 7,
            bucketType: "allPrivate",
          },
        ]);
      }),
    ).pipe(Effect.provide(FetchHttpClient.layer)),
);

it.effect("Azure ARM acquisition does not acquire tokens or mutate configuration", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let tokens = 0;
      const requests: string[] = [];
      const fixture = yield* httpFixture((incoming, response) => {
        requests.push(incoming.url ?? "");
        incoming.resume();
        response.setHeader("content-type", "application/json");
        response.end(JSON.stringify({ properties: { provisioningState: "Succeeded" } }));
      });
      const service = yield* AzureAccountAdministration.make({
        endpoint: fixture.endpoint,
        subscriptionId: "subscription",
        resourceGroup: "group",
        account: "ownedaccount",
        apiVersion: "2025-06-01",
        accessToken: Effect.sync(() => {
          tokens++;
          return Redacted.make("fixture-arm-token");
        }),
      });
      assert.strictEqual(tokens, 0);
      assert.strictEqual((yield* service.getLifecycle())._tag, "Observed");
      assert.strictEqual(tokens, 1);
      assert.include(requests[0], "/managementPolicies/default?api-version=2025-06-01");
    }),
  ).pipe(Effect.provide(FetchHttpClient.layer)),
);
