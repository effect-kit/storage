import { assert, it } from "@effect/vitest";
import { Effect } from "effect";

import { GcsStorage } from "../../../src/providers/gcs/index.js";
import { httpFixture } from "../../fixtures/gcs/http-fixture.js";

it.effect(
  "GCS generation and metageneration conditions preserve strings through competing writes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture;
        const storage = yield* GcsStorage.make({
          bucket: "test-bucket",
          apiEndpoint: fixture.endpoint,
          useAuthWithCustomEndpoint: false,
        });
        const initial = yield* storage.put({ key: "cas", body: new Uint8Array([1]) });
        const generation = initial.ref.version?.id;
        if (!generation) return assert.fail("Missing generation");
        const results = yield* Effect.all(
          [2, 3].map((byte) =>
            storage
              .putObject(
                { key: "cas", body: new Uint8Array([byte]) },
                { ifGenerationMatch: generation },
              )
              .pipe(Effect.result),
          ),
          { concurrency: 2 },
        );
        assert.strictEqual(results.filter((result) => result._tag === "Success").length, 1);
        assert.strictEqual(
          results.filter(
            (result) =>
              result._tag === "Failure" && result.failure.reason._tag === "PreconditionFailed",
          ).length,
          1,
        );
        const current = yield* storage.head({ key: "cas" });
        const notDeleted = yield* storage
          .deleteGeneration({
            ref: current.ref,
            preconditions: { ifGenerationNotMatch: current.ref.version?.id ?? "invalid" },
          })
          .pipe(Effect.result);
        assert.strictEqual(
          notDeleted._tag === "Failure" && notDeleted.failure.reason._tag,
          "PreconditionFailed",
        );
        assert.strictEqual(
          (yield* storage.head({ key: "cas" })).ref.version?.id,
          current.ref.version?.id,
        );
        const updated = yield* storage.updateMetadata({
          ref: current.ref,
          headers: { contentType: "text/plain" },
          preconditions: { ifMetagenerationMatch: "1" },
        });
        assert.strictEqual(
          updated.ref.version?._tag === "GcsGeneration" && updated.ref.version.metageneration,
          "2",
        );
        const rejected = yield* storage
          .updateMetadata({
            ref: current.ref,
            headers: {},
            preconditions: { ifMetagenerationMatch: "1" },
          })
          .pipe(Effect.result);
        assert.strictEqual(
          rejected._tag === "Failure" && rejected.failure.reason._tag,
          "PreconditionFailed",
        );
      }),
    ),
);

it.effect(
  "GCS rewrite continuations and compose return only confirmed destination identities",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture;
        const storage = yield* GcsStorage.make({
          bucket: "test-bucket",
          apiEndpoint: fixture.endpoint,
          useAuthWithCustomEndpoint: false,
        });
        const source = yield* storage.put({ key: "source", body: new Uint8Array([1, 2]) });
        const initial = yield* storage.rewrite({ source: source.ref, destinationKey: "copy" });
        assert.strictEqual(initial._tag, "Pending");
        if (initial._tag !== "Pending" || !initial.token)
          return assert.fail("Missing continuation");
        const complete = yield* storage.rewrite({
          source: source.ref,
          destinationKey: "copy",
          token: initial.token,
        });
        assert.strictEqual(complete._tag, "Completed");
        if (complete._tag !== "Completed") return assert.fail("Expected completed rewrite");
        assert.strictEqual(complete.metadata.ref.key, "copy");
        const composite = yield* storage.compose({
          sources: [source.ref, complete.metadata.ref],
          destinationKey: "composed",
        });
        assert.strictEqual(composite.size, 4);
        yield* storage.deleteGeneration({
          ref: composite.ref,
          preconditions: { ifGenerationMatch: composite.ref.version?.id ?? "invalid" },
        });
      }),
    ),
);
