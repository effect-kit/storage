import { Context, Effect, Layer, Predicate, Redacted, Schema } from "effect";

import { GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { XMLBuilder } from "fast-xml-parser";

import { readXml } from "./xml.js";
export { WasabiCompliance } from "./compliance.js";
import { storageError } from "../errors.js";
import { decodeResponse } from "../internal/provider-response.js";
import { decodeRequest } from "../internal/request-validation.js";
import { VersionRef } from "../lifecycle.js";
import { ObjectKey } from "../object.js";
import { S3Storage } from "../s3.js";
import type { S3StorageService } from "../s3.js";
import { lifecycleTarget } from "../s3/lifecycle-validation.js";
import { metadata } from "../s3/object-mapping.js";
import { extendRequest } from "../s3/request-extension.js";
import { request } from "../s3/transport.js";

const builder = new XMLBuilder({ ignoreAttributes: false });
const make = (storage: S3StorageService) => {
  const guard = () =>
    storage.provider !== "wasabi" || storage.config.encryption
      ? Effect.fail(
          storageError(
            "wasabiNative",
            "wasabi",
            { _tag: "Unsupported", capability: "unencrypted-wasabi-native-profile" },
            "not-started",
          ),
        )
      : Effect.void;
  return {
    move: Effect.fnUntraced(function* (input: {
      readonly sourceKey: string;
      readonly destinationKey: string;
      readonly overwrite: boolean;
      readonly intent: "rename-object-and-all-versions";
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          sourceKey: ObjectKey,
          destinationKey: ObjectKey.check(Schema.isPattern(/^[\x20-\x7e]+$/u)),
          overwrite: Schema.Boolean,
          intent: Schema.Literal("rename-object-and-all-versions"),
        }),
        input,
        "nativeMove",
        "wasabi",
        true,
      );
      yield* guard();
      return yield* Effect.scoped(
        Effect.gen(function* () {
          const command = new GetObjectCommand({
            Bucket: storage.config.bucket,
            Key: value.sourceKey,
          });
          extendRequest(command, {
            method: "MOVE",
            headers: {
              Destination: value.destinationKey,
              Overwrite: String(value.overwrite),
              "X-Wasabi-Prefix": "false",
              "X-Wasabi-Quiet": "false",
            },
          });
          const result = yield* request(storage, "nativeMove", true, (signal) =>
            storage.client.send(command, { abortSignal: signal }),
          );
          const { xml, decoded } = yield* readXml(result.Body, "nativeMove");
          const root = yield* decodeResponse(
            Schema.Struct({
              MoveObjectResult: Schema.Struct({
                MoveObject: Schema.optional(
                  Schema.Union([
                    Schema.Record(Schema.String, Schema.Unknown),
                    Schema.Array(Schema.Record(Schema.String, Schema.Unknown)),
                  ]),
                ),
                MoveCount: Schema.optional(Schema.String),
              }),
            }),
            decoded,
            "nativeMove",
            "wasabi",
          );
          const entries = root.MoveObjectResult.MoveObject;
          const outcomes = yield* Effect.forEach(
            entries ? (Array.isArray(entries) ? entries : [entries]) : [],
            (entry) =>
              decodeResponse(
                Schema.Struct({
                  SourceKey: ObjectKey,
                  DestinationKey: ObjectKey,
                  VersionId: Schema.optional(Schema.String),
                  Error: Schema.optional(Schema.Unknown),
                }),
                entry,
                "nativeMove",
                "wasabi",
              ).pipe(
                Effect.map((move) => ({
                  sourceKey: move.SourceKey,
                  destinationKey: move.DestinationKey,
                  versionId: move.VersionId,
                  status: move.Error ? ("failed" as const) : ("moved" as const),
                })),
              ),
          );
          if (
            !outcomes.length ||
            outcomes.some(
              (entry) =>
                entry.sourceKey !== value.sourceKey ||
                entry.destinationKey !== value.destinationKey,
            )
          )
            return yield* storageError(
              "nativeMove",
              "wasabi",
              { _tag: "ProviderFailure", code: "InvalidMoveReceipt" },
              "unknown",
            );
          return {
            _tag: outcomes.some((entry) => entry.status === "failed")
              ? ("Partial" as const)
              : ("Completed" as const),
            outcomes,
            nativeReceipt: Redacted.make(xml),
          };
        }),
      );
    }),
    compose: Effect.fnUntraced(function* (input: {
      readonly sources: ReadonlyArray<VersionRef>;
      readonly destinationKey: string;
      readonly intent: "compose-native-object";
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          sources: Schema.Array(VersionRef).check(Schema.isMinLength(1), Schema.isMaxLength(32)),
          destinationKey: ObjectKey,
          intent: Schema.Literal("compose-native-object"),
        }),
        input,
        "nativeCompose",
        "wasabi",
        true,
      );
      yield* guard();
      for (const source of value.sources)
        yield* lifecycleTarget(storage, source, "nativeCompose", true);
      const xml = builder.build({
        ComposeRequest: {
          Component: value.sources.map((ref) => ({ Key: ref.key, VersionId: ref.version.id })),
        },
      }) as string;
      const bytes = new TextEncoder().encode(xml);
      const command = new PutObjectCommand({
        Bucket: storage.config.bucket,
        Key: value.destinationKey,
        Body: bytes,
        ContentLength: bytes.length,
        ContentType: "text/xml",
      });
      extendRequest(command, { query: { compose: "" } });
      const output = yield* request(storage, "nativeCompose", true, (signal) =>
        storage.client.send(command, { abortSignal: signal }),
      );
      return metadata(storage.config, value.destinationKey, output);
    }),
    append: Effect.fnUntraced(function* (input: {
      readonly key: string;
      readonly body: Uint8Array;
      readonly intent: "append-without-native-cas";
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          key: ObjectKey,
          body: Schema.declare<Uint8Array>(Predicate.isUint8Array),
          intent: Schema.Literal("append-without-native-cas"),
        }),
        input,
        "nativeAppend",
        "wasabi",
        true,
      );
      yield* guard();
      if (value.body.byteLength > 16 * 1024 ** 2)
        return yield* storageError(
          "nativeAppend",
          "wasabi",
          { _tag: "LimitExceeded", limit: 16 * 1024 ** 2 },
          "not-started",
        );
      const command = new PutObjectCommand({
        Bucket: storage.config.bucket,
        Key: value.key,
        Body: value.body,
        ContentLength: value.body.length,
      });
      extendRequest(command, { query: { append: "" } });
      const output = yield* request(storage, "nativeAppend", true, (signal) =>
        storage.client.send(command, { abortSignal: signal }),
      );
      return {
        ref: metadata(storage.config, value.key, output).ref,
        etag: output.ETag,
        appendedBytes: value.body.length,
      };
    }),
  };
};
export class WasabiNative extends Context.Service<WasabiNative, ReturnType<typeof make>>()(
  "@effect-kit/storage/wasabi/WasabiNative",
) {
  static readonly make = make;
  static readonly layer = Layer.effect(WasabiNative, Effect.map(S3Storage, make));
}
