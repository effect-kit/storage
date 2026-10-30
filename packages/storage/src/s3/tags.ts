import { Context, Effect, Layer } from "effect";

import {
  GetObjectTaggingCommand,
  PutObjectTaggingCommand,
  DeleteObjectTaggingCommand,
} from "@aws-sdk/client-s3";

import { storageError } from "../errors.js";
import { decodeRequest } from "../internal/request-validation.js";
import { SetTags, StorageTags, TagTarget } from "../lifecycle.js";
import type { TagsService } from "../lifecycle.js";
import { S3Storage } from "../s3.js";
import type { S3StorageService } from "../s3.js";
import { lifecycleFeature, lifecycleTarget } from "./lifecycle-validation.js";
import { request } from "./transport.js";

export class S3Tags extends Context.Service<S3Tags, TagsService>()(
  "@effect-kit/storage/s3/S3Tags",
) {
  static make(storage: S3StorageService): TagsService {
    return {
      get: Effect.fnUntraced(function* (input) {
        const value = yield* decodeRequest(TagTarget, input, "getTags", storage.provider);
        yield* lifecycleFeature(storage, "tags");
        const target = yield* lifecycleTarget(storage, value.ref, "getTags");
        const result = yield* request(storage, "getTags", false, (signal) =>
          storage.client.send(new GetObjectTaggingCommand(target), { abortSignal: signal }),
        );
        const tags: Record<string, string> = {};
        for (const tag of result.TagSet ?? []) {
          if (tag.Key === undefined || tag.Value === undefined || Object.hasOwn(tags, tag.Key))
            return yield* storageError("getTags", storage.provider, {
              _tag: "ProviderFailure",
              code: "InvalidTags",
            });
          Object.defineProperty(tags, tag.Key, {
            value: tag.Value,
            enumerable: true,
            configurable: true,
          });
        }
        return tags;
      }),
      set: Effect.fnUntraced(function* (input) {
        const value = yield* decodeRequest(SetTags, input, "setTags", storage.provider, true);
        yield* lifecycleFeature(storage, "tags");
        const entries = Object.entries(value.tags);
        if (
          entries.length > 10 ||
          entries.some(
            ([key, tagValue]) =>
              key.length > 128 || tagValue.length > 256 || key.toLowerCase().startsWith("aws:"),
          )
        )
          return yield* storageError(
            "setTags",
            storage.provider,
            { _tag: "InvalidInput", field: "tags" },
            "not-started",
          );
        const target = yield* lifecycleTarget(storage, value.ref, "setTags");
        yield* request(storage, "setTags", true, (signal) =>
          storage.client.send(
            new PutObjectTaggingCommand({
              ...target,
              Tagging: { TagSet: entries.map(([Key, Value]) => ({ Key, Value })) },
            }),
            { abortSignal: signal },
          ),
        );
      }),
      delete: Effect.fnUntraced(function* (input) {
        const value = yield* decodeRequest(TagTarget, input, "deleteTags", storage.provider, true);
        yield* lifecycleFeature(storage, "tags");
        const target = yield* lifecycleTarget(storage, value.ref, "deleteTags");
        yield* request(storage, "deleteTags", true, (signal) =>
          storage.client.send(new DeleteObjectTaggingCommand(target), { abortSignal: signal }),
        );
      }),
    };
  }
  static readonly layer = Layer.effectContext(
    Effect.map(S3Storage, (storage) => {
      const service = S3Tags.make(storage);
      return Context.make(S3Tags, service).pipe(Context.add(StorageTags, service));
    }),
  );
}
