import { Context } from "effect";
import type { Effect, Scope, Stream } from "effect";

import type { StorageError } from "../../core/errors.js";
import type { ObjectMetadata } from "../../core/object.js";
import type { HeadRequest } from "../../core/requests.js";

export interface RepresentationCodec {
  readonly format: string;
  readonly encode: (
    body: Stream.Stream<Uint8Array, StorageError, Scope.Scope>,
  ) => Stream.Stream<Uint8Array, StorageError, Scope.Scope>;
  readonly decode: (
    body: Stream.Stream<Uint8Array, StorageError, Scope.Scope>,
  ) => Stream.Stream<Uint8Array, StorageError, Scope.Scope>;
}
export interface RepresentationOptions {
  readonly maxDecodedBytes: number;
  readonly maxStoredBytes: number;
  readonly maxExpansionRatio?: number;
}
export class StorageRepresentation extends Context.Service<
  StorageRepresentation,
  {
    readonly inspect: (target: HeadRequest) => Effect.Effect<
      {
        readonly format: string;
        readonly logical: ObjectMetadata;
        readonly stored: ObjectMetadata;
      },
      StorageError
    >;
  }
>()("@effect-kit/storage/StorageRepresentation") {}
