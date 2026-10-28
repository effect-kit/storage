import type { Effect } from "effect";

import type { StorageError } from "../errors.js";
import type { ObjectMetadata, ObjectRef } from "../object.js";

export interface ObjectVersions {
  readonly listPage: (request: {
    readonly prefix?: string;
    readonly cursor?: string;
  }) => Effect.Effect<
    {
      readonly entries: ReadonlyArray<
        | { readonly _tag: "Version"; readonly metadata: ObjectMetadata }
        | { readonly _tag: "DeleteMarker"; readonly ref: ObjectRef }
      >;
      readonly cursor?: string;
    },
    StorageError
  >;
  readonly deleteVersion: (ref: ObjectRef) => Effect.Effect<void, StorageError>;
}

export interface ObjectTags {
  readonly get: (ref: ObjectRef) => Effect.Effect<Readonly<Record<string, string>>, StorageError>;
  readonly set: (
    ref: ObjectRef,
    tags: Readonly<Record<string, string>>,
  ) => Effect.Effect<void, StorageError>;
  readonly delete: (ref: ObjectRef) => Effect.Effect<void, StorageError>;
}

/** Policy/state stay provider-specific; an implementation cannot flatten protection modes. */
export interface ObjectRetention<Policy, State> {
  readonly get: (ref: ObjectRef) => Effect.Effect<State, StorageError>;
  readonly set: (ref: ObjectRef, policy: Policy) => Effect.Effect<State, StorageError>;
  readonly setLegalHold: (ref: ObjectRef, enabled: boolean) => Effect.Effect<void, StorageError>;
}

/** Management credentials and concrete configuration belong to the provider entry point. */
export interface StorageAdministration<Configuration, State> {
  readonly head: (name: string) => Effect.Effect<State, StorageError>;
  readonly create: (
    name: string,
    configuration: Configuration,
  ) => Effect.Effect<State, StorageError>;
  readonly deleteEmpty: (name: string) => Effect.Effect<void, StorageError>;
}
