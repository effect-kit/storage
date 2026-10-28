import { Context } from "effect";
import type { DateTime, Effect, Redacted } from "effect";

import type { StorageError } from "./errors.js";
import type { ObjectMetadata, ObjectRef } from "./object.js";
import type { WriteCondition } from "./requests.js";

export interface StorageCapabilities {
  readonly putConditions: ReadonlyArray<"create-only" | "if-match">;
  readonly getConditions: ReadonlyArray<"if-match" | "if-none-match">;
  readonly deleteConditions: ReadonlyArray<"if-match">;
  readonly ranges: ReadonlyArray<"bounded" | "from" | "suffix">;
  readonly versionedReads: "none" | "current-only" | "history";
  readonly list: {
    readonly maxPageSize: number;
    readonly delimiter: boolean;
    readonly cursorPersistence: "none" | "process" | "provider";
    readonly snapshot: boolean;
  };
}

export interface GrantConstraint {
  readonly kind: string;
  readonly enforcement: "provider" | "backend" | "advisory";
  readonly value: string | number;
}

export interface SignedRequest {
  readonly method: "GET" | "HEAD" | "PUT" | "DELETE";
  readonly url: Redacted.Redacted<string>;
  readonly headers: Redacted.Redacted<Readonly<Record<string, string>>>;
  readonly expiresAt: DateTime.Utc;
  readonly ref: ObjectRef;
  readonly constraints: ReadonlyArray<GrantConstraint>;
}

export class StorageSigning extends Context.Service<
  StorageSigning,
  {
    readonly sign: (request: {
      readonly ref: ObjectRef;
      readonly method: SignedRequest["method"];
      readonly expiresInSeconds: number;
    }) => Effect.Effect<SignedRequest, StorageError>;
  }
>()("@effect-kit/storage/StorageSigning") {}

export class StoragePublicUrls extends Context.Service<
  StoragePublicUrls,
  {
    /** Resolving a URL does not change the object's visibility. */
    readonly url: (ref: ObjectRef) => Effect.Effect<string, StorageError>;
  }
>()("@effect-kit/storage/StoragePublicUrls") {}

export class StorageCopy extends Context.Service<
  StorageCopy,
  {
    readonly copy: (request: {
      readonly source: ObjectRef;
      readonly destinationKey: string;
      readonly condition?: WriteCondition | undefined;
    }) => Effect.Effect<ObjectMetadata, StorageError>;
  }
>()("@effect-kit/storage/StorageCopy") {}
