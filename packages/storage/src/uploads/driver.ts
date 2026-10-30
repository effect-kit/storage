import { Context, Effect, Layer } from "effect";

import type { StorageError } from "../errors.js";
import { failUpload } from "./schema.js";
import type { UploadGrant, UploadRecord } from "./schema.js";
export type UploadFailure = StorageError | ReturnType<typeof failUpload>;
export interface UploadGrantDriverService {
  readonly maximumGrantLifetimeMillis?: number;
  readonly confirmsTermination?: boolean;
  readonly begin: (record: UploadRecord) => Effect.Effect<unknown, UploadFailure>;
  readonly grant: (
    record: UploadRecord,
    expiresAt: number,
  ) => Effect.Effect<UploadGrant, UploadFailure>;
  readonly part?: (
    record: UploadRecord,
    partNumber: number,
    expiresAt: number,
  ) => Effect.Effect<UploadGrant, UploadFailure>;
  readonly complete?: (
    record: UploadRecord,
    parts: ReadonlyArray<{ readonly partNumber: number; readonly receipt: string }>,
  ) => Effect.Effect<void, UploadFailure>;
  readonly abort: (record: UploadRecord) => Effect.Effect<void, UploadFailure>;
}
export class UploadGrantDriver extends Context.Service<
  UploadGrantDriver,
  UploadGrantDriverService
>()("@effect-kit/storage/UploadGrantDriver") {
  static readonly proxy = Layer.succeed(UploadGrantDriver, {
    confirmsTermination: true,
    begin: (record) =>
      ["auto", "proxy"].includes(record.input.strategy ?? "auto")
        ? Effect.succeed(null)
        : Effect.fail(failUpload("Unsupported")),
    grant: (_record, expiresAt) => Effect.succeed({ _tag: "Proxy" as const, expiresAt }),
    abort: () => Effect.void,
  });
}
