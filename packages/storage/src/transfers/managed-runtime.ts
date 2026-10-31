import { Context, Effect } from "effect";
import { SecureRandom } from "effect/crypto";

import { storageError } from "../core/errors.js";
import { Storage } from "../core/storage.js";
import { TransferDriver, TransferHash } from "./dependencies.js";
import type { TransferPolicy } from "./models.js";
import { resolvePolicy } from "./planner.js";
import { makeRuntime } from "./runtime.js";
import type { TransferRuntime } from "./runtime.js";

/** Companion provided by Transfers.layer so sessions share its exact admission state. */
export class ManagedTransferRuntime extends Context.Service<
  ManagedTransferRuntime,
  TransferRuntime
>()("@effect-kit/storage/internal/ManagedTransferRuntime") {}

export const makeManagedRuntime = Effect.fnUntraced(function* (policy: TransferPolicy = {}) {
  const storage = yield* Storage;
  const endpoint = yield* TransferDriver;
  const digest = yield* TransferHash;
  const random = yield* SecureRandom.SecureRandom;

  if (endpoint.storage !== storage)
    return yield* storageError("transfers.make", storage.provider, {
      _tag: "InvalidConfiguration",
      field: "driver.storage",
    });

  const input = { ...policy, digest };
  const resolved = yield* resolvePolicy(input, endpoint);
  return makeRuntime(endpoint, input, resolved, random);
});
