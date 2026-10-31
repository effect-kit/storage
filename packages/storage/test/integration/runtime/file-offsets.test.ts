import { assert, it } from "@effect/vitest";
import { Effect, FileSystem, Path } from "effect";

import * as BunStorage from "../../../src/runtime/bun.js";

it.effect("Bun handle adapter preserves sparse offsets, explicit reads and cursor state", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped();
      const file = yield* fs.open(path.join(root, "offsets"), { flag: "wx+", noFollow: true });
      yield* file.seek(10n, "start");
      yield* file.writeAll(new Uint8Array([1, 2]));
      yield* file.seek(0n, "start");
      yield* file.writeAll(new Uint8Array([3]));
      const positioned = new Uint8Array(2);
      assert.strictEqual(yield* file.read(positioned, { position: 10n }), 2);
      assert.deepEqual(positioned, new Uint8Array([1, 2]));
      const current = new Uint8Array(1);
      assert.strictEqual(yield* file.read(current), 1);
      assert.strictEqual(current[0], 0);
      yield* file.truncate(1);
      yield* file.seek(0n, "start");
      const final = new Uint8Array(2);
      assert.strictEqual(yield* file.read(final), 1);
      assert.strictEqual(final[0], 3);
    }),
  ).pipe(Effect.provide(BunStorage.layer)),
);
