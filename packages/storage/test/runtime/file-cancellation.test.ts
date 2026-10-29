import { assert, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, FileSystem, Option, Path } from "effect";
import { TestClock } from "effect/testing";

import { fileDownloadTarget, layer as nodeLayer } from "../../src/node.js";

it.effect("file-target interruption waits for an issued write and descriptor cleanup", () =>
  Effect.gen(function* () {
    const entered = Deferred.makeUnsafe<void>();
    const release = Deferred.makeUnsafe<void>();
    let closed = false;
    let written = false;
    const file: FileSystem.File = {
      [FileSystem.FileTypeId]: FileSystem.FileTypeId,
      stat: Effect.die("write fixture has no stat"),
      sync: Effect.void,
      seek: () => Effect.succeed(0n),
      read: () => Effect.succeed(0),
      readAlloc: () => Effect.succeed(Option.none()),
      truncate: () => Effect.void,
      write: () => Effect.succeed(0),
      writeAll: () =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.andThen(
            Effect.sync(() => {
              assert.isFalse(closed);
              written = true;
            }),
          ),
        ),
    };
    const fs = FileSystem.makeNoop({
      open: () =>
        Effect.acquireRelease(Effect.succeed(file), () =>
          Effect.sync(() => {
            closed = true;
          }),
        ),
    });
    const target = yield* fileDownloadTarget({ id: "joined", path: "owned-target" }).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provide(Path.layer),
    );
    const write = yield* target.write(0, new Uint8Array(1)).pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    const interrupt = yield* Fiber.interrupt(write).pipe(Effect.forkChild);
    yield* TestClock.adjust(1);
    assert.isUndefined(interrupt.pollUnsafe());
    assert.isFalse(closed);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(interrupt);
    assert.isTrue(written);
    assert.isTrue(closed);
  }).pipe(Effect.provide(nodeLayer)),
);
