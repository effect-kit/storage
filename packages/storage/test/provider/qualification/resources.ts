/* oxlint-disable no-await-in-loop -- Disk writes and reads are intentionally streamed in order. */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdtemp, open, readdir, rename, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Deferred, Effect, Fiber, Stream } from "effect";

import { FilesystemStorage } from "@effect-kit/storage/filesystem";
import { layer as platform } from "@effect-kit/storage/node";

const mode = process.argv[2];
assert(mode === "direct" || mode === "adapter");
const root = await mkdtemp(join(tmpdir(), "storage-resource-"));
const chunk = new Uint8Array(65536).map((_, index) => index % 251);
const count = 5120;
const payloadBytes = chunk.length * count;
const sample = () => process.memoryUsage();
const descriptors = async () => {
  try {
    return (await readdir(process.platform === "linux" ? "/proc/self/fd" : "/dev/fd")).length;
  } catch {
    return null;
  }
};
globalThis.gc?.();
const initial = sample();
const initialDescriptors = await descriptors();
let peakRss = initial.rss;
let peakHeap = initial.heapUsed;
const observe = () => {
  const current = sample();
  peakRss = Math.max(peakRss, current.rss);
  peakHeap = Math.max(peakHeap, current.heapUsed);
};
const timer = setInterval(observe, 5);
const started = performance.now();
const expected = createHash("sha256");
for (let index = 0; index < count; index++) expected.update(chunk);
const checksum = expected.digest("hex");
let cancellationCases = 0;
try {
  if (mode === "direct") {
    const temporary = join(root, "pending");
    const file = await open(temporary, "wx", 0o600);
    try {
      // Match the adapter's fixed envelope allocation and durability barriers.
      await file.writeFile(new Uint8Array(65536));
      for (let index = 0; index < count; index++) {
        await file.writeFile(chunk);
        observe();
      }
      await file.sync();
    } finally {
      await file.close();
    }
    const destination = join(root, "object");
    await rename(temporary, destination);
    const directory = await open(root, "r");
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
    const actual = createHash("sha256");
    let read = 0;
    for await (const bytes of createReadStream(destination, {
      start: 65536,
      highWaterMark: chunk.length,
    })) {
      read += bytes.length;
      actual.update(bytes);
      observe();
    }
    assert.equal(read, payloadBytes);
    assert.equal(actual.digest("hex"), checksum);
  } else {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const storage = yield* FilesystemStorage.make({ root, storeId: "resource-fixture" });
          const body = Stream.fromIterable(
            Array.from({ length: count }, () => chunk),
            { chunkSize: 1 },
          );
          const receipt = yield* storage.put({ key: "large", body, contentLength: payloadBytes });
          assert.equal(receipt.size, payloadBytes);
          const result = yield* storage.get({ ref: receipt.ref });
          assert(result._tag === "Object");
          const actual = createHash("sha256");
          let read = 0;
          yield* Stream.runForEach(result.body, (bytes) =>
            Effect.sync(() => {
              read += bytes.length;
              actual.update(bytes);
              observe();
            }),
          );
          assert.equal(read, payloadBytes);
          assert.equal(actual.digest("hex"), checksum);
          yield* storage.delete({ ref: receipt.ref });

          // Each interruption occurs after the staged file opened and its source was pulled.
          yield* Effect.forEach(
            Array.from({ length: 64 }, (_, index) => index),
            (index) =>
              Effect.gen(function* () {
                const pulled = yield* Deferred.make<void>();
                const pending = Stream.concat(
                  Stream.succeed(chunk).pipe(Stream.tap(() => Deferred.succeed(pulled, undefined))),
                  Stream.never,
                );
                const fiber = yield* storage
                  .put({ key: `cancel-${index}`, body: pending })
                  .pipe(Effect.forkChild);
                yield* Deferred.await(pulled);
                yield* Fiber.interrupt(fiber);
                cancellationCases++;
              }),
            { concurrency: 4 },
          );
          yield* Effect.forEach(
            Array.from({ length: 128 }, (_, index) => index),
            (index) =>
              Effect.gen(function* () {
                const saved = yield* storage.put({
                  key: `small-${index}`,
                  body: chunk.subarray(0, 1024),
                });
                yield* storage.delete({ ref: saved.ref });
              }),
            { concurrency: 4 },
          );
        }),
      ).pipe(Effect.provide(platform)),
    );
    assert.deepEqual(await readdir(join(root, "staging")), []);
    assert.deepEqual(await readdir(join(root, "objects")), []);
  }
  const elapsedMillis = performance.now() - started;
  clearInterval(timer);
  globalThis.gc?.();
  const final = sample();
  const finalDescriptors = await descriptors();
  const rssLimitBytes = 160 * 1024 ** 2;
  assert(peakRss - initial.rss < rssLimitBytes, "RSS growth exceeded the local fixture ceiling");
  if (initialDescriptors !== null && finalDescriptors !== null)
    assert(finalDescriptors <= initialDescriptors + 4, "File descriptors leaked");
  process.stdout.write(
    JSON.stringify({
      mode,
      payloadBytes,
      chunkBytes: chunk.length,
      checksum,
      elapsedMillis,
      initialRss: initial.rss,
      peakRss,
      finalRss: final.rss,
      peakHeap,
      rssLimitBytes,
      initialDescriptors,
      finalDescriptors,
      cancellationCases,
      smallFiles: mode === "adapter" ? 128 : 0,
      cleanupPending: 0,
    }),
  );
} finally {
  clearInterval(timer);
  await rm(root, { recursive: true, force: true });
}
