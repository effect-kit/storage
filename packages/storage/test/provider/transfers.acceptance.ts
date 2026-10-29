import { writeFile } from "node:fs/promises";

import { assert, it } from "@effect/vitest";
import { Effect, Stream } from "effect";

import { sha256 } from "../../src/node.js";
import { makeS3TransferDriver } from "../../src/s3/transfer-driver.js";
import { UploadSource } from "../../src/transfers.js";
import { makeTransfers } from "../transfers/fixture.js";
import { environment, s3Fixture } from "./s3-fixture.js";

const MiB = 1024 * 1024;
const local = environment.mode === "local";

it.live.skipIf(!local)(
  "managed native and multipart copy verify bytes before pinned source deletion",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { storage, prefix } = yield* s3Fixture();
        const endpoint = makeS3TransferDriver(storage);
        const nativeCopy = endpoint.nativeCopy;
        if (!nativeCopy) return yield* Effect.die("S3 copy hook missing");
        const transfers = yield* makeTransfers(endpoint, {
          partBytes: 5 * MiB,
          partConcurrency: 2,
          memoryBudgetBytes: 32 * MiB,
          maxBytes: 16 * MiB,
        });
        const empty = yield* transfers.upload({
          key: `${prefix}empty`,
          source: UploadSource.stream(Stream.empty),
        });
        assert.strictEqual((yield* storage.head({ ref: empty.metadata.ref })).size, 0);
        const source = yield* transfers.upload({
          key: `${prefix}source`,
          source: UploadSource.bytes(new Uint8Array(6 * MiB + 3).fill(7)),
        });
        const copied = yield* transfers.copy({
          source: endpoint,
          ref: source.metadata.ref,
          destinationKey: `${prefix}native`,
        });
        assert.strictEqual(copied.strategy, "native-copy");
        assert.strictEqual(copied.progress.bytesVerified, 6 * MiB + 3);

        const multipartCopy = yield* makeTransfers(
          { ...endpoint, nativeCopy: { ...nativeCopy, maximumBytes: MiB } },
          {
            partBytes: 5 * MiB,
            partConcurrency: 2,
            memoryBudgetBytes: 32 * MiB,
            maxBytes: 16 * MiB,
          },
        );
        const moved = yield* multipartCopy.move({
          source: endpoint,
          ref: source.metadata.ref,
          destinationKey: `${prefix}multipart-copy`,
        });
        assert.strictEqual(moved._tag, "Moved");
        assert.strictEqual(moved.destination.strategy, "multipart-copy");
        assert.strictEqual(moved.destination.progress.bytesVerified, 6 * MiB + 3);
      }),
    ),
);

it.live.skipIf(!local)(
  "generated 320 MiB transfers measure resource use with a 32 MiB engine budget",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { storage, prefix } = yield* s3Fixture();
        const size = 320 * MiB;
        const budget = 32 * MiB;
        assert.isFunction(globalThis.gc);
        globalThis.gc?.();
        const baseline = process.memoryUsage();
        const peak = { ...baseline };
        const retainedPeak = { ...baseline };
        const baselineResources = process.getActiveResourcesInfo();
        let peakResources = baselineResources.length;
        const sample = () => {
          const current = process.memoryUsage();
          for (const key of Object.keys(peak) as Array<keyof typeof peak>)
            peak[key] = Math.max(peak[key], current[key]);
          peakResources = Math.max(peakResources, process.getActiveResourcesInfo().length);
        };
        const sampleRetained = () => {
          sample();
          globalThis.gc?.();
          const current = process.memoryUsage();
          for (const key of Object.keys(retainedPeak) as Array<keyof typeof retainedPeak>)
            retainedPeak[key] = Math.max(retainedPeak[key], current[key]);
        };
        yield* Effect.acquireRelease(
          Effect.sync(() => setInterval(sample, 20)),
          (timer) => Effect.sync(() => clearInterval(timer)),
        );
        let chunks = 0;
        const transfers = yield* makeTransfers(makeS3TransferDriver(storage), {
          partBytes: 5 * MiB,
          partConcurrency: 2,
          requestConcurrency: 3,
          memoryBudgetBytes: budget,
          maxBytes: size,
          deadlineMillis: 180000,
          observer: () => Effect.sync(sampleRetained),
        });
        const receipt = yield* transfers.upload({
          key: `${prefix}generated`,
          source: UploadSource.stream(
            Stream.fromIterable(
              (function* () {
                for (let offset = 0; offset < size; offset += 65536) {
                  chunks++;
                  yield new Uint8Array(65536).fill(37);
                }
              })(),
              { chunkSize: 1 },
            ),
          ),
        });
        assert.strictEqual(chunks, size / 65536);
        const verified = yield* transfers.download({
          ref: receipt.metadata.ref,
          target: { write: () => Effect.void },
          expectedChecksum: receipt.checksum,
        });
        assert.strictEqual(verified.progress.bytesVerified, size);
        const resources = yield* transfers.resources;
        assert.isAtMost(resources.peakReservedBytes, budget);
        assert.strictEqual(resources.reservedBytes, 0);
        assert.strictEqual(resources.activeRequests, 0);
        sample();

        const report = {
          runtime: process.version,
          platform: process.platform,
          architecture: process.arch,
          protocol: "owned-local-rustfs",
          sdk: "@aws-sdk/client-s3@3.1159.0",
          objectBytes: size,
          sourceChunkBytes: 65536,
          memoryBudgetBytes: budget,
          partBytes: 5 * MiB,
          partConcurrency: 2,
          transportAllowancePerRequestBytes: MiB,
          digestWorkspaceBytes: sha256.workspaceBytes,
          baseline,
          peak,
          retainedPeak,
          forcedGcAtProgress: typeof globalThis.gc === "function",
          final: process.memoryUsage(),
          baselineResourceCount: baselineResources.length,
          peakResourceCount: peakResources,
          finalResourceCount: process.getActiveResourcesInfo().length,
          resources,
          attempts: receipt.progress.attempts + verified.progress.attempts,
          checksum: verified.checksum,
          limitations: [
            "Process RSS includes the runtime, SDK, allocator and garbage awaiting collection; workspace reservations are not an RSS ceiling.",
            "The RustFS container is a separate process, limited to 1 GiB; this result does not qualify hosted S3 or cloud performance.",
          ],
        };
        yield* Effect.promise(() =>
          writeFile(
            "../../.artifacts/transfers-resources.json",
            `${JSON.stringify(report, null, 2)}\n`,
          ),
        );
        // Whole-object staging would itself require 320 MiB, before any SDK or runtime overhead.
        assert.isBelow(peak.arrayBuffers - baseline.arrayBuffers, size);
        assert.isAtMost(retainedPeak.arrayBuffers - baseline.arrayBuffers, budget);
      }),
    ),
  { timeout: 180000 },
);
