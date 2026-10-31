import { Effect, Layer } from "effect";
import { AsyncResult, AtomRegistry } from "effect/reactivity";
import type { Atom } from "effect/reactivity";

import { assert, it, vi } from "vitest";

import { UploadAtoms } from "../../../src/client/atoms/index.js";
import { UploadClient } from "../../../src/client/index.js";
import { failUpload } from "../../../src/uploads/schema.js";
import type { UploadStatus } from "../../../src/uploads/schema.js";

const id = "a".repeat(64);
const status = (state: UploadStatus["state"]): UploadStatus => ({
  version: 1,
  id,
  state,
  cleanupPending: state === "aborted",
  expiresAt: 100000,
});
const fixture = () => {
  const calls = { create: 0, transfer: 0, abort: 0, released: 0, resume: 0 };
  let release: (() => void) | undefined;
  let report: Parameters<UploadClient["Service"]["create"]>[0]["onProgress"];
  const client = UploadClient.of({
    create: (input) =>
      Effect.sync(() => {
        calls.create++;
        report = input.onProgress;
        return {
          version: 1 as const,
          idempotencyKey: input.idempotencyKey ?? "key",
          id,
          input: {
            purpose: input.purpose,
            context: input.context,
            file: {
              name: input.file.name,
              size: input.file.size,
              contentType: "text/plain",
              sha256: id,
            },
          },
        };
      }),
    transfer: (input) =>
      Effect.gen(function* () {
        input.onProtocol?.("multipart", id);
        calls.transfer++;
        yield* Effect.callback<void>((resume) => {
          release = () => resume(Effect.void);
          return Effect.sync(() => {
            calls.released++;
          });
        });
        return { uploadId: id, parts: [] };
      }),
    complete: () => Effect.succeed(status("published")),
    status: () => Effect.succeed(status("uploading")),
    abort: () =>
      Effect.sync(() => {
        calls.abort++;
        return status("aborted");
      }),
    reconcile: () => Effect.succeed(status("published")),
    upload: () => Effect.fail(failUpload("Unsupported")),
    resume: (input) =>
      Effect.suspend(() => {
        calls.resume++;
        return input.file.name === "right.txt"
          ? Effect.succeed(status("published"))
          : Effect.fail(failUpload("Conflict"));
      }),
  });
  const atoms = UploadAtoms.make({
    layer: Layer.succeed(UploadClient, client),
    purpose: "test",
    context: {},
    strategy: "multipart",
  });
  const registry = AtomRegistry.make();
  const file = new File(["hello"], "right.txt", { type: "text/plain" });
  return {
    atoms,
    client,
    registry,
    file,
    calls,
    release: () => release?.(),
    report: () =>
      report?.({
        phase: "uploading",
        bytesAccepted: 3,
        totalBytes: 5,
        measurement: "provider-accepted",
      }),
  };
};

const settle = async <A, E, I>(
  registry: AtomRegistry.AtomRegistry,
  atom: Atom.AtomResultFn<I, A, E>,
  input: I,
) => {
  registry.set(atom, input);
  await vi.waitFor(() => {
    const result = registry.get(atom);
    assert(!AsyncResult.isInitial(result) && !result.waiting);
  });
  return registry.get(atom);
};

it("definitions and idle reads do not create uploads; publication is authoritative", async () => {
  const test = fixture();
  try {
    assert.equal(test.registry.get(test.atoms.state).phase, "idle");
    assert.equal(test.calls.create, 0);
    assert(AsyncResult.isSuccess(await settle(test.registry, test.atoms.start, test.file)));
    await vi.waitFor(() => assert.equal(test.calls.transfer, 1));
    assert.equal(test.registry.get(test.atoms.state).phase, "uploading");
    test.report();
    await vi.waitFor(() => assert.equal(test.registry.get(test.atoms.progress)?.bytesAccepted, 3));
    test.release();
    await vi.waitFor(() => assert.equal(test.registry.get(test.atoms.state).phase, "completed"));
    assert.equal(test.registry.get(test.atoms.state).status?.state, "published");
  } finally {
    test.registry.dispose();
  }
});

it("same-slot starts reject while independent registries keep separate work", async () => {
  const test = fixture();
  const other = AtomRegistry.make();
  try {
    await settle(test.registry, test.atoms.start, test.file);
    assert(AsyncResult.isFailure(await settle(test.registry, test.atoms.start, test.file)));
    assert.equal(test.calls.create, 1);
    await settle(other, test.atoms.start, test.file);
    await vi.waitFor(() => assert.equal(test.calls.create, 2));
    await settle(test.registry, test.atoms.cancel, undefined);
    assert.equal(test.registry.get(test.atoms.state).phase, "cancelled");
    assert.equal(other.get(test.atoms.state).phase, "uploading");
    assert.equal(test.registry.get(test.atoms.state).status?.cleanupPending, true);
  } finally {
    test.registry.dispose();
    other.dispose();
  }
});

it("pause joins local work and rejects late progress; resume validates the selected source", async () => {
  const test = fixture();
  try {
    await settle(test.registry, test.atoms.start, test.file);
    await vi.waitFor(() => assert.equal(test.calls.transfer, 1));
    await settle(test.registry, test.atoms.pause, undefined);
    assert.equal(test.calls.abort, 0);
    assert.equal(test.calls.released, 1);
    test.report();
    assert.equal(test.registry.get(test.atoms.state).phase, "paused");
    await settle(test.registry, test.atoms.resume, { file: new File(["other"], "wrong.txt") });
    await vi.waitFor(() => assert.equal(test.registry.get(test.atoms.state).phase, "uncertain"));
    await settle(test.registry, test.atoms.resume, { file: test.file });
    await vi.waitFor(() => assert.equal(test.registry.get(test.atoms.state).phase, "completed"));
  } finally {
    test.registry.dispose();
  }
});

it("unsubscribing a view keeps its upload alive; explicit disposal joins and rejects new work", async () => {
  const test = fixture();
  try {
    const unsubscribe = test.registry.subscribe(test.atoms.state, () => undefined);
    await settle(test.registry, test.atoms.start, test.file);
    await vi.waitFor(() => assert.equal(test.calls.transfer, 1));
    unsubscribe();
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(test.calls.released, 0);
    await settle(test.registry, test.atoms.dispose, undefined);
    assert.equal(test.calls.released, 1);
    assert.equal(test.registry.get(test.atoms.state).phase, "disposed");
    assert(AsyncResult.isFailure(await settle(test.registry, test.atoms.start, test.file)));
  } finally {
    test.registry.dispose();
  }
});

it("registry disposal interrupts active transfer without remote abandonment", async () => {
  const test = fixture();
  await settle(test.registry, test.atoms.start, test.file);
  await vi.waitFor(() => assert.equal(test.calls.transfer, 1));
  test.registry.dispose();
  await vi.waitFor(() => assert.equal(test.calls.released, 1));
  assert.equal(test.calls.abort, 0);
});

it("initialization failures stay visible without fabricating an upload failure or request", async () => {
  const atoms = UploadAtoms.make({
    layer: Layer.effect(UploadClient, Effect.fail("offline" as const)),
    purpose: "test",
    context: {},
  });
  const registry = AtomRegistry.make();
  try {
    registry.get(atoms.initialization);
    await vi.waitFor(() => assert(AsyncResult.isFailure(registry.get(atoms.initialization))));
    assert.equal(registry.get(atoms.state).phase, "idle");
    assert(AsyncResult.isFailure(await settle(registry, atoms.start, new File([], "empty.txt"))));
  } finally {
    registry.dispose();
  }
});

it("failed abandonment remains uncertain until authoritative reconciliation", async () => {
  const test = fixture();
  const atoms = UploadAtoms.make({
    layer: Layer.succeed(UploadClient, {
      ...test.client,
      abort: () => Effect.fail(failUpload("Unavailable")),
    }),
    purpose: "test",
    context: {},
  });
  try {
    await settle(test.registry, atoms.start, test.file);
    await vi.waitFor(() => assert.equal(test.calls.transfer, 1));
    assert(AsyncResult.isFailure(await settle(test.registry, atoms.reset, undefined)));
    assert(AsyncResult.isFailure(await settle(test.registry, atoms.cancel, undefined)));
    assert.equal(test.registry.get(atoms.state).phase, "uncertain");
    assert.equal(test.calls.released, 1);
    await settle(test.registry, atoms.reconcile, undefined);
    assert.equal(test.registry.get(atoms.state).phase, "completed");
  } finally {
    test.registry.dispose();
  }
});

it("progress bursts are coalesced and cannot roll verification back to uploading", async () => {
  const test = fixture();
  const atoms = UploadAtoms.make({
    layer: Layer.succeed(UploadClient, { ...test.client, complete: () => Effect.never }),
    purpose: "test",
    context: {},
  });
  try {
    await settle(test.registry, atoms.start, test.file);
    await vi.waitFor(() => assert.equal(test.calls.transfer, 1));
    let notifications = 0;
    const unsubscribe = test.registry.subscribe(atoms.progress, () => {
      notifications++;
    });
    for (let index = 0; index < 1000; index++) test.report();
    await vi.waitFor(() => assert.equal(test.registry.get(atoms.progress)?.bytesAccepted, 3));
    assert.isAtMost(notifications, 3);
    test.report();
    test.release();
    await vi.waitFor(() => assert.equal(test.registry.get(atoms.state).phase, "verifying"));
    await new Promise((resolve) => setTimeout(resolve, 40));
    assert.equal(test.registry.get(atoms.state).phase, "verifying");
    unsubscribe();
    await settle(test.registry, atoms.dispose, undefined);
  } finally {
    test.registry.dispose();
  }
});
