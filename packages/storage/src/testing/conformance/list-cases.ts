import { Effect, Stream } from "effect";

import { Storage } from "../../core/storage.js";
import type { StorageConformanceCase } from "../conformance.js";
import { check, expectReason } from "./assertions.js";

export const listCases: ReadonlyArray<StorageConformanceCase> = [
  {
    name: "paged-prefix-traversal",
    run: (storage) =>
      Effect.gen(function* () {
        for (const key of ["p/a", "p/b", "p/c", "unrelated"]) {
          yield* storage.put({ key, body: new Uint8Array() });
        }
        const entries = yield* Stream.runCollect(
          Storage.list({ prefix: "p/", limit: 1 }).pipe(Stream.provideService(Storage, storage)),
        );
        const keys = entries
          .filter((entry) => entry._tag === "Object")
          .map((entry) => entry.metadata.ref.key)
          .toSorted();
        yield* check(
          JSON.stringify(keys) === JSON.stringify(["p/a", "p/b", "p/c"]),
          "Listing lost/duplicated keys",
        );
      }),
  },
  {
    name: "common-prefixes",
    requires: (profile) => profile.list.delimiter,
    run: (storage) =>
      Effect.gen(function* () {
        for (const key of ["p/a/x", "p/a/y", "p/b"])
          yield* storage.put({ key, body: new Uint8Array() });
        const page = yield* storage.listPage({ prefix: "p/", delimiter: "/" });
        yield* check(
          page.entries.some((entry) => entry._tag === "CommonPrefix" && entry.prefix === "p/a/"),
          "Missing aggregated common prefix",
        );
        yield* check(
          page.entries.filter((entry) => entry._tag === "CommonPrefix").length === 1,
          "Repeated common prefix",
        );
      }),
  },
  {
    name: "cursor-query-binding",
    run: (storage) =>
      Effect.gen(function* () {
        for (const key of ["q/a", "q/b"]) yield* storage.put({ key, body: new Uint8Array() });
        const page = yield* storage.listPage({ prefix: "q/", limit: 1 });
        if (!page.cursor) return yield* check(false, "Page omitted required continuation");
        yield* expectReason(
          storage.listPage({ prefix: "other/", limit: 1, cursor: page.cursor }),
          "InvalidInput",
        );
      }),
  },
];
