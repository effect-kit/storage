import { Context, Effect, Option, Predicate, Schema, Stream } from "effect";
import type { Scope } from "effect";

import type { StorageCapabilities } from "./capabilities.js";
import type { StorageError } from "./errors.js";
import { storageError } from "./errors.js";
import { decodeRequest, validateTarget } from "./internal/request-validation.js";
import { ObjectSize } from "./object.js";
import type { ObjectMetadata } from "./object.js";
import { DeleteRequest, GetRequest, HeadRequest, ListRequest, PutRequest } from "./requests.js";
import type { GetResult, ListEntry, ListPage } from "./requests.js";

export interface StorageService {
  readonly provider: string;
  readonly storeId: string;
  readonly capabilities: StorageCapabilities;
  readonly put: <E = never, R = never>(
    request: PutRequest<E, R>,
  ) => Effect.Effect<ObjectMetadata, StorageError | E, R>;
  readonly get: (request: GetRequest) => Effect.Effect<GetResult, StorageError, Scope.Scope>;
  readonly head: (request: HeadRequest) => Effect.Effect<ObjectMetadata, StorageError>;
  readonly delete: (request: DeleteRequest) => Effect.Effect<void, StorageError>;
  readonly listPage: (request?: ListRequest) => Effect.Effect<ListPage, StorageError>;
}

export class Storage extends Context.Service<Storage, StorageService>()(
  "@effect-kit/storage/Storage",
) {
  /** Guards request boundaries and ties returned bodies to their acquisition scope. */
  static make(service: StorageService): StorageService {
    const { provider, storeId } = service;
    const capabilities: StorageCapabilities = Object.freeze({
      putConditions: Object.freeze([...service.capabilities.putConditions]),
      getConditions: Object.freeze([...service.capabilities.getConditions]),
      deleteConditions: Object.freeze([...service.capabilities.deleteConditions]),
      ranges: Object.freeze([...service.capabilities.ranges]),
      versionedReads: service.capabilities.versionedReads,
      list: Object.freeze({ ...service.capabilities.list }),
    });

    return Storage.of({
      ...service,
      capabilities,
      put: Effect.fnUntraced(function* <E = never, R = never>(
        request: PutRequest<E, R>,
      ): Effect.fn.Return<ObjectMetadata, StorageError | E, R> {
        const input = yield* decodeRequest(PutRequest, request, "put", provider);
        if (!Predicate.isUint8Array(input.body) && !Stream.isStream(input.body)) {
          return yield* storageError(
            "put",
            provider,
            { _tag: "InvalidInput", field: "body" },
            "not-started",
          );
        }

        const condition = input.condition?._tag === "CreateOnly" ? "create-only" : "if-match";
        if (input.condition && !capabilities.putConditions.includes(condition)) {
          return yield* storageError(
            "put",
            provider,
            { _tag: "Unsupported", capability: condition },
            "not-started",
          );
        }

        return yield* service.put({ ...input, body: request.body });
      }),
      get: Effect.fnUntraced(function* (request: GetRequest) {
        const input = yield* decodeRequest(GetRequest, request, "get", provider);
        yield* validateTarget(input, storeId, provider, "get", capabilities);

        const condition = input.condition?._tag === "IfMatch" ? "if-match" : "if-none-match";
        if (input.condition && !capabilities.getConditions.includes(condition)) {
          return yield* storageError("get", provider, {
            _tag: "Unsupported",
            capability: condition,
          });
        }

        const range = input.range?._tag.toLowerCase() as "bounded" | "from" | "suffix" | undefined;
        if (range && !capabilities.ranges.includes(range)) {
          return yield* storageError("get", provider, { _tag: "Unsupported", capability: range });
        }

        const lifetime = yield* Effect.acquireRelease(
          Effect.sync(() => ({ open: true, consumed: false })),
          (state) =>
            Effect.sync(() => {
              state.open = false;
            }),
        );
        const result = yield* service.get(input);
        if (result._tag === "NotModified") return result;

        const checkOpen = Effect.suspend(() =>
          lifetime.open
            ? Effect.void
            : Effect.fail(storageError("get", provider, { _tag: "ScopeClosed" })),
        );
        return {
          ...result,
          body: Stream.unwrap(
            Effect.gen(function* () {
              yield* checkOpen;
              if (lifetime.consumed) {
                return yield* storageError("get", provider, { _tag: "BodyConsumed" });
              }

              lifetime.consumed = true;
              return Stream.mapEffect(result.body, (chunk) => checkOpen.pipe(Effect.as(chunk)));
            }),
          ),
        };
      }),
      head: Effect.fnUntraced(function* (request: HeadRequest) {
        const input = yield* decodeRequest(HeadRequest, request, "head", provider);
        yield* validateTarget(input, storeId, provider, "head", capabilities);
        return yield* service.head(input);
      }),
      delete: Effect.fnUntraced(function* (request: DeleteRequest) {
        const input = yield* decodeRequest(DeleteRequest, request, "delete", provider);
        yield* validateTarget(input, storeId, provider, "delete", capabilities);
        if (input.condition && !capabilities.deleteConditions.includes("if-match")) {
          return yield* storageError(
            "delete",
            provider,
            { _tag: "Unsupported", capability: "if-match" },
            "not-started",
          );
        }

        return yield* service.delete(input);
      }),
      listPage: Effect.fnUntraced(function* (request: ListRequest = {}) {
        const input = yield* decodeRequest(ListRequest, request, "listPage", provider);
        if (input.delimiter && !capabilities.list.delimiter) {
          return yield* storageError("listPage", provider, {
            _tag: "Unsupported",
            capability: "delimiter",
          });
        }
        if (input.limit && input.limit > capabilities.list.maxPageSize) {
          return yield* storageError("listPage", provider, {
            _tag: "InvalidInput",
            field: "limit",
          });
        }

        return yield* service.listPage(input);
      }),
    });
  }

  static readonly exists = Effect.fnUntraced(function* (request: HeadRequest) {
    const storage = yield* Storage;
    return yield* storage.head(request).pipe(
      Effect.as(true),
      Effect.catchReason("StorageError", "NotFound", () => Effect.succeed(false)),
    );
  });

  static readonly getBytes = Effect.fnUntraced(function* (
    request: GetRequest & { readonly maxBytes: number },
  ) {
    const storage = yield* Storage;
    const { maxBytes, ...target } = request;
    yield* decodeRequest(ObjectSize, maxBytes, "getBytes", storage.provider);

    return yield* Effect.scoped(
      Effect.gen(function* () {
        const response = yield* storage.get(target);
        if (response._tag === "NotModified") {
          return yield* storageError("getBytes", storage.provider, { _tag: "NotModified" });
        }
        if (response.contentLength !== undefined && response.contentLength > maxBytes) {
          return yield* storageError("getBytes", storage.provider, {
            _tag: "LimitExceeded",
            limit: maxBytes,
          });
        }

        const chunks: Uint8Array[] = [];
        let total = 0;
        yield* Stream.runForEach(response.body, (chunk) =>
          Effect.gen(function* () {
            if (chunk.byteLength > maxBytes - total) {
              return yield* storageError("getBytes", storage.provider, {
                _tag: "LimitExceeded",
                limit: maxBytes,
              });
            }
            chunks.push(Uint8Array.from(chunk));
            total += chunk.byteLength;
          }),
        );

        if (response.contentLength !== undefined && total !== response.contentLength) {
          return yield* storageError("getBytes", storage.provider, {
            _tag: "LengthMismatch",
            expected: response.contentLength,
            actual: total,
          });
        }

        const bytes = new Uint8Array(total);
        let offset = 0;
        for (const chunk of chunks) {
          bytes.set(chunk, offset);
          offset += chunk.byteLength;
        }

        return bytes;
      }),
    );
  });

  static readonly getText = Effect.fnUntraced(function* (
    request: GetRequest & { readonly maxBytes: number },
  ) {
    const bytes = yield* Storage.getBytes(request);
    const storage = yield* Storage;
    return yield* Effect.try({
      try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
      catch: () =>
        storageError("getText", storage.provider, { _tag: "InvalidInput", field: "utf8" }),
    });
  });

  static list(request: ListRequest = {}): Stream.Stream<ListEntry, StorageError, Storage> {
    return Stream.unwrap(
      Effect.map(Storage, (storage) =>
        Stream.paginate(request, (current) =>
          Effect.gen(function* () {
            const page = yield* storage.listPage(current);
            if (page.cursor !== undefined && page.cursor === current.cursor) {
              return yield* storageError("listPage", storage.provider, {
                _tag: "ProviderFailure",
                code: "RepeatedCursor",
              });
            }
            return [
              page.entries,
              page.cursor === undefined
                ? Option.none()
                : Option.some({ ...request, cursor: page.cursor }),
            ] as const;
          }),
        ),
      ),
    );
  }

  static readonly deleteMany = Effect.fnUntraced(function* (
    requests: ReadonlyArray<DeleteRequest>,
    options: { readonly concurrency?: number } = {},
  ) {
    const storage = yield* Storage;
    const concurrency = options.concurrency ?? 4;
    yield* decodeRequest(
      Schema.Int.check(Schema.isGreaterThan(0)),
      concurrency,
      "deleteMany",
      storage.provider,
    );
    return yield* Effect.forEach(
      requests,
      (request) =>
        Effect.map(Effect.result(storage.delete(request)), (result) => ({ request, result })),
      { concurrency },
    );
  });
}
