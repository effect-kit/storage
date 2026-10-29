import { Context, DateTime, Effect, Layer, Predicate, Redacted, Schema } from "effect";

import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  UploadPartCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

import { StorageSigning } from "../capabilities.js";
import type { GrantConstraint, SignedRequest } from "../capabilities.js";
import { storageError } from "../errors.js";
import type { StorageError } from "../errors.js";
import { decodeRequest } from "../internal/request-validation.js";
import { ObjectHeaders, ObjectRef, ObjectSize } from "../object.js";
import { WriteCondition } from "../requests.js";
import { S3Storage } from "../s3.js";
import type { S3StorageService } from "../s3.js";
import { authority, encryptionHeaders, readEncryption } from "./config.js";
import { ChecksumOptions, PartNumber, Upload } from "./native-models.js";
import { condition, headers, validateRef } from "./object-mapping.js";
import { signingCredentials } from "./signing-credentials.js";
import { request } from "./transport.js";

const Expiry = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 604800 }));
const SignRequest = Schema.Struct({
  ref: ObjectRef,
  method: Schema.Literals(["GET", "HEAD", "PUT", "DELETE"]),
  expiresInSeconds: Expiry,
  headers: Schema.optional(ObjectHeaders),
  condition: Schema.optional(WriteCondition),
  checksum: Schema.optional(ChecksumOptions),
  contentLength: Schema.optional(ObjectSize),
});
export type S3SignRequest = typeof SignRequest.Type;

const SignPart = Schema.Struct({
  upload: Upload,
  partNumber: PartNumber,
  expiresInSeconds: Expiry,
  checksum: Schema.optional(ChecksumOptions),
  contentLength: Schema.optional(ObjectSize),
});
export type S3SignPartRequest = typeof SignPart.Type;

export interface S3SigningService {
  readonly sign: (input: S3SignRequest) => Effect.Effect<SignedRequest, StorageError>;
  readonly signPart: (input: S3SignPartRequest) => Effect.Effect<SignedRequest, StorageError>;
}

const nativeHeaderNames: Readonly<Record<string, string>> = {
  ContentType: "content-type",
  ContentEncoding: "content-encoding",
  ContentLanguage: "content-language",
  ContentDisposition: "content-disposition",
  CacheControl: "cache-control",
  ContentLength: "content-length",
  IfMatch: "if-match",
  IfNoneMatch: "if-none-match",
  ExpectedBucketOwner: "x-amz-expected-bucket-owner",
  RequestPayer: "x-amz-request-payer",
  SSECustomerAlgorithm: "x-amz-server-side-encryption-customer-algorithm",
  SSECustomerKey: "x-amz-server-side-encryption-customer-key",
  SSECustomerKeyMD5: "x-amz-server-side-encryption-customer-key-md5",
  ServerSideEncryption: "x-amz-server-side-encryption",
  SSEKMSKeyId: "x-amz-server-side-encryption-aws-kms-key-id",
  BucketKeyEnabled: "x-amz-server-side-encryption-bucket-key-enabled",
  ChecksumCRC32: "x-amz-checksum-crc32",
  ChecksumCRC32C: "x-amz-checksum-crc32c",
  ChecksumCRC64NVME: "x-amz-checksum-crc64nvme",
  ChecksumSHA1: "x-amz-checksum-sha1",
  ChecksumSHA256: "x-amz-checksum-sha256",
  ChecksumSHA512: "x-amz-checksum-sha512",
  ChecksumMD5: "x-amz-checksum-md5",
  ChecksumXXHASH64: "x-amz-checksum-xxhash64",
  ChecksumXXHASH3: "x-amz-checksum-xxhash3",
  ChecksumXXHASH128: "x-amz-checksum-xxhash128",
};

function requiredHeaders(input: object): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [native, name] of Object.entries(nativeHeaderNames)) {
    const value: unknown = Reflect.get(input, native);
    if (value !== undefined) result[name] = String(value);
  }
  const metadata: unknown = Reflect.get(input, "Metadata");
  if (metadata && typeof metadata === "object") {
    for (const [name, value] of Object.entries(metadata))
      result[`x-amz-meta-${name.toLowerCase()}`] = String(value);
  }
  return result;
}

const signed = Effect.fnUntraced(function* (
  storage: S3StorageService,
  ref: ObjectRef,
  method: SignedRequest["method"],
  expiresInSeconds: number,
  command:
    | GetObjectCommand
    | PutObjectCommand
    | HeadObjectCommand
    | DeleteObjectCommand
    | UploadPartCommand,
) {
  const credentials = yield* signingCredentials(storage, expiresInSeconds);
  const expected = requiredHeaders(command.input);
  let captured: Record<string, string> = {};
  credentials.client.middlewareStack.add(
    (next) => async (args) => {
      if (Predicate.isObject(args.request) && Predicate.isObject(args.request.headers)) {
        captured = Object.fromEntries(
          Object.entries(args.request.headers).filter(
            (entry): entry is [string, string] => typeof entry[1] === "string",
          ),
        );
      }
      return next(args);
    },
    { name: "storageSigningHeaders", step: "build", priority: "low" },
  );
  const options = {
    expiresIn: credentials.seconds,
    signingDate: DateTime.toDateUtc(credentials.now),
    signableHeaders: new Set(Object.keys(expected)),
    unhoistableHeaders: new Set(Object.keys(expected)),
  };
  const url = yield* request(storage, "sign", false, () => {
    if (command instanceof PutObjectCommand)
      return getSignedUrl(credentials.client, command, options);
    if (command instanceof GetObjectCommand)
      return getSignedUrl(credentials.client, command, options);
    if (command instanceof HeadObjectCommand)
      return getSignedUrl(credentials.client, command, options);
    if (command instanceof UploadPartCommand)
      return getSignedUrl(credentials.client, command, options);
    return getSignedUrl(credentials.client, command, options);
  });
  const names = new URL(url).searchParams.get("X-Amz-SignedHeaders")?.split(";") ?? [];
  const required: Record<string, string> = {};
  for (const name of names) {
    if (name === "host") continue;
    const value = captured[name];
    if (value === undefined)
      return yield* storageError("sign", "s3", {
        _tag: "ProviderFailure",
        code: "UnknownSignedHeader",
      });
    required[name] = value;
  }
  const constraints: GrantConstraint[] = Object.keys(required).map((name) => ({
    kind: `header:${name}`,
    value: "required",
    enforcement: "provider",
  }));
  return {
    method,
    ref,
    url: Redacted.make(url),
    headers: Redacted.make(required),
    expiresAt: credentials.expiresAt,
    constraints,
  } satisfies SignedRequest;
});

export class S3Signing extends Context.Service<S3Signing, S3SigningService>()(
  "@effect-kit/storage/s3/S3Signing",
) {
  static make(storage: S3StorageService): S3SigningService {
    return {
      sign: Effect.fnUntraced(function* (input) {
        const value = yield* decodeRequest(SignRequest, input, "sign", "s3");
        yield* validateRef(value.ref, storage.config, "sign");
        if (
          value.method !== "PUT" &&
          (value.headers || value.condition || value.checksum || value.contentLength !== undefined)
        )
          return yield* storageError("sign", "s3", {
            _tag: "Unsupported",
            capability: "write-fields-on-read-grant",
          });
        if (value.method === "PUT" && value.ref.version)
          return yield* storageError("sign", "s3", {
            _tag: "Unsupported",
            capability: "write-version",
          });
        if (value.checksum?.ChecksumAlgorithm || value.checksum?.ChecksumType)
          return yield* storageError("sign", "s3", {
            _tag: "InvalidInput",
            field: "checksum.value-required",
          });
        const base = {
          ...authority(storage.config),
          Key: value.ref.key,
          VersionId: value.ref.version?.id,
        };
        const command =
          value.method === "GET"
            ? new GetObjectCommand({ ...base, ...readEncryption(storage.config.encryption) })
            : value.method === "HEAD"
              ? new HeadObjectCommand({ ...base, ...readEncryption(storage.config.encryption) })
              : value.method === "DELETE"
                ? new DeleteObjectCommand(base)
                : new PutObjectCommand({
                    ...base,
                    ...headers(value.headers ?? {}),
                    ...encryptionHeaders(storage.config.encryption),
                    ...condition(value.condition),
                    ...value.checksum,
                    ContentLength: value.contentLength,
                  });
        return yield* signed(storage, value.ref, value.method, value.expiresInSeconds, command);
      }),
      signPart: Effect.fnUntraced(function* (input) {
        const value = yield* decodeRequest(SignPart, input, "signPart", "s3");
        yield* validateRef(value.upload.ref, storage.config, "signPart");
        if (
          value.upload.ref.version ||
          value.checksum?.ChecksumAlgorithm ||
          value.checksum?.ChecksumType
        )
          return yield* storageError("signPart", "s3", { _tag: "InvalidInput", field: "part" });
        return yield* signed(
          storage,
          value.upload.ref,
          "PUT",
          value.expiresInSeconds,
          new UploadPartCommand({
            ...authority(storage.config),
            ...encryptionHeaders(storage.config.encryption),
            ...value.checksum,
            Key: value.upload.ref.key,
            UploadId: Redacted.value(value.upload.uploadId),
            PartNumber: value.partNumber,
            ContentLength: value.contentLength,
          }),
        );
      }),
    };
  }

  static readonly layer = Layer.effectContext(
    Effect.map(S3Storage, (storage) => {
      const service = S3Signing.make(storage);
      return Context.make(S3Signing, service).pipe(Context.add(StorageSigning, service));
    }),
  );
}
