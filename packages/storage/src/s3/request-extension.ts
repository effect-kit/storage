import { Predicate } from "effect";

import type {
  CopyObjectCommand,
  CreateBucketCommand,
  DeleteBucketCommand,
  ListBucketsCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  ListObjectVersionsCommand,
  PutObjectCommand,
  RestoreObjectCommand,
} from "@aws-sdk/client-s3";

/** Public per-command middleware: extension headers/queries participate in SDK signing. */
export function extendRequest(
  command:
    | GetObjectCommand
    | PutObjectCommand
    | CopyObjectCommand
    | DeleteObjectCommand
    | RestoreObjectCommand
    | CreateBucketCommand
    | DeleteBucketCommand
    | ListBucketsCommand
    | ListObjectVersionsCommand,
  options: {
    readonly method?: string;
    readonly bucketRoot?: boolean;
    readonly headers?: Readonly<Record<string, string>>;
    readonly query?: Readonly<Record<string, string>>;
    readonly observeHeaders?: (headers: Readonly<Record<string, string>>) => void;
  },
) {
  // Only the common HTTP request is changed; command inputs/outputs pass through untouched.
  const stack = command.middlewareStack as GetObjectCommand["middlewareStack"];
  stack.add(
    (next) => async (args) => {
      if (!Predicate.isObject(args.request) || !Predicate.isObject(args.request.headers))
        throw new Error("Invalid SDK request");
      if (options.method) args.request.method = options.method;
      if (options.bucketRoot && typeof args.request.path === "string")
        args.request.path = args.request.path.slice(0, args.request.path.lastIndexOf("/")) || "/";
      for (const [key, value] of Object.entries(options.headers ?? {}))
        Reflect.set(args.request.headers, key, value);
      if (options.query) {
        const previous = Predicate.isObject(args.request.query) ? args.request.query : {};
        args.request.query = { ...previous, ...options.query };
      }
      const result = await next(args);
      if (
        options.observeHeaders &&
        Predicate.isObject(result.response) &&
        Predicate.isObject(result.response.headers)
      )
        options.observeHeaders(
          Object.fromEntries(
            Object.entries(result.response.headers).filter(
              (entry): entry is [string, string] => typeof entry[1] === "string",
            ),
          ),
        );
      return result;
    },
    { name: "effectStorageNativeExtension", step: "build", priority: "low" },
  );
}
