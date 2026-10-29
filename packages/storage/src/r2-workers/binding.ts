export interface R2ReadableBody {
  readonly locked: boolean;
  cancel(reason?: unknown): Promise<void>;
  getReader(): {
    read(): Promise<{ done: boolean; value?: Uint8Array | undefined }>;
    cancel(reason?: unknown): Promise<void>;
    releaseLock(): void;
  };
}

/** Narrow structural binding types avoid importing or installing global Workers declarations. */
export interface R2HttpMetadata {
  readonly contentType?: string;
  readonly contentEncoding?: string;
  readonly contentLanguage?: string;
  readonly contentDisposition?: string;
  readonly cacheControl?: string;
}
export interface R2ObjectLike {
  readonly key: string;
  readonly size: number;
  readonly etag: string;
  readonly httpEtag: string;
  readonly uploaded: Date;
  readonly httpMetadata?: R2HttpMetadata | undefined;
  readonly customMetadata?: Record<string, string> | undefined;
  readonly checksums: {
    readonly md5?: ArrayBuffer | undefined;
    readonly sha1?: ArrayBuffer | undefined;
    readonly sha256?: ArrayBuffer | undefined;
    readonly sha384?: ArrayBuffer | undefined;
    readonly sha512?: ArrayBuffer | undefined;
  };
  readonly range?:
    | {
        readonly offset?: number | undefined;
        readonly length?: number | undefined;
        readonly suffix?: number;
      }
    | undefined;
  readonly body?: R2ReadableBody | undefined;
}
export interface R2Conditional {
  readonly etagMatches?: string;
  readonly etagDoesNotMatch?: string;
}
export interface R2PutOptions {
  readonly onlyIf?: R2Conditional;
  readonly httpMetadata?: R2HttpMetadata;
  readonly customMetadata?: Record<string, string>;
}
export interface R2Part {
  readonly partNumber: number;
  readonly etag: string;
}
export interface R2MultipartTransport {
  readonly key: string;
  readonly uploadId: string;
  uploadPart(partNumber: number, value: Uint8Array | ReadableStream<Uint8Array>): Promise<R2Part>;
  complete(parts: R2Part[]): Promise<R2ObjectLike>;
  abort(): Promise<void>;
}
export interface R2BucketTransport {
  head(key: string): Promise<R2ObjectLike | null>;
  get(
    key: string,
    options?: {
      onlyIf?: R2Conditional;
      range?:
        | { offset: number; length?: number }
        | { offset?: number; length: number }
        | { suffix: number };
    },
  ): Promise<R2ObjectLike | null>;
  put(
    key: string,
    value: Uint8Array | ReadableStream<Uint8Array>,
    options?: R2PutOptions,
  ): Promise<R2ObjectLike | null>;
  delete(key: string): Promise<void>;
  list(options?: {
    prefix?: string;
    delimiter?: string;
    limit?: number;
    cursor?: string;
    include?: ("httpMetadata" | "customMetadata")[];
  }): Promise<{
    objects: R2ObjectLike[];
    truncated: boolean;
    cursor?: string;
    delimitedPrefixes: string[];
  }>;
  createMultipartUpload(
    key: string,
    options?: Omit<R2PutOptions, "onlyIf">,
  ): Promise<R2MultipartTransport>;
  resumeMultipartUpload(key: string, uploadId: string): R2MultipartTransport;
}

// Workers and lib.dom disagree on BYOB reader overloads. Constrain the native result
// surface here; body parameters are interpreted once at the native transport boundary.
export interface R2MultipartBinding extends Omit<R2MultipartTransport, "uploadPart"> {
  uploadPart(...args: never[]): Promise<R2Part>;
}
export interface R2BucketBinding extends Omit<
  R2BucketTransport,
  "put" | "createMultipartUpload" | "resumeMultipartUpload"
> {
  put(...args: never[]): Promise<R2ObjectLike | null>;
  createMultipartUpload(
    key: string,
    options?: Omit<R2PutOptions, "onlyIf">,
  ): Promise<R2MultipartBinding>;
  resumeMultipartUpload(key: string, uploadId: string): R2MultipartBinding;
}
