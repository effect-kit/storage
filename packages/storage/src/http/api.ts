import { Record, Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/http-api";

import {
  GrantResponse,
  PartRequest,
  PartsRequest,
  UploadId,
  UploadRequest,
  UploadStatus,
} from "../uploads/schema.js";
export const uploadErrorStatuses = {
  BadRequest: 400,
  Unauthorized: 401,
  Forbidden: 403,
  NotFound: 404,
  Conflict: 409,
  Expired: 410,
  Unsupported: 422,
  VerificationFailed: 422,
  Unavailable: 503,
} as const;
const errors = Record.toEntries(uploadErrorStatuses).map(([code, status]) =>
  Schema.Struct({ _tag: Schema.Literal("UploadError"), code: Schema.Literal(code) }).pipe(
    HttpApiSchema.status(status),
  ),
);
export const makeStorageUploadApi = (prefix: `/${string}` = "/storage") =>
  HttpApi.make("storage-uploads-v1").add(
    HttpApiGroup.make("uploads")
      .add(
        HttpApiEndpoint.post("create", "/uploads", {
          payload: UploadRequest,
          success: UploadStatus,
          error: errors,
        }),
        HttpApiEndpoint.get("status", "/uploads/:id", {
          params: { id: UploadId },
          success: UploadStatus,
          error: errors,
        }),
        HttpApiEndpoint.post("grant", "/uploads/:id/grants", {
          params: { id: UploadId },
          success: GrantResponse,
          error: errors,
        }),
        HttpApiEndpoint.post("part", "/uploads/:id/parts/sign", {
          params: { id: UploadId },
          payload: PartRequest,
          success: GrantResponse,
          error: errors,
        }),
        HttpApiEndpoint.post("complete", "/uploads/:id/complete", {
          params: { id: UploadId },
          payload: Schema.Struct({ parts: Schema.optional(PartsRequest.fields.parts) }),
          success: UploadStatus,
          error: errors,
        }),
        HttpApiEndpoint.post("reconcile", "/uploads/:id/reconcile", {
          params: { id: UploadId },
          success: UploadStatus,
          error: errors,
        }),
        HttpApiEndpoint.delete("abort", "/uploads/:id", {
          params: { id: UploadId },
          success: UploadStatus,
          error: errors,
        }),
      )
      .prefix(prefix),
  );
