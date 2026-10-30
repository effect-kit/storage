import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup } from "effect/http-api";

import {
  GrantResponse,
  PartRequest,
  PartsRequest,
  UploadError,
  UploadId,
  UploadRequest,
  UploadStatus,
} from "../uploads/schema.js";
export const makeStorageUploadApi = (prefix: `/${string}` = "/storage") =>
  HttpApi.make("storage-uploads-v1").add(
    HttpApiGroup.make("uploads")
      .add(
        HttpApiEndpoint.post("create", "/uploads", {
          payload: UploadRequest,
          success: UploadStatus,
          error: UploadError,
        }),
        HttpApiEndpoint.get("status", "/uploads/:id", {
          params: { id: UploadId },
          success: UploadStatus,
          error: UploadError,
        }),
        HttpApiEndpoint.post("grant", "/uploads/:id/grants", {
          params: { id: UploadId },
          success: GrantResponse,
          error: UploadError,
        }),
        HttpApiEndpoint.post("part", "/uploads/:id/parts/sign", {
          params: { id: UploadId },
          payload: PartRequest,
          success: GrantResponse,
          error: UploadError,
        }),
        HttpApiEndpoint.post("complete", "/uploads/:id/complete", {
          params: { id: UploadId },
          payload: Schema.Struct({ parts: Schema.optional(PartsRequest.fields.parts) }),
          success: UploadStatus,
          error: UploadError,
        }),
        HttpApiEndpoint.post("reconcile", "/uploads/:id/reconcile", {
          params: { id: UploadId },
          success: UploadStatus,
          error: UploadError,
        }),
        HttpApiEndpoint.delete("abort", "/uploads/:id", {
          params: { id: UploadId },
          success: UploadStatus,
          error: UploadError,
        }),
      )
      .prefix(prefix),
  );
