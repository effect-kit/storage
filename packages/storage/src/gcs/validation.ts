import { Schema } from "effect";

import { decodeRequest } from "../internal/request-validation.js";
import { ObjectKey } from "../object.js";

export const GcsKey = ObjectKey.check(
  Schema.makeFilter(
    (key) => key !== "." && key !== ".." && new TextEncoder().encode(key).length <= 1024,
    { expected: "a GCS object name within 1024 UTF-8 bytes, excluding dot and dot-dot" },
  ),
);
export const validateKey = (key: string, operation: string, mutation = false) =>
  decodeRequest(GcsKey, key, operation, "gcs", mutation);
