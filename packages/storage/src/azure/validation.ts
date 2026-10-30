import { Schema } from "effect";

import { decodeRequest } from "../internal/request-validation.js";
import { ObjectKey } from "../object.js";

/** The SDK normalizes dot segments, so accepting them would address a different blob. */
export const AzureKey = ObjectKey.check(
  Schema.makeFilter(
    (key) =>
      Array.from(key).length <= 1024 &&
      !key.split("/").some((segment) => segment === "." || segment === ".."),
    { expected: "an Azure blob name within 1024 characters without dot segments" },
  ),
);
export const validateKey = (key: string, operation: string, mutation = false) =>
  decodeRequest(AzureKey, key, operation, "azure", mutation);
