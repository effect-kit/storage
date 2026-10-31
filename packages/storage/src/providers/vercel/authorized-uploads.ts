import { Effect, Layer } from "effect";

import { signedUploadDriver } from "../../uploads/signed-driver.js";
import { VercelUploadGrants } from "./signing.js";
export const VercelAuthorizedUploads = Layer.unwrap(
  Effect.map(VercelUploadGrants, (grants) =>
    signedUploadDriver((record, expiresInSeconds) =>
      grants.create({
        key: record.stagingKey,
        expiresInSeconds,
        maximumSizeInBytes: record.input.file.size,
        allowedContentTypes: [record.input.file.contentType],
        allowOverwrite: false,
      }),
    ),
  ),
);
