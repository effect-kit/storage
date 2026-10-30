import assert from "node:assert/strict";
import { generateKeyPairSync } from "node:crypto";

import { Effect, Redacted } from "effect";

import { StorageSharedKeyCredential } from "@azure/storage-blob";

import { azureRequest, googleRequest } from "../../../examples/native-signing.mjs";

const privateKey = generateKeyPairSync("rsa", { modulusLength: 2048 })
  .privateKey.export({ type: "pkcs8", format: "pem" })
  .toString();
const google = await Effect.runPromise(
  googleRequest(
    {
      bucket: "test-bucket",
      projectId: "example",
      storeId: "example",
      credentials: {
        client_email: "storage@example.iam.gserviceaccount.com",
        private_key: privateKey,
      },
    },
    {
      ref: {
        storeId: "example",
        key: "example",
        version: { _tag: "GcsGeneration", id: "9007199254740993" },
      },
      method: "GET",
      expiresInSeconds: 60,
    },
  ),
);
assert.equal(
  new URL(Redacted.value(google.url)).searchParams.get("generation"),
  "9007199254740993",
);
const azure = await Effect.runPromise(
  azureRequest(
    { containerUrl: "https://example.blob.core.windows.net/test", storeId: "example" },
    { ref: { storeId: "example", key: "example" }, method: "PUT", expiresInSeconds: 60 },
    {
      kind: "shared-key",
      credential: new StorageSharedKeyCredential(
        "example",
        Buffer.from("local-signing-fixture").toString("base64"),
      ),
    },
  ),
);
assert.equal(Redacted.value(azure.headers)["x-ms-blob-type"], "BlockBlob");
console.log(JSON.stringify({ signingExamples: "passed", hostedQualified: false }));
