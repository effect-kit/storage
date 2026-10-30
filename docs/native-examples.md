# Native provider examples

After `pnpm build`, `packages/storage/examples/native-upload.mjs` runs basic puts
and persistent upload sessions. The GCS/Azure application factories expose platform
services too, so file sources capture the runtime dependencies once. Primitive
Storage alone needs neither a checkpoint store nor a runtime hashing layer; this
example wires the full application to demonstrate restartable workflows.

Supply these common settings through your normal secure environment configuration:

| Variable                       | Meaning                                               |
| ------------------------------ | ----------------------------------------------------- |
| `STORAGE_PROVIDER`             | `gcs` or `azure`                                      |
| `STORAGE_CHECKPOINT_DIRECTORY` | Private application-owned checkpoint directory        |
| `STORAGE_CHECKPOINT_KEY`       | Stable base64 encoding of a 32-byte encryption key    |
| `STORAGE_CONFIGURATION_ID`     | Stable identity for the provider/policy configuration |

For GCS, set `GCS_BUCKET` and optionally `GCS_PROJECT_ID`; the official SDK supplies
ADC/workload credentials. Only local emulators need `GCS_ENDPOINT` with explicit
`GCS_ANONYMOUS=true`. Azure uses `AZURE_STORAGE_CONTAINER_URL`. It accepts a SAS URL
without loading identity, shared-key credentials through `AZURE_STORAGE_ACCOUNT`
and `AZURE_STORAGE_ACCESS_KEY`, or optional `/azure/identity` DefaultAzureCredential
when neither is provided. Keep credential and SAS values out of shell history.

Use an existing bucket/container and an authorized disposable object namespace:

```sh
node packages/storage/examples/native-upload.mjs put uploads/basic.bin ./input.bin
node packages/storage/examples/native-upload.mjs create UNIQUE_ID ./input.bin uploads/durable.bin
node packages/storage/examples/native-upload.mjs resume UNIQUE_ID ./input.bin
# Restart the process with the same file, configuration and checkpoint key.
node packages/storage/examples/native-upload.mjs status UNIQUE_ID
node packages/storage/examples/native-upload.mjs reconcile UNIQUE_ID
```

`reconcile` handles uncertain publication; then `resume` verifies and completes the
session. Ordinary status output excludes private protocol handles. Files and
checkpoint ownership must satisfy the [session contract](sessions.md).

`native-signing.mjs` exports `googleRequest` and `azureRequest` Effect programs,
plus the separate signing layers. Pass a ref from the matching namespace, method
and expiry. Supply Azure shared-key or Redacted user-delegation authority explicitly;
Google credentials must permit local signing or IAM signBlob. Grants remain Redacted.
Unwrap only when returning the grant to an authorized client.

`pnpm test:native:local` executes the basic/file-session examples against owned
loopback fixtures and runs the signing programs with generated/local fixture
credentials. This proves composition and serialization, not hosted signing access.
