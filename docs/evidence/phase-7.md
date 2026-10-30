# Phase 7 evidence — native GCS and Azure

Date: 2026-10-30. Runtime: Node 24.22.0. Effect/platform: 4.1.0.
SDKs: `@google-cloud/storage` 8.4.0, `@azure/storage-blob` 12.34.0,
optional `@azure/identity` 4.14.0. Neither cloud provider has hosted qualification yet.

The implementation includes portable primitives, native generations/metadata,
GCS offset sessions, Azure blocks/leases/async copy, separate signing/public URLs,
and shared managed/durable protocols. Managed native uploads and durable completion
verify exact published bytes by pinned SHA-256 readback. S3 behavior remains covered
by its existing unit and local protocol suites.

Local evidence consists of:

- GCS controlled HTTP exchanges through the official SDK authorization hook: unsafe-number
  generation preservation, SDK native-Headers authorization forwarding, exact
  primitive request accounting, namespace alias rejection, same-response body metadata, partial acknowledgement, lost
  final response, generation/metageneration conditions, rewrite continuation, compose,
  full-object CRC32C validation and protected durable offsets.
- SDK-generated GCS signed URLs/POST policies and Azure SAS with local fixture authority;
  no cloud signing permission or credential-refresh claim follows from those tests.
- Azure SDK controlled HTTP async-copy initiation, pending/success/failed status, superseded
  copy-ID rejection and cancellation. No accepted copy is reported as confirmed bytes.
- Owned Azurite: all 13 portable conformance cases plus native block/lease/safe-abort
  and managed/durable scenarios. No cloud account, public bucket or hosted fixture is used.
- Nine actual SIGKILL boundaries with new Node processes, the production atomic file KVS,
  AES-256-GCM protection and stable ownership: GCS handle save, write response, offset
  save, final response and verification save; Azure stage response, block save, commit
  response and verification save. Protected files are inspected for plaintext handles.
- Executed basic file puts, create/resume/status CLI examples for both native
  providers and signing Effect programs with local/generated authority. The example
  factories expose runtime file/hash dependencies and capture provider/persistence
  services in the same composition.
- Exact packed consumers install only GCS or Azure's chosen SDK peer; NodeNext/Bundler
  declarations and native imports run without AWS, the other native SDK, platform-node
  or Azure identity peers. Generic/browser graphs stay SDK-free.

Commands:

```sh
pnpm format
pnpm ci:check
pnpm test:extended
pnpm test:native:local
```

`test:native:local` owns a loopback-only disposable Azurite container, namespace and
fixture data, joins its child processes and removes its own resources. It pins image
`mcr.microsoft.com/azure-storage/azurite@sha256:830430c1da1a2d537e08f3e6764dd1f5ae00cf0346bcaf625b968ec3f0971fd5`.
SDK 12.34 uses service version 2026-10-06; this emulator requires explicit
`--skipApiVersionCheck`. That bypass is recorded in `.artifacts/native-local.json`
and is not evidence for new-version semantics. Azurite cannot qualify Azure AD,
delegation SAS, real version retention, HNS, CPK/encryption scopes or native CRC64
streaming framing. GCS's controlled fixture cannot qualify Google IAM, refresh,
cloud consistency, actual session expiry or cloud rewrite semantics.

Hosted tests exist and remain unrun. Use an existing disposable namespace with an
explicit authorized prefix. They never create cloud accounts, buckets or containers:

```sh
STORAGE_NATIVE_HOSTED_PROVIDER=gcs \
STORAGE_NATIVE_TEST_PREFIX=effect-storage-acceptance/YOUR_RUN/ \
STORAGE_NATIVE_GCS_BUCKET=YOUR_EXISTING_TEST_BUCKET \
pnpm test:native:hosted

STORAGE_NATIVE_HOSTED_PROVIDER=azure \
STORAGE_NATIVE_TEST_PREFIX=effect-storage-acceptance/YOUR_RUN/ \
STORAGE_NATIVE_AZURE_CONTAINER_URL=https://YOUR_ACCOUNT.blob.core.windows.net/YOUR_CONTAINER \
pnpm test:native:hosted
```

GCS uses the SDK's normal ADC/workload credentials. Azure uses DefaultAzureCredential.
Keep secret values in the platform's secure credential configuration. The suite tests
independent native conditions/identity, leases where applicable and verified native
uploads. Cleanup enumerates only its newly generated prefix and deletes explicitly
observed generations, versions and snapshots. Retention can reject cleanup; it is
reported as a failure. Hosted signing, real credential expiry/refresh, encryption,
soft delete, retention, account variants and long resource/soak qualification remain
separate evidence work. Running a suite does not automatically promote catalog status.

AWS and deployed R2 acceptance remain postponed at the owner's request. That existing
postponement is independent of GCS/Azure's unrun hosted checks.

The local run also passed all four runtime helper lanes (Node/Bun/Deno/Chromium)
and the existing local S3 suite (10 passes and one AWS-only skip), including the
320 MiB/32 MiB resource scenario and separate-process recovery. Those results do
not qualify the native cloud adapters on additional runtimes.
