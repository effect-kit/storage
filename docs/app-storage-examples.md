# Supabase and Vercel examples

Build with `pnpm build`. `packages/storage/examples/supabase.mjs` and `vercel.mjs`
export complete application Layer factories. Supabase composes protected sessions;
Vercel provides native Storage/Transfers/signing and intentionally omits durable
multipart upload. No SDK is imported until its provider example is selected.

The runnable `node packages/storage/examples/app-upload.mjs` accepts:

- `put KEY FILE` for a managed file upload on either provider.
- Supabase `create ID FILE KEY`, `resume ID FILE`, `status ID`, `reconcile ID`.

Set `STORAGE_PROVIDER=supabase` and STORAGE_SUPABASE_URL, STORAGE_SUPABASE_BUCKET,
STORAGE_SUPABASE_TOKEN, STORAGE_STORE_ID. The URL is the configured project/storage
origin. STORAGE_SUPABASE_AUTH_MODE is user by default; service must be explicitly
selected for privileged credentials. This sample's token is static; production
Layers can supply a refresh Effect instead.

Supabase also needs STORAGE_CHECKPOINT_DIRECTORY, STORAGE_CHECKPOINT_KEY (32 bytes,
base64), and STORAGE_CONFIGURATION_ID. Reuse the same protected key, directory and
configuration identity across restarts. Exactly one owner accesses the namespace.
Changing project, bucket, actor or policy requires a new configuration identity.

For Vercel set STORAGE_PROVIDER=vercel, STORAGE_VERCEL_STORE_ID, STORAGE_VERCEL_ACCESS
(public/private), STORAGE_VERCEL_TOKEN and VERCEL_BLOB_RETRIES=0. Runtime credentials
belong in the server environment. Primitive metadata/custom-header and cache limits
are documented in [the adapter guide](vercel.md).

The compiled public consumers in `test/consumers/supabase-types.ts` and
`vercel-types.ts` demonstrate signing, upload grants and native browser/handler
construction. A Vercel handler additionally needs Effect KeyValueStore, an exclusive
receipt namespace, application authorize and onObserved effects. Its idempotency key
must participate in application publication transactions. A browser grant or provider
callback does not perform application file-content/MIME acceptance.

`pnpm test:app:local` builds/packs the library, installs isolated consumers, bundles
Supabase/Vercel browser imports, initializes both in Chromium and runs five actual
tus SIGKILL/restarts. It uses loopback fixtures and synthetic tokens, with no account
or cloud bill. Browser initialization is not browser upload/CORS acceptance.

`pnpm test:app:hosted` never creates projects, stores or buckets. It requires
STORAGE_APP_HOSTED_PROVIDER (supabase/vercel) and STORAGE_APP_TEST_PREFIX matching
`effect-storage-acceptance/NAME/`. Every run appends a unique run ID, and cleanup
removes only that run's known keys. The prefix must be authorized by application RLS.

Supabase hosted input: STORAGE_SUPABASE_URL, STORAGE_SUPABASE_BUCKET,
STORAGE_SUPABASE_USER_A_JWT, STORAGE_SUPABASE_USER_B_JWT,
STORAGE_SUPABASE_EXPIRED_JWT and STORAGE_SUPABASE_SERVICE_TOKEN. Configure two users
so A can use this prefix, B cannot, and the privileged credential can inspect it.
Vercel hosted input: STORAGE_VERCEL_STORE_ID, STORAGE_VERCEL_ACCESS,
STORAGE_VERCEL_TOKEN, VERCEL_BLOB_RETRIES=0. Run public/private stores independently.
The hosted lane covers primitives, conditions, ranges, native transfers, signed reads,
Supabase upload grants and Vercel size/type grant enforcement. OIDC and deployed
callback delivery remain additional evidence gates.

Do not paste secrets into reports. The manual local workflow uploads only its
whitelisted JSON summary; it never uploads bundles, environment files or checkpoints.
