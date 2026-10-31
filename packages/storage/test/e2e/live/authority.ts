import assert from "node:assert/strict";

import { Context, DateTime, Effect, Layer, Redacted, Result } from "effect";

import { Storage } from "@effect-kit/storage";
import type { StorageService, StorageSigning } from "@effect-kit/storage";

import type { ProviderSettings } from "../../../../../scripts/providers/configuration.ts";
import { providerLayer } from "./adapters.ts";
import { ownedObjects } from "./fixtures.ts";
import type { SuiteReport } from "./suite.ts";

export const authoritySuite = Effect.fnUntraced(function* (
  settings: ProviderSettings,
  storage: StorageService,
  signing: StorageSigning["Service"],
  report: SuiteReport,
) {
  const objects = yield* ownedObjects(storage, settings.prefix, report);
  const metadata = yield* objects.put("authority", new Uint8Array([11, 12, 13]));
  const credentialName = settings.secrets.SECRET_ACCESS_KEY ? "SECRET_ACCESS_KEY" : "TOKEN";
  const credential = settings.secrets[credentialName];
  assert(credential, "Release authority suite requires an explicit supported credential");
  // Alter a significant signature character; the final base64url character can contain padding bits.
  const index =
    credentialName === "TOKEN" && credential.split(".").length === 3
      ? credential.lastIndexOf(".") + 1
      : credential.length - 1;
  const invalid = `${credential.slice(0, index)}${credential[index] === "a" ? "b" : "a"}${credential.slice(index + 1)}`;
  const deniedLayer = yield* Effect.promise(() =>
    providerLayer(
      {
        ...settings,
        secrets: { ...settings.secrets, [credentialName]: invalid },
      },
      "portable",
    ),
  );
  const deniedContext = yield* Layer.build(deniedLayer);
  const denied = yield* Effect.result(
    Context.get(deniedContext, Storage).head({ key: metadata.ref.key }),
  );
  assert(
    Result.isFailure(denied) &&
      (denied.failure.reason._tag === "AccessDenied" ||
        // Supabase can conceal an existing private bucket from a rejected user identity.
        (settings.provider === "supabase" && denied.failure.reason._tag === "NotFound")),
  );
  report.checks.push("invalid-credential-denied");

  const request = yield* signing.sign({ ref: metadata.ref, method: "GET", expiresInSeconds: 3 });
  const fetchGrant = () =>
    Effect.tryPromise({
      try: async () => {
        const response = await fetch(Redacted.value(request.url), {
          headers: Redacted.value(request.headers),
          redirect: "error",
          cache: "no-store",
          signal: AbortSignal.timeout(15000),
        });
        await response.body?.cancel();
        return response.status;
      },
      catch: () => new Error("Grant request failed"),
    });
  assert.equal(yield* fetchGrant(), 200);
  yield* Effect.sleep(Math.max(0, DateTime.toEpochMillis(request.expiresAt) - Date.now()) + 1500);
  const expiredStatus = yield* fetchGrant();
  // Supabase's storage API uses HTTP 400 for expired signed tokens.
  assert(
    (settings.provider === "supabase" ? [400, 401, 403] : [401, 403]).includes(expiredStatus),
    "Expired grant remained usable",
  );
  report.checks.push("expired-grant-denied");
});
