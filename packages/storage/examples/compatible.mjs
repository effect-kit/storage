import { Effect, Schema } from "effect";

import { Storage } from "@effect-kit/storage";

const services = {
  r2: "R2Storage",
  minio: "MinioStorage",
  tigris: "TigrisStorage",
  spaces: "SpacesStorage",
  b2: "B2Storage",
  wasabi: "WasabiStorage",
  scaleway: "ScalewayStorage",
  hetzner: "HetznerStorage",
};
const provider = Schema.decodeUnknownSync(Schema.Literals(Object.keys(services)))(
  process.env.STORAGE_COMPATIBLE_PROVIDER,
);
const settings = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
)(process.env.STORAGE_COMPATIBLE_CONFIG);
const accessKeyId = Schema.decodeUnknownSync(Schema.NonEmptyString)(
  process.env.STORAGE_COMPATIBLE_ACCESS_KEY_ID,
);
const secretAccessKey = Schema.decodeUnknownSync(Schema.NonEmptyString)(
  process.env.STORAGE_COMPATIBLE_SECRET_ACCESS_KEY,
);
const key = Schema.decodeUnknownSync(Schema.NonEmptyString)(process.env.STORAGE_COMPATIBLE_KEY);
const entry = await import(`@effect-kit/storage/${provider}`);
const live = entry[services[provider]].layer({
  ...settings,
  credentials: { accessKeyId, secretAccessKey },
});

await Effect.runPromise(
  Effect.gen(function* () {
    const storage = yield* Storage;
    const receipt = yield* storage.put({
      key,
      body: new TextEncoder().encode("Effect Storage example"),
    });
    const text = yield* Storage.getText({ key, maxBytes: 1024 });
    process.stdout.write(
      `${JSON.stringify({ provider: storage.provider, size: receipt.size, text })}\n`,
    );
  }).pipe(Effect.provide(live)),
);
