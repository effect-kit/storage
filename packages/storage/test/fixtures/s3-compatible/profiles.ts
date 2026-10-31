import type { Effect, Scope } from "effect";

import type { StorageError } from "../../../src/core/errors.js";
import { B2Storage } from "../../../src/providers/b2/index.js";
import { HetznerStorage } from "../../../src/providers/hetzner/index.js";
import { MinioStorage } from "../../../src/providers/minio/index.js";
import { R2Storage } from "../../../src/providers/r2/index.js";
import type { S3StorageService } from "../../../src/providers/s3/index.js";
import { ScalewayStorage } from "../../../src/providers/scaleway/index.js";
import { SpacesStorage } from "../../../src/providers/spaces/index.js";
import { TigrisStorage } from "../../../src/providers/tigris/index.js";
import { WasabiStorage } from "../../../src/providers/wasabi/index.js";
import { fixtureCredentials } from "../s3/http-fixture.js";

export const settings = (endpoint?: string) => ({
  bucket: "storage-tests",
  credentials: fixtureCredentials(),
  ...(endpoint ? { endpoint, forcePathStyle: true } : {}),
});
export const profileFixtures: ReadonlyArray<{
  name: string;
  make: (endpoint?: string) => Effect.Effect<S3StorageService, StorageError, Scope.Scope>;
}> = [
  {
    name: "r2",
    make: (endpoint) => R2Storage.make({ ...settings(endpoint), accountId: "a".repeat(32) }),
  },
  {
    name: "minio",
    make: (endpoint) =>
      MinioStorage.make({
        ...settings(endpoint),
        endpoint: endpoint ?? "https://minio.example.test",
        region: "us-east-1",
        edition: "community",
        serverVersion: "deployment-owned-test-profile",
        versioning: false,
      }),
  },
  {
    name: "tigris",
    make: (endpoint) => TigrisStorage.make({ ...settings(endpoint), placement: "multi-region" }),
  },
  {
    name: "spaces",
    make: (endpoint) =>
      SpacesStorage.make({ ...settings(endpoint), region: "nyc3", tier: "standard" }),
  },
  {
    name: "b2",
    make: (endpoint) => B2Storage.make({ ...settings(endpoint), region: "us-west-004" }),
  },
  {
    name: "wasabi",
    make: (endpoint) => WasabiStorage.make({ ...settings(endpoint), region: "us-east-1" }),
  },
  {
    name: "scaleway",
    make: (endpoint) => ScalewayStorage.make({ ...settings(endpoint), region: "fr-par" }),
  },
  {
    name: "hetzner",
    make: (endpoint) =>
      HetznerStorage.make({ ...settings(endpoint), region: "fsn1", versioning: true }),
  },
];
