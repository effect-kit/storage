import type { Effect, Scope } from "effect";

import { B2Storage } from "../../src/b2.js";
import type { StorageError } from "../../src/errors.js";
import { HetznerStorage } from "../../src/hetzner.js";
import { MinioStorage } from "../../src/minio.js";
import { R2Storage } from "../../src/r2.js";
import type { S3StorageService } from "../../src/s3.js";
import { ScalewayStorage } from "../../src/scaleway.js";
import { SpacesStorage } from "../../src/spaces.js";
import { TigrisStorage } from "../../src/tigris.js";
import { WasabiStorage } from "../../src/wasabi.js";
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
