import { Effect, Layer } from "effect";

import { AzureStorage } from "../dist/azure.js";
import { AzureSigning } from "../dist/azure/signing.js";
import { GcsStorage } from "../dist/gcs.js";
import { GcsPost } from "../dist/gcs/post.js";
import { GcsSigning } from "../dist/gcs/signing.js";
import { StorageSigning } from "../dist/index.js";

export const googleSigning = (config, authority) =>
  Layer.merge(GcsSigning.layer(authority), GcsPost.layer(authority)).pipe(
    Layer.provide(GcsStorage.layer(config)),
  );
export const azureSigning = (config, authority) =>
  AzureSigning.layer(authority).pipe(Layer.provide(AzureStorage.layer(config)));

export const googleRequest = (config, request, authority) =>
  Effect.flatMap(StorageSigning, (signing) => signing.sign(request)).pipe(
    Effect.provide(googleSigning(config, authority)),
  );
export const azureRequest = (config, request, authority) =>
  Effect.flatMap(StorageSigning, (signing) => signing.sign(request)).pipe(
    Effect.provide(azureSigning(config, authority)),
  );
