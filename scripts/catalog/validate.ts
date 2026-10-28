import { existsSync, realpathSync, statSync } from "node:fs";
import { isAbsolute, relative, resolve, sep } from "node:path";

import { Schema } from "effect";

import type { Evidence } from "./schema.ts";
import { FeatureCatalog, ProviderCatalog, featureIds, providerIds } from "./schema.ts";

export const decodeFeatures = Schema.decodeUnknownSync(FeatureCatalog, {
  onExcessProperty: "error",
});
export const decodeProviders = Schema.decodeUnknownSync(ProviderCatalog, {
  onExcessProperty: "error",
});

export function validateCatalogs(
  featuresInput: unknown,
  providersInput: unknown,
  root: string,
): string[] {
  const features = decodeFeatures(featuresInput);
  const providers = decodeProviders(providersInput);
  const issues: string[] = [];
  const repository = realpathSync(root);

  const checkLink = (link: string) => {
    if (link.startsWith("https://")) {
      if (URL.parse(link) === null) issues.push(`Invalid URL: ${link}`);
      return;
    }

    const path = link.split("#")[0] ?? "";
    const absolute = resolve(root, path);
    if (
      !path ||
      isAbsolute(path) ||
      path.includes("\\") ||
      !existsSync(absolute) ||
      !statSync(absolute).isFile()
    ) {
      issues.push(`Missing or unsafe reference: ${link}`);
      return;
    }

    const local = relative(repository, realpathSync(absolute));
    if (
      local === ".." ||
      local.startsWith(`..${sep}`) ||
      isAbsolute(local) ||
      ["research", ".artifacts", "node_modules"].includes(local.split(sep)[0] ?? "")
    ) {
      issues.push(`Reference must use maintained repository files: ${link}`);
    }
  };

  const inventory = (expected: readonly string[], actual: readonly string[], kind: string) => {
    for (const id of expected) {
      if (actual.filter((candidate) => candidate === id).length !== 1) {
        issues.push(`${kind} must contain exactly one ${id}`);
      }
    }
  };

  const checkEvidence = (records: readonly (typeof Evidence.Type)[]) => {
    for (const record of records) checkLink(record.source);
  };
  inventory(
    featureIds,
    features.features.map((feature) => feature.id),
    "Features",
  );
  inventory(
    providerIds,
    providers.providers.map((provider) => provider.id),
    "Providers",
  );

  for (const feature of features.features) {
    for (const link of [
      ...feature.specifications,
      ...feature.implementations,
      ...feature.tests,
      ...feature.examples,
    ])
      checkLink(link);
    checkEvidence(feature.evidence);
    if (
      ["implemented", "verified"].includes(feature.status) &&
      (!feature.implementations.length || !feature.tests.length || !feature.specifications.length)
    ) {
      issues.push(`${feature.id} lacks implementation, tests or specification`);
    }
    if (
      feature.status === "verified" &&
      !feature.evidence.some(
        (record) =>
          record.testId !== null &&
          ["local-protocol", "hosted", "runtime", "artifact"].includes(record.kind),
      )
    ) {
      issues.push(`${feature.id} lacks executed evidence`);
    }
    if ((feature.status === "explicitly-deferred") !== (feature.deferral !== null)) {
      issues.push(`${feature.id} must record an approved deferral exactly when deferred`);
    }
  }

  for (const provider of providers.providers) {
    provider.sources.forEach(checkLink);
    for (const primitive of ["put", "get", "head", "delete", "listPage"]) {
      if (!provider.operations.some((operation) => operation.operation === primitive)) {
        issues.push(`${provider.id} is missing ${primitive}`);
      }
    }
    const identities = new Set<string>();
    for (const operation of provider.operations) {
      const identity = [operation.operation, operation.configuration, operation.runtime].join("\0");
      if (identities.has(identity))
        issues.push(`${provider.id} repeats an operation/configuration/runtime`);
      identities.add(identity);
      for (const link of [...operation.implementations, ...operation.tests]) checkLink(link);
      checkEvidence(operation.evidence);
      if (
        operation.status === "implemented-not-live-verified" &&
        (!operation.implementations.length || !operation.tests.length)
      ) {
        issues.push(`${provider.id}/${operation.operation} lacks implementation/test references`);
      }
      if (operation.status === "verified") {
        const acceptable = ["filesystem", "memory"].includes(provider.id) ? "runtime" : "hosted";
        if (
          provider.implementation !== "implemented" ||
          operation.support !== "native" ||
          !operation.implementations.length ||
          !operation.tests.length ||
          !operation.evidence.some(
            (record) =>
              record.kind === acceptable &&
              record.testId !== null &&
              record.runtime === operation.runtime &&
              record.configuration === operation.configuration,
          )
        ) {
          issues.push(
            `${provider.id}/${operation.operation} lacks matching implemented/live evidence`,
          );
        }
      }
      if (
        operation.status === "unsupported-native" &&
        (operation.support !== "unavailable" ||
          !operation.evidence.some((record) =>
            ["documentation", "sdk-source", "hosted"].includes(record.kind),
          ))
      ) {
        issues.push(`${provider.id}/${operation.operation} lacks native limitation evidence`);
      }
      if ((operation.status === "explicitly-deferred") !== (operation.deferral !== null)) {
        issues.push(`${provider.id}/${operation.operation} has inconsistent deferral`);
      }
    }
  }

  return issues;
}
