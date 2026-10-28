import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import type { Types } from "effect";

import { decodeFeatures, decodeProviders, validateCatalogs } from "./validate.ts";

const features = () =>
  structuredClone(
    decodeFeatures(JSON.parse(readFileSync("docs/features.json", "utf8"))),
  ) as Types.DeepMutable<ReturnType<typeof decodeFeatures>>;
const providers = () =>
  structuredClone(
    decodeProviders(JSON.parse(readFileSync("docs/providers.json", "utf8"))),
  ) as Types.DeepMutable<ReturnType<typeof decodeProviders>>;
function firstFixture<A>(values: readonly A[]): A {
  const value = values[0];
  assert.ok(value !== undefined);
  return value;
}

const check = (featureInput: unknown, providerInput: unknown) =>
  validateCatalogs(featureInput, providerInput, process.cwd());

test("maintained catalogs cover the complete feature and provider inventory", () => {
  assert.deepEqual(check(features(), providers()), []);
});

test("missing and duplicate feature IDs cannot hide scope", () => {
  const catalog = features();
  catalog.features.pop();
  catalog.features.push(firstFixture(catalog.features));

  assert.ok(check(catalog, providers()).some((issue) => issue.includes("REL-01")));
  assert.ok(check(catalog, providers()).some((issue) => issue.includes("OBJ-01")));
});

test("missing providers and core operations are rejected", () => {
  const catalog = providers();
  catalog.providers.pop();
  firstFixture(catalog.providers).operations.pop();

  const issues = check(features(), catalog);
  assert.ok(issues.some((issue) => issue.includes("hetzner")));
  assert.ok(issues.some((issue) => issue.includes("listPage")));
});

test("local protocol evidence cannot certify a cloud provider", () => {
  const catalog = providers();
  const provider = firstFixture(catalog.providers);
  const operation = firstFixture(provider.operations);
  provider.implementation = "implemented";
  operation.status = "verified";
  operation.support = "native";
  operation.implementations = ["packages/storage/src/object.ts"];
  operation.tests = ["packages/storage/test/object.test.ts"];
  operation.evidence = [
    {
      kind: "local-protocol",
      date: "2026-10-28",
      source: "docs/evidence/phase-1.md",
      sdkVersion: "3.1158.0",
      apiVersion: "S3",
      runtime: operation.runtime,
      configuration: operation.configuration,
      testId: "synthetic",
      limitations: [],
    },
  ];

  assert.ok(check(features(), catalog).some((issue) => issue.includes("live evidence")));
  firstFixture(operation.evidence).kind = "hosted";
  firstFixture(operation.evidence).configuration = "different-bucket-type";
  assert.ok(check(features(), catalog).some((issue) => issue.includes("live evidence")));
});

test("unsupported-native requires sourced limitation evidence", () => {
  const catalog = providers();
  firstFixture(firstFixture(catalog.providers).operations).status = "unsupported-native";

  assert.ok(check(features(), catalog).some((issue) => issue.includes("native limitation")));
});

test("ignored research and escaping paths cannot become maintained references", () => {
  for (const source of ["research/handoff.md", "../", "/tmp"]) {
    const catalog = features();
    firstFixture(catalog.features).specifications = [source];
    assert.notDeepEqual(check(catalog, providers()), []);
  }
});

test("deferrals require an explicit destination, rationale, owner and approval", () => {
  const catalog = features();
  firstFixture(catalog.features).status = "explicitly-deferred";

  assert.ok(check(catalog, providers()).some((issue) => issue.includes("approved deferral")));
  firstFixture(catalog.features).deferral = {
    reason: "Example test only",
    destination: "phase-4",
    owner: "maintainers",
    approval: "test-fixture",
  };
  assert.deepEqual(check(catalog, providers()), []);
});

test("unknown statuses, extra fields and invalid phases fail schema decoding", () => {
  const catalog = features();
  assert.throws(() => decodeFeatures({ ...catalog, unchecked: true }));
  assert.throws(() =>
    decodeFeatures({
      ...catalog,
      features: [{ ...catalog.features[0], completionPhase: 16 }],
    }),
  );
  assert.throws(() =>
    decodeProviders({
      ...providers(),
      providers: [{ ...providers().providers[0], implementation: "almost-ready" }],
    }),
  );
});
