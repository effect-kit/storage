import { Schema } from "effect";

export const featureIds = [
  "OBJ-01",
  "OBJ-02",
  "OBJ-03",
  "OBJ-04",
  "OBJ-05",
  "OBJ-06",
  "OBJ-07",
  "OBJ-08",
  "OBJ-09",
  "SIG-01",
  "SIG-02",
  "SIG-03",
  "XFR-01",
  "XFR-02",
  "XFR-03",
  "XFR-04",
  "XFR-05",
  "XFR-06",
  "INT-01",
  "INT-02",
  "INT-03",
  "SEC-01",
  "SEC-02",
  "SEC-03",
  "SEC-04",
  "VER-01",
  "VER-02",
  "GOV-01",
  "GOV-02",
  "GOV-03",
  "GOV-04",
  "ADM-01",
  "ADM-02",
  "ADM-03",
  "EVT-01",
  "EVT-02",
  "CMP-01",
  "CMP-02",
  "CMP-03",
  "CLI-01",
  "WEB-01",
  "WEB-02",
  "WEB-03",
  "DX-01",
  "DX-02",
  "OBS-01",
  "REL-01",
] as const;

export const providerIds = [
  "aws-s3",
  "r2-s3",
  "r2-workers",
  "minio",
  "gcs",
  "azure",
  "filesystem",
  "memory",
  "supabase",
  "vercel",
  "spaces",
  "b2",
  "wasabi",
  "tigris",
  "scaleway",
  "hetzner",
] as const;

const Text = Schema.NonEmptyString;
const Phase = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 15 }));
const Date = Schema.String.check(Schema.isPattern(/^\d{4}-\d{2}-\d{2}$/));
const References = Schema.Array(Text);
const Status = Schema.Literals([
  "verified",
  "implemented-not-live-verified",
  "unsupported-native",
  "unsupported-adapter",
  "research-needed",
  "explicitly-deferred",
]);

export const Evidence = Schema.Struct({
  kind: Schema.Literals([
    "documentation",
    "sdk-source",
    "compile",
    "local-protocol",
    "hosted",
    "runtime",
    "artifact",
  ]),
  date: Date,
  source: Text,
  sdkVersion: Schema.NullOr(Text),
  apiVersion: Schema.NullOr(Text),
  runtime: Text,
  configuration: Text,
  testId: Schema.NullOr(Text),
  limitations: References,
});

const Deferral = Schema.Struct({
  reason: Text,
  destination: Text,
  owner: Text,
  approval: Text,
});

export const FeatureCatalog = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  updatedAt: Date,
  features: Schema.Array(
    Schema.Struct({
      id: Schema.Literals(featureIds),
      title: Text,
      target: Text,
      completionPhase: Phase,
      status: Schema.Literals([
        "planned",
        "in-progress",
        "implemented",
        "verified",
        "explicitly-deferred",
      ]),
      specifications: References,
      implementations: References,
      tests: References,
      examples: References,
      evidence: Schema.Array(Evidence),
      deferral: Schema.NullOr(Deferral),
    }),
  ),
});

export const ProviderCatalog = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  updatedAt: Date,
  providers: Schema.Array(
    Schema.Struct({
      id: Schema.Literals(providerIds),
      name: Text,
      transport: Text,
      implementationPhase: Phase,
      implementation: Schema.Literals(["planned", "in-progress", "implemented"]),
      runtimes: References,
      sources: References,
      notes: References,
      operations: Schema.Array(
        Schema.Struct({
          featureId: Schema.Literals(featureIds),
          operation: Text,
          configuration: Text,
          runtime: Text,
          support: Schema.Literals(["unknown", "native", "unavailable"]),
          status: Status,
          constraints: References,
          implementations: References,
          tests: References,
          evidence: Schema.Array(Evidence),
          deferral: Schema.NullOr(Deferral),
        }),
      ),
    }),
  ),
});
