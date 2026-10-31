import { createHash, X509Certificate } from "node:crypto";

import { Schema } from "effect";

import type { Candidate } from "./plan.ts";

const Audit = Schema.Struct({
  invalid: Schema.Array(Schema.Unknown),
  missing: Schema.Array(Schema.Unknown),
  verified: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      version: Schema.String,
      attestationBundles: Schema.Array(
        Schema.Struct({
          predicateType: Schema.String,
          bundle: Schema.Struct({
            dsseEnvelope: Schema.Struct({ payload: Schema.String }),
            verificationMaterial: Schema.Unknown,
          }),
        }),
      ),
    }),
  ),
});
const Material = Schema.Union([
  Schema.Struct({ certificate: Schema.Struct({ rawBytes: Schema.String }) }),
  Schema.Struct({
    x509CertificateChain: Schema.Struct({
      certificates: Schema.NonEmptyArray(Schema.Struct({ rawBytes: Schema.String })),
    }),
  }),
]);
const Statement = Schema.Struct({
  predicateType: Schema.Literal("https://slsa.dev/provenance/v1"),
  subject: Schema.Array(
    Schema.Struct({ name: Schema.String, digest: Schema.Struct({ sha512: Schema.String }) }),
  ),
  predicate: Schema.Struct({
    buildDefinition: Schema.Struct({
      buildType: Schema.Literal(
        "https://slsa-framework.github.io/github-actions-buildtypes/workflow/v1",
      ),
      externalParameters: Schema.Struct({
        workflow: Schema.Struct({
          repository: Schema.String,
          path: Schema.String,
          ref: Schema.String,
        }),
      }),
      resolvedDependencies: Schema.Array(
        Schema.Struct({ uri: Schema.String, digest: Schema.Struct({ gitCommit: Schema.String }) }),
      ),
    }),
    runDetails: Schema.Struct({
      builder: Schema.Struct({
        id: Schema.Literal("https://github.com/actions/runner/github-hosted"),
      }),
    }),
  }),
});

/** Only accepts output from a successful npm audit signatures --include-attestations run. */
export function verifyProvenance(
  input: unknown,
  entry: Candidate["packages"][number],
  commit: string,
  branch = "main",
) {
  const audit = Schema.decodeUnknownSync(Audit)(input);
  if (audit.invalid.length || audit.missing.length) throw new Error("npm signature audit failed");
  const packages = audit.verified.filter(
    (item) => item.name === entry.name && item.version === entry.version,
  );
  if (packages.length !== 1) throw new Error("Missing exact verified package attestation");
  const attestations = packages[0]?.attestationBundles ?? [];
  if (
    !attestations.some(
      (item) =>
        item.predicateType === "https://github.com/npm/attestation/tree/main/specs/publish/v0.1",
    )
  )
    throw new Error("Registry publication attestation missing");
  const bundles = attestations.filter(
    (item) => item.predicateType === "https://slsa.dev/provenance/v1",
  );
  if (bundles.length !== 1) throw new Error("SLSA provenance missing or ambiguous");
  const bundle = bundles[0]?.bundle;
  if (!bundle || bundle.dsseEnvelope.payload.length > 4 * 1024 * 1024)
    throw new Error("Invalid provenance payload");
  const statement = Schema.decodeUnknownSync(Statement)(
    JSON.parse(Buffer.from(bundle.dsseEnvelope.payload, "base64").toString("utf8")),
  );
  const workflow = statement.predicate.buildDefinition.externalParameters.workflow;
  const repository = "https://github.com/effect-kit/storage";
  const ref = `refs/heads/${branch}`;
  const source = statement.predicate.buildDefinition.resolvedDependencies;
  if (
    workflow.repository !== repository ||
    workflow.path !== ".github/workflows/release.yml" ||
    workflow.ref !== ref ||
    !source.some(
      (item) => item.uri === `git+${repository}@${ref}` && item.digest.gitCommit === commit,
    ) ||
    statement.subject.length !== 1 ||
    statement.subject[0]?.digest.sha512 !== createHash("sha512").update(entry.bytes).digest("hex")
  )
    throw new Error("Provenance source or archive identity mismatch");
  const material = Schema.decodeUnknownSync(Material)(bundle.verificationMaterial);
  const bytes =
    "certificate" in material
      ? material.certificate.rawBytes
      : material.x509CertificateChain.certificates[0].rawBytes;
  const certificate = new X509Certificate(Buffer.from(bytes, "base64"));
  const identities = new Set([
    `${repository}/.github/workflows/release.yml@${ref}`,
    "https://github.com/effect-kit/.github/.github/workflows/release.yml@refs/tags/v1",
  ]);
  if (
    !certificate.subjectAltName
      ?.split(/,\s*/u)
      .some((san) => identities.has(san.replace(/^URI:/u, "")))
  )
    throw new Error("Unexpected signing workflow identity");
  return { repository, workflow: workflow.path, ref, commit, verifier: "npm-audit-signatures" };
}
