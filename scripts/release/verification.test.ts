import assert from "node:assert/strict";
import { readFile, rename, symlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { test } from "node:test";
import { gunzipSync, gzipSync } from "node:zlib";

import { inspectArchive } from "./archive.ts";
import { verifyCandidate } from "./check.ts";
import { releaseFailure } from "./failure.ts";
import { archive, candidateFixture, workflowCertificate, foreignCertificate } from "./fixtures.ts";
import { integrity, parsePlan, readCandidate, unchanged } from "./plan.ts";
import { verifyProvenance } from "./provenance.ts";
import { matchRegistry } from "./registry.ts";
import { smoke } from "./smoke.ts";

const plan = (entries: unknown[]) => ({ version: 1, plan: [entries] });

test("release failures retain safe command codes without leaking subprocess output", () => {
  const failure = Object.assign(new Error("secret-canary"), {
    stdout: "ERR_PNPM_NO_OFFLINE_META secret-canary",
    stderr: "Bearer secret-canary",
    code: "UNTRUSTED_SECRET_VALUE_WITH_SPACES invalid",
  });
  const report = releaseFailure(failure, "consumer");
  assert.deepEqual(report, { stage: "consumer", code: "ERR_PNPM_NO_OFFLINE_META" });
  assert(!JSON.stringify(report).includes("secret-canary"));
  assert.deepEqual(releaseFailure(null, "candidate-policy"), {
    stage: "candidate-policy",
    code: "CHECK_FAILED",
  });
});

test("archive inspection rejects traversal, links, duplicate entries and corruption", () => {
  const file = { name: "package/dist/index.js", body: "export {};" };
  assert.equal(inspectArchive(archive([file])).size, 1);
  for (const name of [
    "../escape",
    "/package/file",
    "package/../file",
    "package/.env",
    "package/a\\b",
  ])
    assert.throws(() => inspectArchive(archive([{ ...file, name }])));
  for (const kind of ["1", "2", "x", "g", "L", "K"])
    assert.throws(() => inspectArchive(archive([{ ...file, kind }])));
  assert.throws(() => inspectArchive(archive([file, file])));
  const tar = gunzipSync(archive([file]));
  tar[1] = 1;
  assert.throws(() => inspectArchive(gzipSync(tar)));
  assert.throws(() => inspectArchive(gzipSync(Buffer.alloc(512))));
  assert.throws(() => inspectArchive(archive([file]).subarray(0, 20)));
});

test("plan identity and canonical mappings reject malformed release candidates", async () => {
  const fixture = await candidateFixture();
  try {
    const entry = fixture.plan.plan[0]?.[0];
    assert(entry);
    assert.equal(parsePlan(fixture.plan, fixture.expected).length, 2);
    for (const invalid of [
      plan([]),
      plan([entry, entry]),
      plan([{ ...entry, name: "@other/unreviewed" }]),
      plan([{ ...entry, version: "2.0.0" }]),
      plan([{ ...entry, access: "restricted" }]),
      plan([{ ...entry, tag: "--unsafe" }]),
      plan([{ ...entry, unknown: true }]),
      plan([{ ...entry, tarball: { ...entry.tarball, path: "../candidate.tgz" } }]),
    ])
      assert.throws(() => parsePlan(invalid, fixture.expected));

    await writeFile(join(fixture.root, "packages/extra.tgz"), "extra");
    await assert.rejects(readCandidate(fixture.root, fixture.expected), /Extra or missing/u);
  } finally {
    await fixture.dispose();
  }
});

test("exact archives are bound to integrity and cannot be symlinked or replaced", async () => {
  const fixture = await candidateFixture();
  try {
    const candidate = await readCandidate(fixture.root, fixture.expected);
    const entry = candidate.packages[0];
    assert(entry);
    await unchanged(candidate);
    const before = await readFile(entry.archive);
    await writeFile(entry.archive, "changed");
    await assert.rejects(unchanged(candidate), /Archive changed/u);
    await assert.rejects(readCandidate(fixture.root, fixture.expected), /integrity/u);
    await writeFile(entry.archive, before);
    await rename(entry.archive, `${entry.archive}.original`);
    await symlink(`${entry.archive}.original`, entry.archive);
    // A symlink must fail even when its bytes and target are otherwise legitimate.
    await assert.rejects(readCandidate(fixture.root, fixture.expected));
  } finally {
    await fixture.dispose();
  }
});

test("every package is checked; a broken second consumer invalidates a previous pass", async () => {
  const fixture = await candidateFixture();
  try {
    const visited: string[] = [];
    const candidate = await readCandidate(fixture.root, fixture.expected);
    const report = await verifyCandidate(
      fixture.root,
      async (entry) => {
        visited.push(entry.name);
        return ["fixture-consumer"];
      },
      fixture.expected,
    );
    assert.deepEqual(visited, [...fixture.expected.keys()]);
    assert.equal(report.packages.length, 2);
    await unchanged(candidate);

    await assert.rejects(
      verifyCandidate(
        fixture.root,
        async (entry) => {
          if (entry.name.endsWith("second")) throw new Error("Broken second package");
          return ["fixture-consumer"];
        },
        fixture.expected,
      ),
    );
    assert.equal(
      JSON.parse(await readFile(join(fixture.root, "verification.json"), "utf8")).status,
      "failed",
    );
    await unchanged(candidate);
    await writeFile(join(fixture.root, "publish-plan.json"), "{}");
    await assert.rejects(verifyCandidate(fixture.root, async () => [], fixture.expected));
    assert.equal(
      JSON.parse(await readFile(join(fixture.root, "verification.json"), "utf8")).status,
      "failed",
    );
  } finally {
    await fixture.dispose();
  }
});

test("controlled registry smoke checks exact bytes for every package without rebuilding", async () => {
  const fixture = await candidateFixture();
  try {
    const candidate = await readCandidate(fixture.root, fixture.expected);
    const visited: string[] = [];
    const download = async (entry: (typeof candidate.packages)[number]) => ({
      metadata: {
        name: entry.name,
        version: entry.version,
        dist: { integrity: entry.digest, tarball: "https://registry.npmjs.org/fixture.tgz" },
      },
      bytes: entry.bytes,
      audit: undefined,
    });
    const consumer = async (entry: (typeof candidate.packages)[number]) => {
      visited.push(entry.name);
      return ["fixture-consumer"];
    };
    const report = await smoke(fixture.root, false, { download, consumer }, fixture.expected);
    assert.equal(report.status, "passed");
    assert.deepEqual(visited, [...fixture.expected.keys()]);
    await unchanged(candidate);

    const entry = candidate.packages[0];
    assert(entry);
    const fetched = await download(entry);
    assert.throws(() => matchRegistry(fetched.metadata, new Uint8Array([1]), entry));
    assert.throws(() =>
      matchRegistry({ ...fetched.metadata, version: "9.0.0" }, fetched.bytes, entry),
    );
    assert.throws(() => verifyProvenance(undefined, entry, "commit"));
    assert.throws(() =>
      verifyProvenance({ invalid: ["bad"], missing: [], verified: [] }, entry, "commit"),
    );
    assert.throws(() =>
      verifyProvenance({ invalid: [], missing: [], verified: [] }, entry, "commit"),
    );
    await assert.rejects(smoke(fixture.root, true, { download, consumer }, fixture.expected));
    assert.equal(
      JSON.parse(await readFile(join(fixture.root, "registry-verification.json"), "utf8")).status,
      "failed",
    );
    await unchanged(candidate);
  } finally {
    await fixture.dispose();
  }
});

test("a correctly hashed archive with the wrong manifest still fails", async () => {
  const fixture = await candidateFixture();
  try {
    const entry = fixture.plan.plan[0]?.[0];
    assert(entry);
    const bytes = archive([
      {
        name: "package/package.json",
        body: JSON.stringify({ name: entry.name, version: "9.0.0", type: "module" }),
      },
    ]);
    await writeFile(join(fixture.root, entry.tarball.path), bytes);
    entry.tarball.integrity = integrity(bytes);
    await writeFile(join(fixture.root, "publish-plan.json"), JSON.stringify(fixture.plan));
    await assert.rejects(readCandidate(fixture.root, fixture.expected), /identity/u);
  } finally {
    await fixture.dispose();
  }
});

test("provenance policy binds audited evidence to source, bytes and signing workflow", async () => {
  const fixture = await candidateFixture();
  try {
    const candidate = await readCandidate(fixture.root, fixture.expected);
    const entry = candidate.packages[0];
    assert(entry);
    const statement = {
      predicateType: "https://slsa.dev/provenance/v1",
      subject: [
        {
          name: `pkg:npm/${entry.name}@${entry.version}`,
          digest: { sha512: Buffer.from(entry.digest.slice(7), "base64").toString("hex") },
        },
      ],
      predicate: {
        buildDefinition: {
          buildType: "https://slsa-framework.github.io/github-actions-buildtypes/workflow/v1",
          externalParameters: {
            workflow: {
              repository: "https://github.com/effect-kit/storage",
              path: ".github/workflows/release.yml",
              ref: "refs/heads/main",
            },
          },
          resolvedDependencies: [
            {
              uri: "git+https://github.com/effect-kit/storage@refs/heads/main",
              digest: { gitCommit: "expected-commit" },
            },
          ],
        },
        runDetails: { builder: { id: "https://github.com/actions/runner/github-hosted" } },
      },
    };
    const evidence = (payload: unknown, certificate = workflowCertificate) => ({
      invalid: [],
      missing: [],
      verified: [
        {
          name: entry.name,
          version: entry.version,
          attestationBundles: [
            {
              predicateType: "https://github.com/npm/attestation/tree/main/specs/publish/v0.1",
              bundle: { dsseEnvelope: { payload: "" }, verificationMaterial: {} },
            },
            {
              predicateType: "https://slsa.dev/provenance/v1",
              bundle: {
                dsseEnvelope: { payload: Buffer.from(JSON.stringify(payload)).toString("base64") },
                verificationMaterial: { certificate: { rawBytes: certificate } },
              },
            },
          ],
        },
      ],
    });
    assert.equal(
      verifyProvenance(evidence(statement), entry, "expected-commit").commit,
      "expected-commit",
    );
    assert.throws(() => verifyProvenance(evidence(statement), entry, "wrong-commit"));
    assert.throws(() =>
      verifyProvenance(evidence(statement), entry, "expected-commit", "other-branch"),
    );
    assert.throws(() =>
      verifyProvenance(evidence(statement, foreignCertificate), entry, "expected-commit"),
    );
    const wrongArchive = structuredClone(statement);
    const subject = wrongArchive.subject[0];
    assert(subject);
    subject.digest.sha512 = "00";
    assert.throws(() => verifyProvenance(evidence(wrongArchive), entry, "expected-commit"));
    const wrongSource = structuredClone(statement);
    wrongSource.predicate.buildDefinition.externalParameters.workflow.repository =
      "https://github.com/untrusted/storage";
    assert.throws(() => verifyProvenance(evidence(wrongSource), entry, "expected-commit"));
    const missingPublication = evidence(statement);
    const verifiedPackage = missingPublication.verified[0];
    assert(verifiedPackage);
    verifiedPackage.attestationBundles.shift();
    assert.throws(() => verifyProvenance(missingPublication, entry, "expected-commit"));
  } finally {
    await fixture.dispose();
  }
});
