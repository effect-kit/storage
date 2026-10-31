import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gzipSync } from "node:zlib";

import { integrity } from "./plan.ts";

/** Minimal ustar fixture encoder; never used by release packing or verification. */
export function archive(entries: ReadonlyArray<{ name: string; body?: string; kind?: string }>) {
  const blocks: Buffer[] = [];
  for (const { name, body = "", kind = "0" } of entries) {
    const bytes = Buffer.from(body);
    const header = Buffer.alloc(512);
    header.write(name, 0, 100);
    header.write("0000644\0", 100);
    header.write("0000000\0", 108);
    header.write("0000000\0", 116);
    header.write(`${bytes.length.toString(8).padStart(11, "0")}\0`, 124);
    header.write("00000000000\0", 136);
    header.fill(32, 148, 156);
    header.write(kind, 156);
    header.write("ustar\0", 257);
    const checksum = header.reduce((sum, byte) => sum + byte, 0);
    header.write(`${checksum.toString(8).padStart(6, "0")}\0 `, 148);
    blocks.push(header, bytes, Buffer.alloc((512 - (bytes.length % 512)) % 512));
  }
  return gzipSync(Buffer.concat([...blocks, Buffer.alloc(1024)]));
}

export async function candidateFixture() {
  const root = await mkdtemp(join(tmpdir(), "storage-release-test-"));
  await mkdir(join(root, "packages"));
  const names = ["@effect-kit/test-first", "@effect-kit/test-second"];
  const expected = new Map(names.map((name) => [name, "1.0.0"]));
  const license = await readFile("LICENSE", "utf8");
  const entries = await Promise.all(
    names.map(async (name) => {
      const manifest = {
        name,
        version: "1.0.0",
        type: "module",
        exports: "./dist/index.js",
      };
      const bytes = archive([
        { name: "package/package.json", body: JSON.stringify(manifest) },
        { name: "package/LICENSE", body: license },
        { name: "package/dist/index.js", body: "export const value = 1;" },
      ]);
      const path = `packages/${name.slice(1).replace("/", "-")}-1.0.0.tgz`;
      await writeFile(join(root, path), bytes);
      return {
        kind: "publish",
        name,
        version: "1.0.0",
        access: "public",
        tag: "latest",
        tarball: { path, integrity: integrity(bytes) },
      };
    }),
  );
  const plan = { version: 1, plan: [entries] };
  await writeFile(join(root, "publish-plan.json"), JSON.stringify(plan));
  return {
    root,
    expected,
    plan,
    dispose: () => rm(root, { recursive: true, force: true }),
  };
}

// Public self-signed certificates test identity policy only; npm performs cryptographic trust verification.
export const workflowCertificate =
  "MIIDYTCCAkmgAwIBAgIUXcehbYIiu1CN+h1JNm/R9qmZueQwDQYJKoZIhvcNAQELBQAwITEfMB0GA1UEAwwWb2ZmbGluZS1wb2xpY3ktZml4dHVyZTAeFw0yNjEwMzEwNDU5NTRaFw0zNjEwMjgwNDU5NTRaMCExHzAdBgNVBAMMFm9mZmxpbmUtcG9saWN5LWZpeHR1cmUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCU2kM881ME2N9114Tn2Ul7uVlZynXBAQQIOuAv4rWtZRP5grv7CeIMq8AaeynOwXDR4K6cNRSx1fkKy9CE5SkTGJluPsHlNEt1o0i8vaaX/ALG8gOl8lHwMpyBwEJDWQArVNBehPul/szs//X5DvA9qvSzivZuUYZQvr/xoZTbI0MSG2JoITDU7DpY2ygFvnvVkktyD31btIiJxiW53hL+yHF+Dn9Va33DeFQapCEhC6lnM6O5Ff8PPQo8Z0tdItO14nW3k5inrIH1YwQqTK5FtavuuX7k1lztz1LOf4r2IKBZlK3IFoBya10GmRJhuncnhkTcmCWVMK1j0wGM/j4xAgMBAAGjgZAwgY0wHQYDVR0OBBYEFDiZH1wcD6D3tfaiURBymxgd1QuQMA8GA1UdEwEB/wQFMAMBAf8wWwYDVR0RBFQwUoZQaHR0cHM6Ly9naXRodWIuY29tL2VmZmVjdC1raXQvLmdpdGh1Yi8uZ2l0aHViL3dvcmtmbG93cy9yZWxlYXNlLnltbEByZWZzL3RhZ3MvdjEwDQYJKoZIhvcNAQELBQADggEBAG4E1y0p+pGSzRBt7iysgRLxzkg1azXTiE1mgaVxR8UTnCHardt4GbfjlxE/zMlnItW3dJXeFgVyt24aLseCnzj1nn+m8to/hQeg84Jve5iElx0HRI1nGsJjBsQUcv48ctucXRy3cRatRWP9CbC6VA6r69CJNp4LvuTUrBJ7fDYhmnk7Rqpoat2unWTPagn/STbGM7wrNkmWJ78QmdnIL/DuWDTcmbCBTU/FMrzdqlh5I7GTsDfSP7IE+RTXjVLvYAWlnDPz/p1bX/l0AblTagbS++UoUJg4lkBIywSfUs+SkWYzUy2rCS8Nm4Tfb1H5EfW7I1m8PxfOJvfugDyAPpQ=";
export const foreignCertificate =
  "MIIDYDCCAkigAwIBAgIUESpnGzJ3qTiiSUoyDZNShAUciygwDQYJKoZIhvcNAQELBQAwITEfMB0GA1UEAwwWb2ZmbGluZS1wb2xpY3ktZml4dHVyZTAeFw0yNjEwMzEwNDU5NTRaFw0zNjEwMjgwNDU5NTRaMCExHzAdBgNVBAMMFm9mZmxpbmUtcG9saWN5LWZpeHR1cmUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCIC+lvcZCQOde8Y16R1vtn0zRSAQyVAcxY69mYPmfiu1jZ8BH7GJlLN4nUys1nfChNkQvmzqQDVvMacdTvDY0RhvcscjN8OaVbF4Jymj28UFilm0VhwDGiWJsszYI6Dce3O55LY7R0GLcf5Gc1IDoha1vWl2rCvqEeg831EddiAfJmG9zb59AyPYBpr8pGnpT5XfCk1qAt+xLNGKsWFf3yVs25J7UPr/9+2V9dxZOEb28Tdi+WvSBIDLSPeXz4OBuNSFbAifnyxSFyJOT1GlQ7RpPwTpYfGq2oVvIFpWLcMtsYiC9vCOlanG9phh2uRu/wFjASDvjg6upb1GMQdUWvAgMBAAGjgY8wgYwwHQYDVR0OBBYEFFgOQTuejeWecU5yj4xGqs09j3FfMA8GA1UdEwEB/wQFMAMBAf8wWgYDVR0RBFMwUYZPaHR0cHM6Ly9naXRodWIuY29tL3VudHJ1c3RlZC9yZXBvLy5naXRodWIvd29ya2Zsb3dzL3JlbGVhc2UueW1sQHJlZnMvaGVhZHMvbWFpbjANBgkqhkiG9w0BAQsFAAOCAQEANkJzFTZYPUGFjEr5mqcQFCzCuZGPKkDSkR0Ns8n5I4L+7/blv/YotburG2bRbbIyBfmACviUaRQhlS+TDyhgypihqiuArV4/HRySZqU7kkf1qFdflAVJ3rG+PzH3L1MvsbgMAFwH0CNHNIwilGkBhpBFaUXx5V+UyG4wXHoWE6QxE9bWKS8Cx2iyDev8K+wkK65vmP5lvfW5FmOdrTbqNNnNqZDGBDNvmIJazuUVGsLP4S3z+nrT//8qL6Ir0/6aZo4qx+vDJD6m4LU1PmY5rP80FjyNCE5Jo/D7plxXshuoKWz0DGoO7NtRjCI0CfYTWqdohZh1VTNGFMY0IiQF4w==";
