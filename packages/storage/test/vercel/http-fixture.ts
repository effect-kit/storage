import { createServer } from "node:http";

import { Effect } from "effect";

export const httpFixture = Effect.acquireRelease(
  Effect.promise(async () => {
    const objects = new Map<string, { body: Buffer; etag: string; contentType: string }>();
    const uploads = new Map<
      string,
      { key: string; parts: Map<number, Buffer>; contentType: string }
    >();
    const seen: {
      method: string;
      path: string;
      headers: Record<string, string | string[] | undefined>;
    }[] = [];
    let uploadHandler: ((request: Request) => Promise<Response>) | undefined;
    let sequence = 0;
    let fail = false;
    let loseComplete = false;
    let oldCache: { body: Buffer; etag: string } | undefined;
    const result = (key: string, object: { body: Buffer; etag: string; contentType: string }) => ({
      pathname: key,
      url: `${base}/${key}`,
      downloadUrl: `${base}/${key}?download=1`,
      size: object.body.length,
      etag: object.etag,
      contentType: object.contentType,
      contentDisposition: "inline",
      cacheControl: "public,max-age=3600",
      uploadedAt: "2026-10-30T00:00:00Z",
    });
    const server = createServer(async (incoming, outgoing) => {
      const url = new URL(incoming.url ?? "/", `http://${incoming.headers.host}`);
      const chunks: Buffer[] = [];
      for await (const chunk of incoming) chunks.push(Buffer.from(chunk));
      const body = Buffer.concat(chunks);
      seen.push({
        method: incoming.method ?? "",
        path: url.pathname + url.search,
        headers: incoming.headers,
      });
      const json = (status: number, value: unknown) => {
        outgoing.writeHead(status, { "content-type": "application/json" });
        outgoing.end(JSON.stringify(value));
      };
      const error = (status: number, code: string) =>
        json(status, { error: { code, message: "sensitive upstream message" } });

      if (url.pathname === "/upload" && uploadHandler) {
        const headers = Object.fromEntries(
          Object.entries(incoming.headers).flatMap(([key, value]) =>
            value === undefined ? [] : [[key, Array.isArray(value) ? value.join(",") : value]],
          ),
        );
        const response = await uploadHandler(
          new Request(url, { method: "POST", headers, body: new Blob([Uint8Array.from(body)]) }),
        );
        outgoing.writeHead(response.status, Object.fromEntries(response.headers.entries()));
        outgoing.end(Buffer.from(await response.arrayBuffer()));
        return;
      }
      if (fail) {
        error(503, "service_unavailable");
        return;
      }
      if (incoming.headers.authorization?.includes("denied")) {
        error(403, "forbidden");
        return;
      }
      if (url.pathname === "/signed-token") {
        const input = JSON.parse(body.toString());
        const payload = Buffer.from(
          JSON.stringify({
            storeId: "fixture",
            pathname: input.pathname,
            operations: input.operations,
            validUntil: input.validUntil,
          }),
        ).toString("base64url");
        json(200, {
          delegationToken: `${payload}.signature`,
          clientSigningToken: Buffer.alloc(32, 1).toString("base64url"),
          validUntil: input.validUntil,
        });
        return;
      }
      if (url.pathname === "/delete") {
        for (const key of JSON.parse(body.toString()).urls) {
          const object = objects.get(key);
          if (incoming.headers["x-if-match"] && object?.etag !== incoming.headers["x-if-match"]) {
            error(412, "precondition_failed");
            return;
          }
          objects.delete(key);
        }
        json(200, {});
        return;
      }
      if (url.pathname === "/mpu") {
        const key = url.searchParams.get("pathname") ?? "";
        const action = incoming.headers["x-mpu-action"];
        if (action === "create") {
          const id = String(++sequence);
          uploads.set(id, {
            key,
            parts: new Map(),
            contentType: String(incoming.headers["x-content-type"] ?? "application/octet-stream"),
          });
          json(200, { key, uploadId: id });
          return;
        }
        const upload = uploads.get(String(incoming.headers["x-mpu-upload-id"]));
        if (!upload) {
          error(404, "not_found");
          return;
        }
        if (action === "upload") {
          const partNumber = Number(incoming.headers["x-mpu-part-number"]);
          upload.parts.set(partNumber, body);
          json(200, { etag: `"part-${partNumber}"`, partNumber });
          return;
        }
        if (action === "complete") {
          const parts = JSON.parse(body.toString()) as { partNumber: number }[];
          const object = {
            body: Buffer.concat(
              parts.map((part) => upload.parts.get(part.partNumber) ?? Buffer.alloc(0)),
            ),
            etag: `"${++sequence}"`,
            contentType: upload.contentType,
          };
          objects.set(key, object);
          if (loseComplete) {
            loseComplete = false;
            outgoing.destroy();
            return;
          }
          json(200, result(key, object));
          return;
        }
      }
      if (url.pathname === "/" && incoming.method === "PUT") {
        const key = url.searchParams.get("pathname") ?? "";
        const existing = objects.get(key);
        if (
          (incoming.headers["x-allow-overwrite"] === "0" && existing) ||
          (incoming.headers["x-if-match"] && incoming.headers["x-if-match"] !== existing?.etag)
        ) {
          error(412, "precondition_failed");
          return;
        }
        const object = {
          body,
          etag: `"${++sequence}"`,
          contentType: String(incoming.headers["x-content-type"] ?? "application/octet-stream"),
        };
        objects.set(key, object);
        json(200, result(key, object));
        return;
      }
      if (url.pathname === "/" && url.searchParams.has("url")) {
        const key = url.searchParams.get("url") ?? "";
        const object = objects.get(key);
        if (!object) {
          error(404, "not_found");
          return;
        }
        json(200, result(key, object));
        return;
      }
      if (url.pathname === "/" && incoming.method === "GET") {
        const names = [...objects.keys()]
          .filter((key) => key.startsWith(url.searchParams.get("prefix") ?? ""))
          .toSorted();
        const offset = Number(url.searchParams.get("cursor") ?? 0);
        const selected = names.slice(
          offset,
          offset + Number(url.searchParams.get("limit") ?? 1000),
        );
        json(200, {
          blobs: selected.map((key) =>
            result(key, objects.get(key) ?? { body: Buffer.alloc(0), etag: "", contentType: "" }),
          ),
          hasMore: offset + selected.length < names.length,
          cursor: String(offset + selected.length),
        });
        return;
      }
      const key = decodeURIComponent(url.pathname.slice(1));
      const object = objects.get(key);
      if (!object) {
        error(404, "not_found");
        return;
      }
      if (incoming.headers["if-match"] && incoming.headers["if-match"] !== object.etag) {
        error(412, "precondition_failed");
        return;
      }
      if (
        incoming.headers["if-none-match"] === object.etag ||
        incoming.headers["if-none-match"] === "*"
      ) {
        outgoing.writeHead(304, { etag: object.etag });
        outgoing.end();
        return;
      }
      const cached = oldCache && url.searchParams.get("cache") !== "0" ? oldCache : undefined;
      let output = cached?.body ?? object.body;
      const range = /^bytes=(\d*)-(\d*)$/u.exec(String(incoming.headers.range));
      const headers: Record<string, string> = {
        etag: cached?.etag ?? object.etag,
        "content-type": object.contentType,
        "last-modified": "Fri, 30 Oct 2026 00:00:00 GMT",
      };
      if (range) {
        const start = range[1] ? Number(range[1]) : Math.max(0, output.length - Number(range[2]));
        const end = range[1]
          ? Math.min(range[2] ? Number(range[2]) : output.length - 1, output.length - 1)
          : output.length - 1;
        if (start >= output.length) {
          outgoing.writeHead(416);
          outgoing.end();
          return;
        }
        headers["content-range"] = `bytes ${start}-${end}/${output.length}`;
        output = output.subarray(start, end + 1);
      }
      outgoing.writeHead(range ? 206 : 200, {
        ...headers,
        "content-length": String(output.length),
      });
      outgoing.end(output);
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Missing fixture address");
    const base = `http://127.0.0.1:${address.port}`;
    const previous = process.env.VERCEL_BLOB_API_URL;
    const previousRetries = process.env.VERCEL_BLOB_RETRIES;
    process.env.VERCEL_BLOB_API_URL = base;
    process.env.VERCEL_BLOB_RETRIES = "0";
    return {
      server,
      base,
      objects,
      uploads,
      seen,
      previous,
      previousRetries,
      setUploadHandler: (handler: (request: Request) => Promise<Response>) => {
        uploadHandler = handler;
      },
      fail: () => {
        fail = true;
      },
      loseComplete: () => {
        loseComplete = true;
      },
      cache: (body: Buffer) => {
        oldCache = { body, etag: '"cached-old"' };
      },
    };
  }),
  (fixture) =>
    Effect.promise(async () => {
      if (fixture.previous === undefined) delete process.env.VERCEL_BLOB_API_URL;
      else process.env.VERCEL_BLOB_API_URL = fixture.previous;
      if (fixture.previousRetries === undefined) delete process.env.VERCEL_BLOB_RETRIES;
      else process.env.VERCEL_BLOB_RETRIES = fixture.previousRetries;
      fixture.server.closeAllConnections();
      await new Promise<void>((resolve) => fixture.server.close(() => resolve()));
    }),
);
