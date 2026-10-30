import { createServer } from "node:http";

import { Effect } from "effect";

export const httpFixture = Effect.acquireRelease(
  Effect.promise(async () => {
    const objects = new Map<string, { metadata: Record<string, unknown>; body: Buffer }>();
    const sessions = new Map<
      string,
      {
        key: string;
        metadata: Record<string, unknown>;
        bytes: Buffer;
        total: number;
        createOnly: boolean;
        expectedGeneration?: string | undefined;
        completed?: boolean;
      }
    >();
    let generation = 9007199254740993n;
    let writes = 0;
    let authenticated = 0;
    let partialOnce = false;
    let loseFinal = false;
    let malformedFinal = false;
    const server = createServer(async (request, response) => {
      if (request.headers["x-goog-api-key"] === "local-fixture-api-key") authenticated++;
      try {
        const url = new URL(request.url ?? "/", `http://${request.headers.host}`);
        const parts: Buffer[] = [];
        for await (const part of request) parts.push(Buffer.from(part));
        const body = Buffer.concat(parts);
        const json = (status: number, value: unknown) => {
          response.writeHead(status, { "Content-Type": "application/json" });
          response.end(JSON.stringify(value));
        };
        if (url.pathname === "/upload/storage/v1/b/test-bucket/o" && request.method === "POST") {
          if (url.searchParams.get("uploadType") === "multipart") {
            const boundary = String(request.headers["content-type"]).split("boundary=")[1] ?? "";
            const metadataStart = body.indexOf("\r\n\r\n") + 4;
            const metadataEnd = body.indexOf(`\r\n--${boundary}\r\n`, metadataStart);
            const dataStart = body.indexOf("\r\n\r\n", metadataEnd + boundary.length + 6) + 4;
            const dataEnd = body.lastIndexOf(`\r\n--${boundary}--`);
            const input = JSON.parse(
              body.subarray(metadataStart, metadataEnd).toString("utf8"),
            ) as Record<string, unknown>;
            const key = String(input.name);
            const expected = url.searchParams.get("ifGenerationMatch");
            if (
              (expected === "0" && objects.has(key)) ||
              (expected && expected !== "0" && objects.get(key)?.metadata.generation !== expected)
            ) {
              json(412, {});
              return;
            }
            const bytes = Buffer.from(body.subarray(dataStart, dataEnd));
            const meta = {
              ...input,
              generation: String(generation++),
              metageneration: "1",
              size: String(bytes.length),
              etag: String(generation),
            };
            objects.set(key, { metadata: meta, body: bytes });
            if (malformedFinal) {
              malformedFinal = false;
              json(200, {});
              return;
            }
            json(200, meta);
            return;
          }
          const id = String(sessions.size + 1);
          sessions.set(id, {
            key: url.searchParams.get("name") ?? "",
            metadata: JSON.parse(body.toString()),
            bytes: Buffer.alloc(0),
            total: Number(request.headers["x-upload-content-length"]),
            createOnly: url.searchParams.get("ifGenerationMatch") === "0",
            expectedGeneration: url.searchParams.get("ifGenerationMatch") ?? undefined,
          });
          response.writeHead(200, {
            location: `${url.origin}/upload/storage/v1/b/test-bucket/o?uploadType=resumable&upload_id=${id}`,
          });
          response.end();
          return;
        }
        if (
          url.pathname === "/upload/storage/v1/b/test-bucket/o" &&
          url.searchParams.has("upload_id")
        ) {
          const session = sessions.get(url.searchParams.get("upload_id") ?? "");
          if (!session) {
            json(404, {});
            return;
          }
          if (request.method === "DELETE") {
            sessions.delete(url.searchParams.get("upload_id") ?? "");
            response.writeHead(499);
            response.end();
            return;
          }
          if (session.completed) {
            json(200, objects.get(session.key)?.metadata);
            return;
          }
          const range = String(request.headers["content-range"]);
          if (!body.length && session.total !== 0) {
            response.writeHead(
              308,
              session.bytes.length ? { range: `bytes=0-${session.bytes.length - 1}` } : {},
            );
            response.end();
            return;
          }
          const match = /^bytes (\d+)-(\d+)\/(\d+|\*)$/u.exec(range);
          if (match && Number(match[1]) !== session.bytes.length) {
            json(400, {});
            return;
          }
          writes++;
          const accepted = partialOnce ? Math.floor(body.length / 2) : body.length;
          partialOnce = false;
          session.bytes = Buffer.concat([session.bytes, body.subarray(0, accepted)]);
          if (session.bytes.length === session.total) {
            if (
              session.expectedGeneration &&
              session.expectedGeneration !== "0" &&
              objects.get(session.key)?.metadata.generation !== session.expectedGeneration
            ) {
              json(412, {});
              return;
            }
            if (session.createOnly && objects.has(session.key)) {
              json(412, {});
              return;
            }
            session.completed = true;
            const meta = {
              ...session.metadata,
              name: session.key,
              generation: String(generation++),
              metageneration: "1",
              size: String(session.total),
              etag: String(generation),
            };
            objects.set(session.key, { metadata: meta, body: session.bytes });
            if (loseFinal) {
              loseFinal = false;
              request.socket.destroy();
              return;
            }
            if (malformedFinal) {
              malformedFinal = false;
              json(200, {});
              return;
            }
            json(200, meta);
            return;
          }
          response.writeHead(
            308,
            session.bytes.length ? { range: `bytes=0-${session.bytes.length - 1}` } : {},
          );
          response.end();
          return;
        }
        if (url.pathname.includes("/rewriteTo/")) {
          const sourceName = decodeURIComponent(
            url.pathname.split("/o/")[1]?.split("/rewriteTo/")[0] ?? "",
          );
          const source = objects.get(sourceName);
          if (!source || url.searchParams.get("sourceGeneration") !== source.metadata.generation) {
            json(412, {});
            return;
          }
          if (!url.searchParams.get("rewriteToken")) {
            json(200, {
              done: false,
              rewriteToken: "continuation",
              totalBytesRewritten: "0",
              objectSize: String(source.body.length),
            });
            return;
          }
          const destination = decodeURIComponent(url.pathname.split("/o/").at(-1) ?? "");
          const result = {
            ...source.metadata,
            ...JSON.parse(body.toString()),
            name: destination,
            generation: String(generation++),
          };
          objects.set(destination, { metadata: result, body: Buffer.from(source.body) });
          json(200, {
            done: true,
            totalBytesRewritten: String(source.body.length),
            objectSize: String(source.body.length),
            resource: result,
          });
          return;
        }
        if (url.pathname.endsWith("/compose")) {
          const destination = decodeURIComponent(url.pathname.split("/o/")[1]?.slice(0, -8) ?? "");
          const input = JSON.parse(body.toString()) as {
            sourceObjects: Array<{ name: string; generation: string }>;
            destination: Record<string, unknown>;
          };
          const sources = input.sourceObjects.map((entry) => objects.get(entry.name));
          if (
            sources.some(
              (entry, index) =>
                !entry || entry.metadata.generation !== input.sourceObjects[index]?.generation,
            )
          ) {
            json(412, {});
            return;
          }
          const bytes = Buffer.concat(sources.flatMap((entry) => (entry ? [entry.body] : [])));
          const result = {
            ...input.destination,
            name: destination,
            generation: String(generation++),
            size: String(bytes.length),
            metageneration: "1",
          };
          objects.set(destination, { metadata: result, body: bytes });
          json(200, result);
          return;
        }
        const key = decodeURIComponent(url.pathname.split("/o/")[1] ?? "");
        if (!key) {
          const prefix = url.searchParams.get("prefix") ?? "";
          const delimiter = url.searchParams.get("delimiter");
          const entries = new Map<
            string,
            { prefix?: string; metadata?: Record<string, unknown> }
          >();
          for (const [name, entry] of objects) {
            if (!name.startsWith(prefix)) continue;
            const separator = delimiter ? name.indexOf(delimiter, prefix.length) : -1;
            if (separator >= 0) {
              const common = name.slice(0, separator + (delimiter?.length ?? 0));
              entries.set(common, { prefix: common });
            } else entries.set(name, { metadata: entry.metadata });
          }
          const start = Number(url.searchParams.get("pageToken") ?? "0");
          const limit = Number(url.searchParams.get("maxResults") ?? "1000");
          const ordered = [...entries.entries()]
            .toSorted(([left], [right]) => left.localeCompare(right))
            .map(([, entry]) => entry);
          const page = ordered.slice(start, start + limit);
          json(200, {
            items: page.flatMap((entry) => (entry.metadata ? [entry.metadata] : [])),
            prefixes: page.flatMap((entry) => (entry.prefix ? [entry.prefix] : [])),
            ...(start + limit < ordered.length ? { nextPageToken: String(start + limit) } : {}),
          });
          return;
        }
        const stored = objects.get(key);
        if (
          !stored ||
          (url.searchParams.get("generation") &&
            url.searchParams.get("generation") !== stored.metadata.generation)
        ) {
          json(404, {});
          return;
        }
        if (
          request.headers["if-match"] &&
          request.headers["if-match"] !== `"${String(stored.metadata.etag)}"`
        ) {
          json(412, {});
          return;
        }
        if (
          request.headers["if-none-match"] === "*" ||
          request.headers["if-none-match"] === `"${String(stored.metadata.etag)}"`
        ) {
          response.writeHead(304);
          response.end();
          return;
        }
        if (url.searchParams.get("ifGenerationNotMatch") === stored.metadata.generation) {
          response.writeHead(304);
          response.end();
          return;
        }
        if (request.method === "PATCH") {
          if (
            url.searchParams.get("ifMetagenerationMatch") &&
            url.searchParams.get("ifMetagenerationMatch") !== stored.metadata.metageneration
          ) {
            json(412, {});
            return;
          }
          stored.metadata = {
            ...stored.metadata,
            ...JSON.parse(body.toString()),
            metageneration: String(Number(stored.metadata.metageneration) + 1),
          };
          json(200, stored.metadata);
          return;
        }
        if (request.method === "DELETE") {
          objects.delete(key);
          response.writeHead(204);
          response.end();
          return;
        }
        if (url.searchParams.get("alt") === "media") {
          const headers: Record<string, string> = {
            "x-goog-generation": String(stored.metadata.generation),
            "x-goog-metageneration": "1",
            "content-type": "application/octet-stream",
            etag: `"${String(stored.metadata.etag)}"`,
          };
          if (stored.metadata.crc32c)
            headers["x-goog-hash"] = `crc32c=${String(stored.metadata.crc32c)}`;
          for (const [name, value] of Object.entries(
            (stored.metadata.metadata as Record<string, string>) ?? {},
          ))
            headers[`x-goog-meta-${name}`] = value;
          let bytes = stored.body;
          if (request.headers.range) {
            const match = /^bytes=(\d*)-(\d*)$/u.exec(request.headers.range);
            const start = match?.[1]
              ? Number(match[1])
              : Math.max(0, bytes.length - Number(match?.[2]));
            const end =
              match?.[1] && match?.[2]
                ? Math.min(Number(match[2]), bytes.length - 1)
                : bytes.length - 1;
            if (start >= bytes.length || end < start) {
              json(416, {});
              return;
            }
            headers["content-range"] = `bytes ${start}-${end}/${bytes.length}`;
            bytes = bytes.subarray(start, end + 1);
          }
          headers["content-length"] = String(bytes.length);
          response.writeHead(request.headers.range ? 206 : 200, headers);
          response.end(bytes);
          return;
        }
        json(200, stored.metadata);
      } catch {
        response.writeHead(500);
        response.end();
      }
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Fixture address unavailable");
    return {
      server,
      endpoint: `http://127.0.0.1:${address.port}`,
      objects,
      sessions,
      writes: () => writes,
      authenticatedRequests: () => authenticated,
      partial: () => {
        partialOnce = true;
      },
      malformFinal: () => {
        malformedFinal = true;
      },
      loseFinal: () => {
        loseFinal = true;
      },
    };
  }),
  (fixture) =>
    Effect.promise(
      () =>
        new Promise<void>((resolve, reject) => {
          fixture.server.closeAllConnections();
          fixture.server.close((error) => (error ? reject(error) : resolve()));
        }),
    ),
);
