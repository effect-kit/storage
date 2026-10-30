import { createServer } from "node:http";

import { Effect } from "effect";

export const httpFixture = Effect.acquireRelease(
  Effect.promise(async () => {
    const objects = new Map<
      string,
      { body: Buffer; etag: string; metadata: Record<string, string>; contentType: string }
    >();
    const sessions = new Map<
      string,
      {
        key: string;
        total: number;
        body: Buffer;
        metadata: Record<string, string>;
        contentType: string;
        upsert: boolean;
      }
    >();
    const uploadTokens = new Map<string, { key: string; expiresAt: number }>();
    const seen: { method: string; path: string; token: string; bytes: number }[] = [];
    let sequence = 0;
    let losePatch = false;
    let expire = false;
    let partial = false;
    let allowTermination = true;
    const server = createServer(async (incoming, outgoing) => {
      const url = new URL(incoming.url ?? "/", `http://${incoming.headers.host}`);
      const chunks: Buffer[] = [];
      for await (const chunk of incoming) chunks.push(Buffer.from(chunk));
      const body = Buffer.concat(chunks);
      seen.push({
        method: incoming.method ?? "",
        path: url.pathname,
        token: String(incoming.headers.authorization),
        bytes: body.length,
      });
      const json = (status: number, value: unknown) => {
        outgoing.writeHead(status, { "content-type": "application/json" });
        outgoing.end(JSON.stringify(value));
      };
      const denied = incoming.headers.authorization === "Bearer denied";
      if (denied) {
        json(403, { code: "AccessDenied", message: "RLS denied secret" });
        return;
      }
      if (url.pathname === "/storage/v1/upload/resumable" && incoming.method === "OPTIONS") {
        outgoing.writeHead(204, {
          "tus-version": "1.0.0",
          "tus-extension": `creation,expiration${allowTermination ? ",termination" : ""}`,
        });
        outgoing.end();
        return;
      }
      if (url.pathname === "/storage/v1/upload/resumable" && incoming.method === "POST") {
        const values = Object.fromEntries(
          String(incoming.headers["upload-metadata"])
            .split(",")
            .map((part) => {
              const [key, value] = part.split(" ");
              return [key, Buffer.from(value ?? "", "base64").toString()];
            }),
        );
        const id = String(++sequence);
        sessions.set(id, {
          key: values.objectName ?? "",
          total: Number(incoming.headers["upload-length"]),
          body: Buffer.alloc(0),
          metadata: JSON.parse(values.metadata ?? "{}"),
          contentType: values.contentType ?? "",
          upsert: incoming.headers["x-upsert"] === "true",
        });
        outgoing.writeHead(201, {
          location: `/storage/v1/upload/resumable/${id}`,
          "tus-resumable": "1.0.0",
        });
        outgoing.end();
        return;
      }
      if (url.pathname.startsWith("/storage/v1/upload/resumable/")) {
        const session = sessions.get(url.pathname.split("/").at(-1) ?? "");
        if (!session || expire) {
          outgoing.writeHead(410);
          outgoing.end();
          return;
        }
        if (incoming.method === "DELETE") {
          sessions.delete(url.pathname.split("/").at(-1) ?? "");
          outgoing.writeHead(204);
          outgoing.end();
          return;
        }
        if (incoming.method === "PATCH") {
          if (Number(incoming.headers["upload-offset"]) !== session.body.length) {
            outgoing.writeHead(409);
            outgoing.end();
            return;
          }
          if (
            session.body.length + body.length === session.total &&
            !session.upsert &&
            objects.has(session.key)
          ) {
            outgoing.writeHead(409);
            outgoing.end();
            return;
          }
          const accepted = partial ? body.subarray(0, Math.ceil(body.length / 2)) : body;
          partial = false;
          session.body = Buffer.concat([session.body, accepted]);
          if (session.body.length === session.total)
            objects.set(session.key, { ...session, etag: `"${++sequence}"` });
          if (losePatch) {
            losePatch = false;
            outgoing.destroy();
            return;
          }
        }
        if (session.total === 0) objects.set(session.key, { ...session, etag: `"${++sequence}"` });
        outgoing.writeHead(incoming.method === "HEAD" ? 200 : 204, {
          "tus-resumable": "1.0.0",
          "upload-offset": String(session.body.length),
          "upload-length": String(session.total),
        });
        outgoing.end();
        return;
      }
      const sign = /^\/storage\/v1\/object\/(upload\/sign|sign)\/bucket\/(.+)$/u.exec(url.pathname);
      if (sign && incoming.method === "POST") {
        const seconds = sign[1] === "sign" ? JSON.parse(body.toString()).expiresIn : 7200;
        const payload = Buffer.from(
          JSON.stringify({ exp: Math.floor(Date.now() / 1000) + seconds }),
        ).toString("base64url");
        const token = `e30.${payload}.${sign[1] === "sign" ? "download-secret" : "upload-secret"}`;
        if (sign[1] === "upload/sign")
          uploadTokens.set(token, {
            key: decodeURIComponent(sign[2] ?? ""),
            expiresAt: Math.floor(Date.now() / 1000) + seconds,
          });
        json(
          200,
          sign[1] === "sign"
            ? { signedURL: `/object/sign/bucket/${sign[2]}?token=${token}` }
            : { url: `/object/upload/sign/bucket/${sign[2]}?token=${token}` },
        );
        return;
      }
      if (sign && incoming.method === "PUT" && sign[1] === "upload/sign") {
        const authority = uploadTokens.get(url.searchParams.get("token") ?? "");
        const key = decodeURIComponent(sign[2] ?? "");
        if (!authority || authority.key !== key || authority.expiresAt <= Date.now() / 1000) {
          json(403, {});
          return;
        }
        if (objects.has(key)) {
          json(409, {});
          return;
        }
        objects.set(key, {
          body,
          etag: `"${++sequence}"`,
          metadata: {},
          contentType: String(incoming.headers["content-type"] ?? "application/octet-stream"),
        });
        json(200, { Key: `bucket/${key}` });
        return;
      }
      if (url.pathname === "/storage/v1/object/list-v2/bucket") {
        const query = JSON.parse(body.toString());
        const all = [...objects.keys()].filter((key) => key.startsWith(query.prefix)).toSorted();
        const folders = query.with_delimiter
          ? [
              ...new Set(
                all
                  .filter((key) => key.slice(query.prefix.length).includes("/"))
                  .map((key) => `${query.prefix + key.slice(query.prefix.length).split("/")[0]}/`),
              ),
            ]
          : [];
        const names = query.with_delimiter
          ? all.filter((key) => !key.slice(query.prefix.length).includes("/"))
          : all;
        const offset = Number(query.cursor ?? 0);
        const selected = names.slice(offset, offset + query.limit);
        json(200, {
          hasNext: offset + selected.length < names.length,
          ...(offset + selected.length < names.length
            ? { nextCursor: String(offset + selected.length) }
            : {}),
          folders: folders.map((key) => ({ name: key, key })),
          objects: selected.map((key) => ({
            name: key,
            key: key.split("/").at(-1),
            metadata: { size: objects.get(key)?.body.length },
          })),
        });
        return;
      }
      if (url.pathname === "/storage/v1/object/bucket" && incoming.method === "DELETE") {
        for (const key of JSON.parse(body.toString()).prefixes) objects.delete(key);
        json(200, []);
        return;
      }
      const info = url.pathname.startsWith("/storage/v1/object/info/bucket/");
      const prefix = info ? "/storage/v1/object/info/bucket/" : "/storage/v1/object/bucket/";
      if (!url.pathname.startsWith(prefix)) {
        json(404, {});
        return;
      }
      const key = decodeURIComponent(url.pathname.slice(prefix.length));
      if (incoming.method === "POST") {
        if (objects.has(key) && incoming.headers["x-upsert"] === "false") {
          json(409, { error: "Duplicate" });
          return;
        }
        objects.set(key, {
          body,
          etag: `"${++sequence}"`,
          metadata: incoming.headers["x-metadata"]
            ? JSON.parse(Buffer.from(String(incoming.headers["x-metadata"]), "base64").toString())
            : {},
          contentType: String(incoming.headers["content-type"]),
        });
        json(200, { Key: `bucket/${key}`, Id: String(sequence) });
        return;
      }
      const object = objects.get(key);
      if (!object) {
        json(400, { code: "NoSuchKey", message: "not found" });
        return;
      }
      if (info) {
        json(200, {
          name: key,
          size: object.body.length,
          etag: object.etag,
          content_type: object.contentType,
          metadata: Object.keys(object.metadata).length ? object.metadata : null,
        });
        return;
      }
      if (
        incoming.headers["if-match"] &&
        incoming.headers["if-match"] !== object.etag &&
        incoming.headers["if-match"] !== "*"
      ) {
        outgoing.writeHead(412);
        outgoing.end();
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
      let output = object.body;
      const range = /^bytes=(\d*)-(\d*)$/u.exec(String(incoming.headers.range));
      const headers: Record<string, string> = {
        etag: object.etag,
        "content-type": object.contentType,
      };
      if (range) {
        const start = range[1]
          ? Number(range[1])
          : Math.max(0, object.body.length - Number(range[2]));
        const end = range[1]
          ? Math.min(range[2] ? Number(range[2]) : object.body.length - 1, object.body.length - 1)
          : object.body.length - 1;
        if (start >= object.body.length) {
          outgoing.writeHead(416);
          outgoing.end();
          return;
        }
        output = object.body.subarray(start, end + 1);
        headers["content-range"] = `bytes ${start}-${end}/${object.body.length}`;
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
    return {
      url: `http://127.0.0.1:${address.port}`,
      server,
      objects,
      sessions,
      seen,
      losePatch: () => {
        losePatch = true;
      },
      expire: () => {
        expire = true;
      },
      partial: () => {
        partial = true;
      },
      noTermination: () => {
        allowTermination = false;
      },
    };
  }),
  (fixture) =>
    Effect.promise(
      () =>
        new Promise<void>((resolve) => {
          fixture.server.closeAllConnections();
          fixture.server.close(() => resolve());
        }),
    ),
);
