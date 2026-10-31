import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createServer } from "node:http";

const json = (response, value, code = 200) => {
  response.writeHead(code, { "content-type": "application/json" });
  response.end(JSON.stringify(value));
};
const status = (record) => ({
  version: 1,
  id: record.id,
  state: record.state,
  expiresAt: Date.now() + 60000,
  cleanupPending: record.state === "aborted",
  ...(record.state === "published" ? { result: { key: `verified/${record.id}` } } : {}),
});
/** Controlled protocol server, not a provider qualification fixture. */
export async function protocolServer(bundle) {
  const control = {
    holdData: false,
    holdComplete: false,
    deny: false,
    failCreate: false,
    single: false,
  };
  const records = new Map();
  const keys = new Map();
  const pending = [];
  let created = 0;
  const server = createServer((request, response) => {
    void (async () => {
      const path = request.url;
      if (path === "/") {
        response.setHeader("content-type", "text/html");
        response.end(
          '<!doctype html><html lang="en"><head><title>React upload test</title></head><body><div id="root"></div><script src="/probe.js"></script></body></html>',
        );
        return;
      }
      if (path === "/probe.js") {
        response.setHeader("content-type", "text/javascript");
        response.end(bundle);
        return;
      }
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      const bytes = Buffer.concat(chunks);
      if (
        path.startsWith("/storage/") &&
        (control.deny || request.headers.authorization !== "Bearer fresh")
      ) {
        json(response, { code: "Expired" }, 401);
        return;
      }
      if (path === "/storage/uploads" && request.method === "POST") {
        const { idempotencyKey, input } = JSON.parse(bytes.toString());
        let id = keys.get(idempotencyKey);
        if (!id) {
          id = createHash("sha256").update(idempotencyKey).digest("hex");
          keys.set(idempotencyKey, id);
          records.set(id, { id, input, state: "ready", bytes: Buffer.alloc(0) });
          created++;
        }
        if (control.failCreate) json(response, { code: "Unavailable" }, 503);
        else json(response, status(records.get(id)));
        return;
      }
      const id = path.split("/")[3];
      const record = records.get(id);
      if (!record) {
        json(response, { code: "NotFound" }, 404);
        return;
      }
      if (path.startsWith("/data/")) {
        if (request.method === "PUT") record.bytes = bytes;
        else {
          assert.equal(Number(request.headers["upload-offset"]), record.bytes.length);
          record.bytes = Buffer.concat([record.bytes, bytes]);
        }
        record.state = "uploading";
        const respond = () => {
          response.writeHead(204);
          response.end();
        };
        if (control.holdData) pending.push(respond);
        else respond();
        return;
      }
      if (path.endsWith("/grants")) {
        const url = `http://127.0.0.1:${server.address().port}/data/upload/${id}`;
        const grant = control.single
          ? {
              _tag: "SignedRequest",
              method: "PUT",
              url,
              headers: {},
              expiresAt: Date.now() + 60000,
            }
          : {
              _tag: "Resumable",
              protocol: "tus",
              url,
              headers: {},
              offset: record.bytes.length,
              chunkBytes: 3,
              expiresAt: Date.now() + 60000,
            };
        json(response, { version: 1, id, grant });
        return;
      }
      if (request.method === "DELETE") {
        record.state = "aborted";
        json(response, status(record));
        return;
      }
      if (path.endsWith("/complete") || path.endsWith("/reconcile")) {
        if (record.bytes.length !== record.input.file.size) {
          json(response, { code: "Unavailable" }, 409);
          return;
        }
        assert.equal(
          createHash("sha256").update(record.bytes).digest("hex"),
          record.input.file.sha256,
        );
        record.state = "published";
        const respond = () => json(response, status(record));
        if (control.holdComplete) pending.push(respond);
        else respond();
        return;
      }
      json(response, status(record));
    })().catch((error) => {
      json(response, { code: "Unavailable" }, 500);
      server.emit("fixtureError", error);
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    url: `http://127.0.0.1:${server.address().port}`,
    control,
    records,
    created: () => created,
    release: () => {
      control.holdData = false;
      control.holdComplete = false;
      pending.splice(0).forEach((respond) => respond());
    },
    close: async () => {
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
    },
    onError: (callback) => server.on("fixtureError", callback),
  };
}
