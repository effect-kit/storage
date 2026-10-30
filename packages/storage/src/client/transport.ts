import { Effect } from "effect";

import { failUpload } from "../uploads/schema.js";
export interface UploadProgress {
  readonly phase: "hashing" | "uploading" | "verifying" | "completed";
  readonly bytesSent?: number;
  readonly bytesAccepted: number;
  readonly totalBytes: number;
  readonly measurement: "xhr" | "provider-accepted" | "indeterminate";
}
export type Progress = (progress: UploadProgress) => void;
export const providerUrl = (value: string) =>
  Effect.try({
    try: () => {
      const url = new URL(value);
      if (
        url.username ||
        url.password ||
        url.hash ||
        (url.protocol !== "https:" &&
          !(url.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname)))
      )
        throw new Error("Invalid grant endpoint");
      return url.href;
    },
    catch: () => failUpload("BadRequest"),
  });
export const fetchTransfer = (
  url: string,
  method: string,
  headers: Readonly<Record<string, string>>,
  body: Blob | FormData,
) =>
  Effect.tryPromise({
    try: async (signal) => {
      const response = await fetch(url, {
        method,
        headers,
        body,
        credentials: "omit",
        redirect: "error",
        signal,
      });
      await response.body?.cancel();
      return { status: response.status, headers: response.headers };
    },
    catch: () => failUpload("Unavailable"),
  });
export const xhrTransfer = (
  url: string,
  method: string,
  headers: Readonly<Record<string, string>>,
  body: Blob | FormData,
  progress: (sent: number) => void,
) =>
  Effect.callback<
    { readonly status: number; readonly headers: Headers },
    ReturnType<typeof failUpload>
  >((resume) => {
    if (typeof XMLHttpRequest === "undefined") {
      resume(Effect.fail(failUpload("Unsupported")));
      return;
    }
    const xhr = new XMLHttpRequest();
    const listeners = new AbortController();
    try {
      xhr.open(method, url);
      xhr.withCredentials = false;
      for (const [name, value] of Object.entries(headers))
        if (!["content-length", "host"].includes(name.toLowerCase()))
          xhr.setRequestHeader(name, value);
      xhr.upload.addEventListener(
        "progress",
        (event) => {
          try {
            progress(event.loaded);
          } catch {
            xhr.abort();
            resume(Effect.fail(failUpload("Unavailable")));
          }
        },
        { signal: listeners.signal },
      );
      xhr.addEventListener(
        "load",
        () => {
          const parsed = new Headers();
          for (const line of xhr
            .getAllResponseHeaders()
            .trim()
            .split(/[\r\n]+/u)) {
            const colon = line.indexOf(":");
            if (colon > 0) parsed.append(line.slice(0, colon), line.slice(colon + 1).trim());
          }
          resume(Effect.succeed({ status: xhr.status, headers: parsed }));
        },
        { signal: listeners.signal },
      );
      xhr.addEventListener("error", () => resume(Effect.fail(failUpload("Unavailable"))), {
        signal: listeners.signal,
      });
      xhr.addEventListener("abort", () => resume(Effect.fail(failUpload("Unavailable"))), {
        signal: listeners.signal,
      });
      xhr.send(body);
    } catch {
      resume(Effect.fail(failUpload("Unavailable")));
    }
    return Effect.sync(() => {
      listeners.abort();
      xhr.abort();
    });
  });
