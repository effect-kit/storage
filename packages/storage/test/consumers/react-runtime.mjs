import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import { createElement, StrictMode } from "react";
import { renderToString } from "react-dom/server";

import { RegistryContext } from "@effect/atom-react";
import { Effect, Layer } from "effect";
import { AtomRegistry } from "effect/reactivity";

import { UploadClient } from "@effect-kit/storage/client";
import { UploadAtoms } from "@effect-kit/storage/client/atoms";
import { useUpload } from "@effect-kit/storage/react";

let constructed = 0;
const slot = UploadAtoms.make({
  layer: Layer.effect(
    UploadClient,
    Effect.sync(() => {
      constructed++;
      throw new Error("SSR acquired client");
    }),
  ),
  purpose: "test",
  context: {},
});
function Probe() {
  const upload = useUpload(slot);
  assert.equal(upload.state.phase, "idle");
  return createElement("span", {}, upload.state.phase);
}
const registry = AtomRegistry.make();
assert.equal(
  renderToString(
    createElement(
      StrictMode,
      {},
      createElement(RegistryContext.Provider, { value: registry }, createElement(Probe)),
    ),
  ),
  "<span>idle</span>",
);
assert.equal(constructed, 0);
registry.dispose();
const reactPath = import.meta.resolve("@effect-kit/storage/react");
assert.match(await readFile(new URL(reactPath), "utf8"), /["']use client["']/u);
process.stdout.write("Packed React SSR idle snapshots, directive and runtime imports passed.\n");
