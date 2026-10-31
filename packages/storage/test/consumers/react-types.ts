import { createElement } from "react";

import { RegistryProvider, useAtomValue } from "@effect/atom-react";
import { Effect, Layer } from "effect";
import type { Atom } from "effect/reactivity";

import { UploadClient } from "@effect-kit/storage/client";
import { UploadAtoms } from "@effect-kit/storage/client/atoms";
import { useUpload } from "@effect-kit/storage/react";

const slot = UploadAtoms.make({
  layer: UploadClient.layer({ baseUrl: "/storage" }),
  purpose: "attachment",
  context: {},
});
function Component() {
  const upload = useUpload(slot);
  const progress = useAtomValue(slot.progress);
  upload.start(new File([], "empty.txt"));
  upload.resume({ file: new File([], "empty.txt"), uploadId: "checkpoint" });
  upload.pause();
  upload.retry();
  upload.cancel();
  upload.reconcile();
  upload.reset();
  upload.dispose();
  return createElement("span", {}, progress?.measurement ?? upload.state.phase);
}
createElement(RegistryProvider, {}, createElement(Component));
const failed = UploadAtoms.make({
  layer: Layer.effect(UploadClient, Effect.fail("initialization" as const)),
  purpose: "attachment",
  context: {},
});
const initialization: Atom.Failure<typeof failed.initialization> = "initialization";
void initialization;
