import { createElement, StrictMode, useEffect } from "react";
import { createRoot } from "react-dom/client";

import { RegistryContext } from "@effect/atom-react";
import { Effect } from "effect";
import { AtomRegistry } from "effect/reactivity";

import { UploadClient, UploadClientStore } from "@effect-kit/storage/client";
import { UploadAtoms } from "@effect-kit/storage/client/atoms";
import { useUpload } from "@effect-kit/storage/react";

let registry = AtomRegistry.make();
const views = [];
let visible = true;
let token = "fresh";
let renders = 0;
const slots = [0, 1].map((index) =>
  UploadAtoms.make({
    layer: UploadClient.layer({
      baseUrl: "/storage",
      transport: "xhr",
      headers: () => Effect.sync(() => ({ authorization: `Bearer ${token}` })),
      store: UploadClientStore.webStorage(localStorage, `slot-${index}`),
      deadlineMillis: 10000,
    }),
    purpose: "attachment",
    context: {},
    strategy: "resumable",
  }),
);
function View({ index }) {
  renders++;
  const upload = useUpload(slots[index]);
  useEffect(() => {
    views[index] = upload;
    return () => {
      views[index] = undefined;
    };
  }, [upload]);
  return createElement("output", { "data-slot": index }, upload.state.phase);
}
const root = createRoot(document.getElementById("root"));
const render = () =>
  root.render(
    createElement(
      StrictMode,
      {},
      createElement(
        RegistryContext.Provider,
        { value: registry },
        visible ? slots.map((_, index) => createElement(View, { index, key: index })) : null,
      ),
    ),
  );
render();
globalThis.probe = {
  views,
  renders: () => renders,
  file: (text = "hello", name = "file.txt") => new File([text], name, { type: "text/plain" }),
  snapshot: (index = 0) => registry.get(slots[index].state),
  command: (name, input, index = 0) => registry.set(slots[index][name], input),
  hide: () => {
    visible = false;
    render();
  },
  show: () => {
    visible = true;
    render();
  },
  replaceAccount: () => {
    registry.dispose();
    registry = AtomRegistry.make();
    render();
  },
  token: (value) => {
    token = value;
  },
  dispose: () => {
    root.unmount();
    registry.dispose();
  },
};
