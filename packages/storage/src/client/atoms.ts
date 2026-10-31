import { Effect } from "effect";
import type { Layer } from "effect";
import { AsyncResult, Atom } from "effect/reactivity";

import type { UploadClient } from "../client.js";
import { makeController } from "./atoms/controller.js";
import type { UploadSlotOptions } from "./atoms/controller.js";
import { idleSnapshot } from "./atoms/state.js";
import type { UploadSnapshot } from "./atoms/state.js";

export type { UploadPhase, UploadSnapshot } from "./atoms/state.js";
export type { UploadSlotOptions } from "./atoms/controller.js";

export interface UploadAtomsOptions<E = never> extends UploadSlotOptions {
  readonly layer: Layer.Layer<UploadClient, E>;
}

/** Stable definitions; state and resources are isolated by the application's AtomRegistry. */
const make = <E>(options: UploadAtomsOptions<E>) => {
  const runtime = Atom.runtime(options.layer);
  const initial = AsyncResult.initial();
  const slot = {
    purpose: options.purpose,
    context: Object.freeze({ ...options.context }),
    ...(options.strategy ? { strategy: options.strategy } : {}),
  };
  const writable = Atom.make<UploadSnapshot>(idleSnapshot).pipe(Atom.keepAlive);
  const state = Atom.map(writable, (snapshot) => snapshot).pipe(
    Atom.withServerValue(() => idleSnapshot),
  );
  const controller = runtime
    .atom((get) =>
      makeController(slot, (snapshot) => {
        get.registry.set(writable, snapshot);
      }),
    )
    .pipe(
      Atom.keepAlive,
      Atom.withServerValue(() => initial),
    );

  type Controller = Effect.Success<ReturnType<typeof makeController>>;
  const command = <Input, A, Error>(
    run: (service: Controller, input: Input) => Effect.Effect<A, Error>,
  ) =>
    runtime
      .fn(
        (input: Input, get) =>
          Effect.flatMap(get.result(controller), (service) => run(service, input)),
        { concurrent: true },
      )
      .pipe(Atom.withServerValue(() => initial));

  return {
    state,
    progress: Atom.map(state, (snapshot) => snapshot.progress),
    initialization: controller,
    start: command((service, file: File) => service.start(file)),
    resume: command((service, input: { readonly file: File; readonly uploadId?: string }) =>
      service.resume(input),
    ),
    pause: command((service, _input: void) => service.pause),
    cancel: command((service, _input: void) => service.cancel),
    retry: command((service, _input: void) => service.retry),
    reconcile: command((service, _input: void) => service.reconcile),
    reset: command((service, _input: void) => service.reset),
    dispose: command((service, _input: void) => service.dispose),
  };
};

export const UploadAtoms = { make };
export type UploadAtomSet<E = never> = ReturnType<typeof make<E>>;
