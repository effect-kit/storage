"use client";

import { useAtomSet, useAtomValue } from "@effect/atom-react";

import type { UploadAtomSet } from "./client/atoms.js";

/** Views subscribe; the explicit registry owns active uploads across component/navigation changes. */
export const useUpload = <E>(atoms: UploadAtomSet<E>) => {
  const state = useAtomValue(atoms.state);
  const initialization = useAtomValue(atoms.initialization);
  const start = useAtomSet(atoms.start);
  const resume = useAtomSet(atoms.resume);
  const pause = useAtomSet(atoms.pause);
  const cancel = useAtomSet(atoms.cancel);
  const retry = useAtomSet(atoms.retry);
  const reconcile = useAtomSet(atoms.reconcile);
  const reset = useAtomSet(atoms.reset);
  const dispose = useAtomSet(atoms.dispose);
  // Results describe command admission/control; completion lives in the upload snapshot.
  const commands = {
    start: useAtomValue(atoms.start),
    resume: useAtomValue(atoms.resume),
    pause: useAtomValue(atoms.pause),
    cancel: useAtomValue(atoms.cancel),
    retry: useAtomValue(atoms.retry),
    reconcile: useAtomValue(atoms.reconcile),
    reset: useAtomValue(atoms.reset),
    dispose: useAtomValue(atoms.dispose),
  };

  return {
    state,
    initialization,
    commands,
    progress: state.progress,
    result: state.status?.result,
    failure: state.failure,
    canPause: state.canPause,
    canResume: state.canResume,
    isRunning: state.isRunning,
    start,
    resume,
    pause,
    cancel,
    retry,
    reconcile,
    reset,
    dispose,
  };
};
