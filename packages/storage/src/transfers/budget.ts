import { Effect } from "effect";

/** FIFO admission reserves a whole workspace, avoiding producer/worker allocation deadlocks. */
export function makeBudget(capacity: number) {
  let reservedBytes = 0;
  let peakReservedBytes = 0;
  const waiters: Array<{ readonly bytes: number; readonly resume: () => void }> = [];

  const drain = () => {
    while (true) {
      const next = waiters[0];
      if (!next || reservedBytes + next.bytes > capacity) break;
      waiters.shift();
      reservedBytes += next.bytes;
      peakReservedBytes = Math.max(peakReservedBytes, reservedBytes);
      next.resume();
    }
  };

  return {
    snapshot: () => ({ reservedBytes, peakReservedBytes }),
    withLease: <A, E, R>(bytes: number, self: Effect.Effect<A, E, R>) =>
      Effect.uninterruptibleMask((restore) => {
        let admitted = false;
        let waiter: (typeof waiters)[number] | undefined;
        const acquire = Effect.callback<void>((resume) => {
          waiter = {
            bytes,
            resume: () => {
              admitted = true;
              resume(Effect.void);
            },
          };
          waiters.push(waiter);
          drain();
        });

        return restore(acquire).pipe(
          Effect.flatMap(() => restore(self)),
          Effect.onExit(() =>
            Effect.sync(() => {
              if (admitted) reservedBytes -= bytes;
              else if (waiter) {
                const index = waiters.indexOf(waiter);
                if (index !== -1) waiters.splice(index, 1);
              }
              drain();
            }),
          ),
        );
      }),
  };
}
