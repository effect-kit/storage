import { Effect } from "effect";

/** Replace this example with your package's public API. */
export const greet = (name: string): Effect.Effect<string> => Effect.succeed(`Hello, ${name}!`);
