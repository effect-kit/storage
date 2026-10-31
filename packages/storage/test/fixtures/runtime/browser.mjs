import * as platform from "@effect-kit/storage/browser";

import { runRuntime } from "./program.mjs";

globalThis.runtimeResult = runRuntime(platform, "chromium", false);
