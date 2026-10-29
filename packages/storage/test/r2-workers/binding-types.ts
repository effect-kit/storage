import type { R2Bucket } from "@cloudflare/workers-types";

import type { R2BucketBinding } from "../../src/r2-workers.js";
declare const native: R2Bucket;
export const compatibleBinding: R2BucketBinding = native;
