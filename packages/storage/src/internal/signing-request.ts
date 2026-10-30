import { Schema } from "effect";

import { ObjectRef } from "../object.js";
import { decodeRequest } from "./request-validation.js";

const Request = Schema.Struct({
  ref: ObjectRef,
  method: Schema.Literals(["GET", "HEAD", "PUT", "DELETE"]),
  expiresInSeconds: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 604800 })),
});
export const signingRequest = (input: unknown, provider: string) =>
  decodeRequest(Request, input, "sign", provider, true);
