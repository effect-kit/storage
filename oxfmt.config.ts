import config from "klarity/oxfmt";

export default {
  ...config,
  ignorePatterns: [...(config.ignorePatterns ?? []), "research/**", ".artifacts/**"],
};
