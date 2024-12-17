import globals from "globals";
import pluginJs from "@eslint/js";
import prettierConfig from "eslint-config-prettier";

/** @type {import('eslint').Linter.Config[]} */
export default [
  { files: ["**/*.js"], languageOptions: { sourceType: "commonjs" } },
  { languageOptions: { globals: globals.node } },
  pluginJs.configs.recommended,
  prettierConfig,
  {
    ignores: ["**/.yarn", ".pnp.cjs", ".pnp.loader.mjs", "eslint.config.mjs"],
  },
];
