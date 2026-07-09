// Resolves the app's base directory in both run modes:
//  - dev:       node server.js       -> the source folder
//  - packaged:  SixVM-Token-Proxy.exe -> the folder the exe sits in
//    (Node single-executable build; .env, config.json, data/ and the HTML
//    pages live next to the exe)
import path from "node:path";
import { fileURLToPath } from "node:url";

function detectBaseDir() {
  try {
    // require() only exists in the packaged (CommonJS-bundled) build; in dev
    // ESM it throws and we fall through to import.meta.url.
    const { isSea } = require("node:sea");
    if (isSea()) return path.dirname(process.execPath);
  } catch { /* not running inside a single executable */ }
  try {
    return path.dirname(fileURLToPath(import.meta.url));
  } catch {
    return process.cwd();
  }
}

export const BASE_DIR = detectBaseDir();
