// Feature settings, stored in config.json next to the server (gitignored).
// Managed from the /setup page — no manual editing needed.
import fs from "node:fs";
import path from "node:path";
import { BASE_DIR } from "./paths.js";

const CONFIG_PATH = path.join(BASE_DIR, "config.json");

export const DEFAULTS = {
  // Add Anthropic's cache_control marker to large system prompts automatically,
  // so repeated prompt content is billed at ~10% of the normal input price.
  autoCache: true,
  // Answer byte-identical repeat requests from a local cache — zero tokens.
  responseCache: { enabled: false, ttlSeconds: 3600, maxEntries: 500 },
  // Monthly USD caps per client tag; "*" applies to every client without its
  // own entry. A client over budget gets a clear 429 instead of spending more.
  budgets: {},
  // POST a message to a Discord/Slack webhook when daily spend crosses the
  // threshold or a client hits its budget. weeklyReport also posts a 7-day
  // summary every Monday morning.
  alerts: { webhookUrl: "", dailyUsdThreshold: 0, weeklyReport: true },
  // Request explorer: keep the last N prompts & answers in memory so rows on
  // the dashboard can be inspected. Never written to disk.
  capture: { enabled: false, maxEntries: 100 },
  // Route requests to a different model per task tag, e.g.
  // { "summarize": "claude-haiku-4-5", "*": "" }. Empty = no routing.
  routing: {},
  // Output-token guards: a hard cap on max_tokens per request (0 = off), and
  // an effort level per task tag ("low" | "medium" | "high") for models that
  // support it — lower effort means fewer thinking/output tokens.
  limits: { maxOutputTokens: 0, taskEffort: {} },
  // Automatic model selection per request. mode: "off" | "rules" (instant
  // heuristics) | "smart" (heuristics + a tiny judge call for unclear cases).
  // Downgrade-only unless allowUpgrade; cascade retries weak answers on the
  // originally requested model. Never switches model mid-conversation.
  autoRouter: { mode: "off", allowUpgrade: false, cascade: false, judgeModel: "claude-haiku-4-5" },
  // Transient upstream failures (429/500/529/network) are retried with backoff.
  // failoverModel: if set (e.g. "gpt-4o-mini" or "gemini-2.5-flash"), text-only
  // requests that still fail after retries are answered by that model instead.
  reliability: { retries: 2, failoverModel: "" },
  // Data Shield: scans every request for sensitive data before it reaches the
  // AI provider. mode "mask" replaces it with placeholders, "block" rejects the
  // request. Each detector can be turned on/off.
  dataShield: {
    enabled: false,
    mode: "mask", // "mask" | "block"
    detectors: { credit_card: true, ssn: true, email: true, api_key: true, phone: false },
  },
};

function merge(base, extra) {
  for (const [k, v] of Object.entries(extra ?? {})) {
    if (v && typeof v === "object" && !Array.isArray(v) && typeof base[k] === "object" && !Array.isArray(base[k])) {
      merge(base[k], v);
    } else {
      base[k] = v;
    }
  }
  return base;
}

export function loadConfig() {
  try {
    return merge(structuredClone(DEFAULTS), JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8")));
  } catch {
    return structuredClone(DEFAULTS);
  }
}

export function saveConfig(cfg) {
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2) + "\n");
}
