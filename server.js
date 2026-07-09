// SixVM Token Proxy — pass-through proxy for the Anthropic Messages API with
// usage logging, automatic prompt caching, a repeat-answer cache, per-client
// budgets, and spend alerts. Requests reach Anthropic unchanged except for the
// optional cache_control marker added by the auto-cache feature.
import dotenv from "dotenv";
import express from "express";
import { exec } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import { Readable } from "node:stream";
import path from "node:path";
import { BASE_DIR } from "./paths.js";
import { loadConfig, saveConfig } from "./config.js";
import { initStorage, isPlaceholder } from "./storage.js";

const APP_DIR = BASE_DIR;
dotenv.config({ path: path.join(APP_DIR, ".env") });
const ANTHROPIC_URL = "https://api.anthropic.com/v1/messages";
const PORT = process.env.PORT || 8787;

let cfg = loadConfig();
let storage = null; // initialized in boot() below

// PLACEHOLDER PRICES — per-million-token USD rates, keyed by model name.
// Cache write assumes the 5-minute-TTL rate (1.25x input); cache read is 0.1x input.
// These must be updated from Anthropic's current pricing page
// (https://platform.claude.com/docs/en/pricing) before est_cost can be trusted.
const PRICING = {
  "claude-fable-5":    { input: 10.0, output: 50.0, cacheWrite: 12.5,  cacheRead: 1.0 },
  "claude-opus-4-8":   { input: 5.0,  output: 25.0, cacheWrite: 6.25,  cacheRead: 0.5 },
  "claude-opus-4-7":   { input: 5.0,  output: 25.0, cacheWrite: 6.25,  cacheRead: 0.5 },
  "claude-opus-4-6":   { input: 5.0,  output: 25.0, cacheWrite: 6.25,  cacheRead: 0.5 },
  "claude-sonnet-4-6": { input: 3.0,  output: 15.0, cacheWrite: 3.75,  cacheRead: 0.3 },
  "claude-haiku-4-5":  { input: 1.0,  output: 5.0,  cacheWrite: 1.25,  cacheRead: 0.1 },
  // OpenAI (ChatGPT) — placeholders; verify at https://openai.com/api/pricing
  "gpt-4o-mini":       { input: 0.15, output: 0.6,  cacheWrite: 0,     cacheRead: 0.075 },
  "gpt-4o":            { input: 2.5,  output: 10.0, cacheWrite: 0,     cacheRead: 1.25 },
  "gpt-4.1-mini":      { input: 0.4,  output: 1.6,  cacheWrite: 0,     cacheRead: 0.1 },
  "gpt-4.1":           { input: 2.0,  output: 8.0,  cacheWrite: 0,     cacheRead: 0.5 },
  // Google Gemini — placeholders; verify at https://ai.google.dev/pricing
  "gemini-2.5-pro":    { input: 1.25, output: 10.0, cacheWrite: 0,     cacheRead: 0.31 },
  "gemini-2.5-flash":  { input: 0.3,  output: 2.5,  cacheWrite: 0,     cacheRead: 0.075 },
  "gemini-2.0-flash":  { input: 0.1,  output: 0.4,  cacheWrite: 0,     cacheRead: 0.025 },
};

// The API may return a dated full ID (e.g. claude-haiku-4-5-20251001), so fall
// back to a prefix match against the alias keys above.
function pricingFor(model) {
  if (!model) return null;
  if (PRICING[model]) return PRICING[model];
  const key = Object.keys(PRICING).find((k) => model.startsWith(k));
  return key ? PRICING[key] : null;
}

function estimateCost(model, usage) {
  const rates = pricingFor(model);
  if (!rates) return null;
  return (
    ((usage.input_tokens ?? 0) * rates.input +
      (usage.output_tokens ?? 0) * rates.output +
      (usage.cache_creation_input_tokens ?? 0) * rates.cacheWrite +
      (usage.cache_read_input_tokens ?? 0) * rates.cacheRead) /
    1_000_000
  );
}

// Reconstructs the fields logUsage needs from a streamed (SSE) response:
// message_start carries id/model/input+cache tokens, and the final
// message_delta carries the cumulative output_tokens.
function parseSseUsage(sseText) {
  let message = null;
  const finalUsage = {};
  for (const line of sseText.split("\n")) {
    if (!line.startsWith("data:")) continue;
    let evt;
    try {
      evt = JSON.parse(line.slice(5));
    } catch {
      continue;
    }
    if (evt.type === "message_start") message = evt.message;
    else if (evt.type === "message_delta" && evt.usage) Object.assign(finalUsage, evt.usage);
  }
  if (!message) return null;
  return { id: message.id, model: message.model, usage: { ...message.usage, ...finalUsage } };
}

// ---------------------------------------------------------------------------
// Usage logging — fire-and-forget, never affects the caller.
// ---------------------------------------------------------------------------
// Live feed: dashboards connected via SSE get pinged whenever a row is logged.
const liveClients = new Set();
function broadcastLive() {
  for (const res of liveClients) {
    try { res.write("data: {\"logged\":true}\n\n"); } catch { /* client gone */ }
  }
}

// Error tracking: recent upstream/proxy errors, newest first (memory-only).
const errorsLog = [];
function recordError(entry) {
  errorsLog.unshift({ at: new Date().toISOString(), ...entry });
  if (errorsLog.length > 100) errorsLog.pop();
}

async function logUsage({ data, client, taskType, latencyMs, cacheHit = false, provider = "anthropic" }) {
  const usage = data.usage ?? {};
  // A repeat-cache hit spends nothing; tokens are logged so the dashboard's
  // baseline can show what the request WOULD have cost.
  const estCost = cacheHit ? 0 : estimateCost(data.model, usage);

  if (!storage) return;
  try {
    await storage.insertUsage({
      created_at: new Date().toISOString(),
      client,
      task_type: taskType,
      model: data.model ?? null,
      input_tokens: usage.input_tokens ?? 0,
      output_tokens: usage.output_tokens ?? 0,
      cache_write_tokens: usage.cache_creation_input_tokens ?? 0,
      cache_read_tokens: usage.cache_read_input_tokens ?? 0,
      cache_hit: cacheHit,
      est_cost: estCost,
      request_id: data.id ?? null,
      latency_ms: latencyMs,
    });
    trackSpend(client, estCost ?? 0);
    broadcastLive();
  } catch (err) {
    console.error("usage logging failed:", err.message ?? err);
  }
}

// ---------------------------------------------------------------------------
// Feature 1: automatic prompt caching.
// Adds Anthropic's cache_control marker to large system prompts so repeated
// prompt content bills at ~10% of the input price. Skipped when the request
// already uses cache_control anywhere.
// ---------------------------------------------------------------------------
function maybeAddCacheControl(body) {
  if (!cfg.autoCache) return;
  try {
    // If the caller manages caching itself, stay out of the way entirely.
    if (JSON.stringify(body).includes('"cache_control"')) return;

    // System-prompt breakpoint. ~5000 chars ≈ 1250 tokens; below each model's
    // cacheable minimum the API silently ignores the marker, so a low
    // threshold is harmless.
    const sys = body?.system;
    const sysLen = typeof sys === "string"
      ? sys.length
      : Array.isArray(sys) ? sys.reduce((a, b) => a + (b?.text?.length ?? 0), 0) : 0;
    if (sysLen >= 5000) {
      if (typeof sys === "string") {
        body.system = [{ type: "text", text: sys, cache_control: { type: "ephemeral" } }];
      } else {
        const last = sys[sys.length - 1];
        if (last?.type === "text") last.cache_control = { type: "ephemeral" };
      }
    }

    // Conversation-history breakpoint. Multi-turn agents resend the whole
    // history every turn; marking the newest block lets the next turn read
    // everything before it from cache at ~10% price.
    const msgs = body?.messages;
    if (Array.isArray(msgs) && msgs.length && JSON.stringify(msgs).length >= 5000) {
      const lastMsg = msgs[msgs.length - 1];
      if (typeof lastMsg?.content === "string") {
        lastMsg.content = [{ type: "text", text: lastMsg.content, cache_control: { type: "ephemeral" } }];
      } else if (Array.isArray(lastMsg?.content) && lastMsg.content.length) {
        const lastBlock = lastMsg.content[lastMsg.content.length - 1];
        if (["text", "tool_result", "image", "document"].includes(lastBlock?.type)) {
          lastBlock.cache_control = { type: "ephemeral" };
        }
      }
    }
  } catch {
    /* never break a request over an optimization */
  }
}

// ---------------------------------------------------------------------------
// Feature: smart model routing + output-token guards (configured per task tag).
// ---------------------------------------------------------------------------
const EFFORT_MODELS = ["claude-fable-5", "claude-opus-4-8", "claude-opus-4-7", "claude-opus-4-6", "claude-opus-4-5", "claude-sonnet-4-6"];

function applyRoutingAndLimits(body, taskType) {
  let manuallyRouted = false;
  try {
    // Per-task model override ("*" = every task without its own rule).
    const route = cfg.routing?.[taskType] ?? cfg.routing?.["*"];
    if (typeof route === "string" && route.trim() && body?.model) {
      body.model = route.trim();
      manuallyRouted = true;
    }

    // Hard cap on requested output tokens — output is the most expensive kind.
    const cap = Number(cfg.limits?.maxOutputTokens) || 0;
    if (cap > 0 && Number(body?.max_tokens) > cap) body.max_tokens = cap;

    // Effort level per task, only on models that support the parameter and
    // only when the caller didn't set one itself.
    const effort = cfg.limits?.taskEffort?.[taskType];
    if (["low", "medium", "high"].includes(effort)
      && EFFORT_MODELS.some((m) => String(body?.model ?? "").startsWith(m))
      && body?.output_config?.effort == null) {
      body.output_config = { ...(body.output_config ?? {}), effort };
    }
  } catch {
    /* guards must never break a request */
  }
  return manuallyRouted;
}

// ---------------------------------------------------------------------------
// Auto-router: picks the model per request based on task difficulty.
// Level 1: instant heuristics. Level 2 ("smart"): a tiny judge call for
// unclear cases. Level 3 (cascade, applied later in the route): weak answers
// are retried on the originally requested model. Downgrade-only unless
// allowUpgrade, and never switches model mid-conversation (prompt caches are
// per-model, so a mid-conversation switch would forfeit the cache discount).
// ---------------------------------------------------------------------------
const TIER_MODELS = { 1: "claude-haiku-4-5", 2: "claude-sonnet-4-6" }; // tier 3 = keep requested
const SIMPLE_RE = /\b(summari[sz]e|summary|translate|translation|classif|extract|rewrite|rephrase|shorten|proofread|grammar|sentiment|keyword|one word|yes or no|bullet point|title for|caption)/i;
const COMPLEX_RE = /\b(code|coding|program|debug|refactor|implement|architect|algorithm|analy[sz]e|research|strateg|design a|prove|derive|optimi[sz]e|security|vulnerab|legal|contract|diagnos|step[- ]by[- ]step plan)/i;

const judgeCache = new Map(); // question hash -> { tier, expires }
const routerLog = [];         // newest first, capped

function modelRank(model) {
  const p = pricingFor(model);
  return p ? p.input : Infinity; // unknown models rank most expensive — never auto-picked over
}

function requestText(body) {
  let out = "";
  const sys = body?.system;
  if (typeof sys === "string") out += sys.slice(0, 800);
  else if (Array.isArray(sys)) out += sys.map((b) => b?.text ?? "").join(" ").slice(0, 800);
  const last = body?.messages?.[body.messages.length - 1];
  const c = last?.content;
  if (typeof c === "string") out += " " + c;
  else if (Array.isArray(c)) out += " " + c.map((b) => b?.text ?? "").join(" ");
  return out.slice(0, 2500);
}

function heuristicTier(body) {
  if (Array.isArray(body?.tools) && body.tools.length) return { tier: 3, confident: true };
  const text = requestText(body);
  const msgJson = JSON.stringify(body?.messages ?? []);
  let score = 0;
  if (msgJson.length > 20000) score += 2;
  else if (msgJson.length > 6000) score += 1;
  if ((body?.max_tokens ?? 0) >= 4000) score += 1;
  if (msgJson.includes('"image"') || msgJson.includes('"document"')) score += 1;
  if (COMPLEX_RE.test(text)) score += 2;
  if (SIMPLE_RE.test(text)) score -= 2;
  if (score <= -1) return { tier: 1, confident: true };
  if (score >= 3) return { tier: 3, confident: true };
  if (score === 2) return { tier: 2, confident: true };
  return { tier: 2, confident: false }; // 0–1: genuinely unclear
}

async function judgeTier(body) {
  const q = requestText(body);
  const key = crypto.createHash("sha256").update(q).digest("hex");
  const cached = judgeCache.get(key);
  if (cached && cached.expires > Date.now()) return cached.tier;
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 4000);
    const r = await fetch(ANTHROPIC_URL, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": process.env.ANTHROPIC_API_KEY,
        "anthropic-version": process.env.ANTHROPIC_VERSION || "2023-06-01",
      },
      body: JSON.stringify({
        model: cfg.autoRouter.judgeModel || "claude-haiku-4-5",
        max_tokens: 5,
        messages: [{
          role: "user",
          content: "Rate the difficulty of this AI task. Reply with ONLY one digit.\n1 = simple (summarize, translate, classify, extract, short factual answer)\n2 = medium (drafting, multi-step reasoning, moderate analysis)\n3 = hard (coding, deep analysis, architecture, long or high-stakes output)\n\nTask:\n" + q,
        }],
      }),
      signal: controller.signal,
    });
    clearTimeout(timer);
    if (!r.ok) return 2;
    const data = await r.json();
    const m = String(data?.content?.[0]?.text ?? "").match(/[123]/);
    const tier = m ? Number(m[0]) : 2;
    judgeCache.set(key, { tier, expires: Date.now() + 3600_000 });
    if (judgeCache.size > 500) judgeCache.delete(judgeCache.keys().next().value);
    return tier;
  } catch {
    return 2; // judge unavailable — assume medium, downgrade rules still apply
  }
}

function recordRoute(entry) {
  routerLog.unshift({ at: new Date().toISOString(), ...entry });
  if (routerLog.length > 100) routerLog.pop();
}

async function autoRoute(body, client, taskType) {
  const mode = cfg.autoRouter?.mode ?? "off";
  if (mode === "off" || !body?.model) return null;
  if ((body.messages?.length ?? 0) > 2) return null; // never switch mid-conversation
  const requested = body.model;
  // Only route models we have pricing for — an unknown ID may be a brand-new
  // model, and silently swapping it would corrupt the caller's intent.
  if (!pricingFor(requested)) return null;

  let { tier, confident } = heuristicTier(body);
  let decidedBy = "rules";
  if (!confident && mode === "smart") {
    tier = await judgeTier(body);
    decidedBy = "judge";
  }

  let target = TIER_MODELS[tier] ?? null;
  if (tier === 3 && cfg.autoRouter.allowUpgrade && modelRank("claude-opus-4-8") > modelRank(requested)) {
    target = "claude-opus-4-8";
  }
  if (!target) return null;
  if (String(requested).startsWith(target)) return null; // already on the right model
  if (!cfg.autoRouter.allowUpgrade && modelRank(target) > modelRank(requested)) return null;

  body.model = target;
  recordRoute({ client, task: taskType, from: requested, to: target, tier, decidedBy });
  return { requested, routed: target, tier, decidedBy };
}

// A cascade-worthy weak answer: an outright refusal or no usable text at all.
function isWeakAnswer(data) {
  if (data?.stop_reason === "refusal") return true;
  const hasText = Array.isArray(data?.content) && data.content.some((b) => b?.type === "text" && b.text?.trim());
  return !hasText;
}

// ---------------------------------------------------------------------------
// Reliability: retry transient upstream failures with backoff, and optionally
// fail over text-only requests to a backup model on another provider.
// ---------------------------------------------------------------------------
const RETRYABLE = new Set([429, 500, 502, 503, 529]);

async function fetchWithRetry(url, init) {
  const attempts = 1 + Math.min(5, Math.max(0, Number(cfg.reliability?.retries ?? 2)));
  let lastErr = null;
  for (let i = 0; i < attempts; i++) {
    try {
      const r = await fetch(url, init);
      if (!RETRYABLE.has(r.status) || i === attempts - 1) return r;
      const retryAfter = Number(r.headers.get("retry-after"));
      const delay = Math.min(5000, retryAfter > 0 ? retryAfter * 1000 : 800 * 2 ** i);
      await new Promise((s) => setTimeout(s, delay));
    } catch (err) {
      lastErr = err;
      if (i === attempts - 1) throw err;
      await new Promise((s) => setTimeout(s, 800 * 2 ** i));
    }
  }
  throw lastErr ?? new Error("unreachable");
}

// Flattens an Anthropic-shaped request to plain text turns, or null when the
// request uses tools/images/documents (failover only handles plain text).
function textOnly(body) {
  if (Array.isArray(body?.tools) && body.tools.length) return null;
  const out = [];
  const sys = typeof body?.system === "string"
    ? body.system
    : Array.isArray(body?.system) ? body.system.map((b) => b?.text ?? "").join("\n") : null;
  if (sys) out.push({ role: "system", text: sys });
  for (const m of body?.messages ?? []) {
    if (m.role !== "user" && m.role !== "assistant") return null;
    let text;
    if (typeof m.content === "string") text = m.content;
    else if (Array.isArray(m.content)) {
      if (m.content.some((b) => b?.type && b.type !== "text")) return null;
      text = m.content.map((b) => b?.text ?? "").join("\n");
    } else return null;
    out.push({ role: m.role, text });
  }
  return out.length ? out : null;
}

async function failoverCall(body, msgs) {
  const target = String(cfg.reliability?.failoverModel ?? "").trim();
  if (!target) return null;

  if (target.startsWith("gpt")) {
    if (isPlaceholder(process.env.OPENAI_API_KEY)) return null;
    const r = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer " + process.env.OPENAI_API_KEY },
      body: JSON.stringify({
        model: target,
        max_completion_tokens: body.max_tokens ?? 1024,
        messages: msgs.map((m) => ({ role: m.role, content: m.text })),
      }),
    });
    if (!r.ok) return null;
    const d = await r.json();
    const norm = normalizeOpenAi(d);
    return {
      provider: "openai",
      data: {
        id: d.id ?? "failover", type: "message", role: "assistant", model: d.model ?? target,
        content: [{ type: "text", text: d.choices?.[0]?.message?.content ?? "" }],
        stop_reason: "end_turn", stop_sequence: null,
        usage: {
          input_tokens: norm.usage.input_tokens,
          output_tokens: norm.usage.output_tokens,
          cache_creation_input_tokens: 0,
          cache_read_input_tokens: norm.usage.cache_read_input_tokens,
        },
      },
    };
  }

  if (target.startsWith("gemini")) {
    if (isPlaceholder(process.env.GEMINI_API_KEY)) return null;
    const sys = msgs.find((m) => m.role === "system");
    const reqBody = {
      contents: msgs.filter((m) => m.role !== "system")
        .map((m) => ({ role: m.role === "assistant" ? "model" : "user", parts: [{ text: m.text }] })),
      generationConfig: { maxOutputTokens: body.max_tokens ?? 1024 },
    };
    if (sys) reqBody.systemInstruction = { parts: [{ text: sys.text }] };
    const r = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${target}:generateContent`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-goog-api-key": process.env.GEMINI_API_KEY },
      body: JSON.stringify(reqBody),
    });
    if (!r.ok) return null;
    const d = await r.json();
    const norm = normalizeGemini(d, target);
    return {
      provider: "gemini",
      data: {
        id: norm.id ?? "failover", type: "message", role: "assistant", model: norm.model,
        content: [{ type: "text", text: d.candidates?.[0]?.content?.parts?.map((p) => p.text ?? "").join("") ?? "" }],
        stop_reason: "end_turn", stop_sequence: null,
        usage: {
          input_tokens: norm.usage.input_tokens,
          output_tokens: norm.usage.output_tokens,
          cache_creation_input_tokens: 0,
          cache_read_input_tokens: 0,
        },
      },
    };
  }

  return null;
}

// Answers the caller in Anthropic shape via the backup model. Returns the
// response data when it handled the request, null otherwise.
async function tryFailover(body, client, taskType, startedAt, res, isStream) {
  try {
    if (isStream) return null;
    const msgs = textOnly(body);
    if (!msgs) return null;
    const result = await failoverCall(body, msgs);
    if (!result) return null;
    res.json(result.data);
    recordRoute({ client, task: taskType, from: body.model, to: result.data.model, tier: 0, decidedBy: "failover" });
    logUsage({ data: result.data, client, taskType, latencyMs: Date.now() - startedAt, provider: result.provider });
    return result.data;
  } catch (err) {
    console.error("failover failed:", err.message ?? err);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Feature 2: repeat-answer cache.
// Byte-identical requests (ignoring `stream`/`metadata`) within the TTL are
// answered locally — zero tokens spent. Non-streaming only.
// ---------------------------------------------------------------------------
const respCache = new Map(); // key -> { expires, data }
const inflight = new Map();  // key -> Promise<{status, ok, data}> for identical concurrent requests

function respCacheKey(body) {
  const { stream, metadata, ...rest } = body ?? {};
  return crypto.createHash("sha256").update(JSON.stringify(rest)).digest("hex");
}

function respCacheGet(key) {
  const hit = respCache.get(key);
  if (!hit) return null;
  if (Date.now() > hit.expires) {
    respCache.delete(key);
    return null;
  }
  return hit.data;
}

function respCachePut(key, data) {
  const max = Number(cfg.responseCache.maxEntries) || 500;
  while (respCache.size >= max) respCache.delete(respCache.keys().next().value);
  respCache.set(key, {
    expires: Date.now() + (Number(cfg.responseCache.ttlSeconds) || 3600) * 1000,
    data,
  });
}

// ---------------------------------------------------------------------------
// Feature 3: per-client monthly budgets.
// Month-to-date spend is loaded from storage at startup, tracked in memory per
// request, and re-synced from storage every 10 minutes.
// ---------------------------------------------------------------------------
const spend = {
  monthKey: "", byClient: new Map(),
  todayKey: "", todayUsd: 0,
  refreshedAt: 0, dailyAlerted: false, blockedNotified: new Set(),
};

const monthKeyNow = () => new Date().toISOString().slice(0, 7);
const dayKeyNow = () => new Date().toISOString().slice(0, 10);

function rollover() {
  if (spend.monthKey !== monthKeyNow()) {
    spend.monthKey = monthKeyNow();
    spend.byClient = new Map();
    spend.blockedNotified = new Set();
  }
  if (spend.todayKey !== dayKeyNow()) {
    spend.todayKey = dayKeyNow();
    spend.todayUsd = 0;
    spend.dailyAlerted = false;
  }
}

async function refreshSpend() {
  if (!storage) return;
  try {
    rollover();
    const rows = await storage.daily(120);
    const byClient = new Map();
    let todayUsd = 0;
    for (const d of rows) {
      const dayIso = new Date(d.day).toISOString();
      const cost = (Number(d.avg_est_cost) || 0) * Number(d.requests || 0);
      if (dayIso.slice(0, 7) === spend.monthKey) {
        const c = d.client ?? "untagged";
        byClient.set(c, (byClient.get(c) ?? 0) + cost);
      }
      if (dayIso.slice(0, 10) === spend.todayKey) todayUsd += cost;
    }
    spend.byClient = byClient;
    spend.todayUsd = todayUsd;
    spend.refreshedAt = Date.now();
  } catch (err) {
    console.error("spend refresh failed:", err.message ?? err);
  }
}

function trackSpend(client, estCost) {
  rollover();
  const c = client ?? "untagged";
  spend.byClient.set(c, (spend.byClient.get(c) ?? 0) + estCost);
  spend.todayUsd += estCost;

  const threshold = Number(cfg.alerts.dailyUsdThreshold) || 0;
  if (threshold > 0 && spend.todayUsd >= threshold && !spend.dailyAlerted) {
    spend.dailyAlerted = true;
    sendAlert(`⚠️ SixVM Token Proxy: today's Claude spend reached $${spend.todayUsd.toFixed(4)} (alert threshold $${threshold}).`);
  }
  if (Date.now() - spend.refreshedAt > 10 * 60_000) refreshSpend();
}

function overBudget(client) {
  rollover();
  const c = client ?? "untagged";
  const limit = Number(cfg.budgets?.[c]?.monthlyUsd ?? cfg.budgets?.["*"]?.monthlyUsd);
  if (!(limit > 0)) return null;
  const used = spend.byClient.get(c) ?? 0;
  return used >= limit ? { limit, used } : null;
}

// ---------------------------------------------------------------------------
// Feature 4: webhook alerts (Discord or Slack compatible).
// ---------------------------------------------------------------------------
async function sendAlert(text) {
  const url = cfg.alerts.webhookUrl;
  if (!url) return;
  const payload = url.includes("discord.com") ? { content: text } : { text };
  try {
    await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (err) {
    console.error("alert webhook failed:", err.message ?? err);
  }
}

// Weekly report: every Monday from 9am local, post a 7-day summary to the
// webhook (once per Monday; the sent-marker persists in config.json).
async function weeklySummary() {
  if (!cfg.alerts?.webhookUrl || cfg.alerts?.weeklyReport === false || !storage) return;
  const now = new Date();
  if (now.getDay() !== 1 || now.getHours() < 9) return;
  const key = now.toISOString().slice(0, 10);
  if (cfg.lastWeeklyReport === key) return;
  try {
    const rows = await storage.daily(60);
    const cut = Date.now() - 7 * 86400000;
    let req = 0, cost = 0, tin = 0, tout = 0, baseline = 0;
    const byClient = new Map();
    for (const d of rows) {
      if (new Date(d.day).getTime() < cut) continue;
      const c = (Number(d.avg_est_cost) || 0) * Number(d.requests || 0);
      req += Number(d.requests || 0);
      cost += c;
      tin += Number(d.input_tokens || 0);
      tout += Number(d.output_tokens || 0);
      const p = pricingFor(d.model);
      if (p) {
        baseline += ((Number(d.input_tokens || 0) + Number(d.cache_read_tokens || 0) + Number(d.cache_write_tokens || 0)) * p.input
          + Number(d.output_tokens || 0) * p.output) / 1_000_000;
      }
      const name = d.client ?? "untagged";
      byClient.set(name, (byClient.get(name) ?? 0) + c);
    }
    const top = [...byClient.entries()].sort((a, b) => b[1] - a[1])[0];
    const saved = Math.max(0, baseline - cost);
    await sendAlert(
      "📊 SixVM Token Proxy · weekly report\n" +
      `Last 7 days: ${req.toLocaleString()} requests · $${cost.toFixed(4)} spent · ${Math.round(tin / 1000)}k in / ${Math.round(tout / 1000)}k out tokens\n` +
      `Saved vs full price: $${saved.toFixed(4)}` +
      (top ? `\nTop client: ${top[0]} ($${top[1].toFixed(4)})` : ""),
    );
    cfg.lastWeeklyReport = key;
    saveConfig(cfg);
  } catch (err) {
    console.error("weekly report failed:", err.message ?? err);
  }
}
setInterval(weeklySummary, 60 * 60 * 1000);
setTimeout(weeklySummary, 15_000);

// Request explorer: opt-in in-memory capture of prompts & answers, keyed by
// request id. Never persisted to disk or reported anywhere.
const captures = new Map();
function captureRequest(body, data, client, taskType) {
  if (!cfg.capture?.enabled || !data?.id) return;
  try {
    const toText = (c) => typeof c === "string" ? c
      : Array.isArray(c) ? c.map((b) => b?.text ?? "[" + (b?.type ?? "block") + "]").join("\n") : "";
    const lastUser = [...(body?.messages ?? [])].reverse().find((m) => m.role === "user");
    const sys = typeof body?.system === "string" ? body.system
      : Array.isArray(body?.system) ? body.system.map((b) => b?.text ?? "").join("\n") : "";
    captures.set(data.id, {
      id: data.id,
      at: new Date().toISOString(),
      client, task: taskType, model: data.model,
      system: sys.slice(0, 2000),
      prompt: toText(lastUser?.content).slice(0, 6000),
      answer: (Array.isArray(data.content) ? data.content.filter((b) => b.type === "text").map((b) => b.text).join("\n") : "").slice(0, 6000),
    });
    const max = Number(cfg.capture.maxEntries) || 100;
    while (captures.size > max) captures.delete(captures.keys().next().value);
  } catch { /* capture must never break a request */ }
}

// ---------------------------------------------------------------------------
// HTTP surface
// ---------------------------------------------------------------------------
const app = express();
app.disable("x-powered-by");

// Security: the proxy binds to 127.0.0.1 by default, so only this machine can
// reach it (anyone who can reach the proxy can spend the configured API keys).
// To allow agents on other machines, set HOST=0.0.0.0 in .env — then put the
// proxy behind a firewall or VPN.
const HOST = process.env.HOST || "127.0.0.1";

// DNS-rebinding protection: browsers can be tricked into sending requests to
// 127.0.0.1 under an attacker-controlled hostname. Only accept local hostnames
// unless the user explicitly exposed the proxy via HOST.
const LOCAL_HOSTNAMES = new Set(["localhost", "127.0.0.1", "[::1]"]);
app.use((req, res, next) => {
  if (HOST !== "127.0.0.1" && HOST !== "localhost") return next(); // user chose to expose it
  const hostname = String(req.headers.host ?? "").toLowerCase().replace(/:\d+$/, "");
  if (LOCAL_HOSTNAMES.has(hostname)) return next();
  res.status(403).json({ error: "Rejected: unexpected Host header" });
});

// Anthropic accepts request bodies up to 32 MB (base64 PDFs/images), so the
// proxy must not reject them first.
app.use(express.json({ limit: "50mb" }));

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.post("/v1/messages", async (req, res) => {
  const startedAt = Date.now();

  // SixVM-only tagging headers — read here, never forwarded to Anthropic.
  const client = req.get("x-sixvm-client") ?? null;
  const taskType = req.get("x-sixvm-task") ?? null;
  const isStream = req.body?.stream === true;

  if (isPlaceholder(process.env.ANTHROPIC_API_KEY)) {
    return res.status(500).json({
      type: "error",
      error: { type: "proxy_config_error", message: "ANTHROPIC_API_KEY is not set — open /setup in a browser" },
    });
  }

  // x-sixvm-raw: 1 disables routing and answer-caching for this request —
  // the exact requested model runs. Used by the Compare page (comparing two
  // models is meaningless if the router rewrites them) and available to any
  // caller that needs guaranteed-untouched behavior. Budgets still apply.
  const rawMode = req.get("x-sixvm-raw") === "1";

  // Apply per-task routing/guards, the auto-router, and auto-caching before
  // anything else, so the cache key reflects the request that actually
  // reaches Anthropic. Manual per-task rules win over the auto-router.
  const manuallyRouted = rawMode ? false : applyRoutingAndLimits(req.body, taskType);
  const routeInfo = (rawMode || manuallyRouted) ? null : await autoRoute(req.body, client, taskType);
  maybeAddCacheControl(req.body);

  // Repeat-answer cache: a hit costs nothing, so it is checked before budgets.
  let cacheKey = null;
  if (!rawMode && cfg.responseCache.enabled && !isStream) {
    cacheKey = respCacheKey(req.body);
    const hit = respCacheGet(cacheKey);
    if (hit) {
      res.json(hit);
      logUsage({ data: hit, client, taskType, latencyMs: Date.now() - startedAt, cacheHit: true });
      return;
    }
    // In-flight dedup: the same request is already on its way to Anthropic —
    // share that answer instead of paying for a second identical call.
    const pending = inflight.get(cacheKey);
    if (pending) {
      const shared = await pending;
      res.status(shared.status).json(shared.data);
      if (shared.ok) {
        logUsage({ data: shared.data, client, taskType, latencyMs: Date.now() - startedAt, cacheHit: true });
      }
      return;
    }
  }

  // Budget guard.
  const over = overBudget(client);
  if (over) {
    const c = client ?? "untagged";
    if (!spend.blockedNotified.has(c)) {
      spend.blockedNotified.add(c);
      sendAlert(`⛔ SixVM Token Proxy: "${c}" hit its monthly budget of $${over.limit} — its requests are now blocked.`);
    }
    return res.status(429).json({
      type: "error",
      error: {
        type: "budget_exceeded",
        message: `Monthly budget of $${over.limit} for "${c}" is used up ($${over.used.toFixed(6)} spent). Raise or remove the limit on the proxy's /setup page.`,
      },
    });
  }

  // Register this request as the in-flight primary so identical concurrent
  // requests can wait for its answer instead of calling Anthropic again.
  let settleInflight = () => {};
  if (cacheKey) {
    let resolve;
    inflight.set(cacheKey, new Promise((r) => { resolve = r; }));
    settleInflight = (result) => {
      inflight.delete(cacheKey);
      resolve(result);
    };
  }

  // Headers are built explicitly, so the x-sixvm-* headers are stripped by
  // construction. anthropic-beta is passed through so callers can use beta features.
  const headers = {
    "content-type": "application/json",
    "x-api-key": process.env.ANTHROPIC_API_KEY,
    "anthropic-version": process.env.ANTHROPIC_VERSION || "2023-06-01",
  };
  if (req.get("anthropic-beta")) headers["anthropic-beta"] = req.get("anthropic-beta");

  let anthropicRes;
  try {
    anthropicRes = await fetchWithRetry(ANTHROPIC_URL, {
      method: "POST",
      headers,
      body: JSON.stringify(req.body),
    });
  } catch (err) {
    console.error("anthropic request failed:", err);
    recordError({ provider: "anthropic", client, task: taskType, status: 502, message: "network: failed to reach Anthropic" });
    const failover = await tryFailover(req.body, client, taskType, startedAt, res, isStream);
    if (failover) {
      settleInflight({ status: 200, ok: true, data: failover });
      return;
    }
    const errBody = {
      type: "error",
      error: { type: "upstream_error", message: "Failed to reach Anthropic" },
    };
    settleInflight({ status: 502, ok: false, data: errBody });
    return res.status(502).json(errBody);
  }

  // Streaming: pipe the SSE bytes straight through, untouched. A copy is
  // accumulated on the side and parsed after the stream ends to log usage —
  // this never delays or alters what the caller receives.
  if (isStream) {
    res.status(anthropicRes.status);
    res.set("content-type", anthropicRes.headers.get("content-type") ?? "text/event-stream");
    const upstream = Readable.fromWeb(anthropicRes.body);
    let sseText = "";
    upstream.on("data", (chunk) => {
      // 20 MB guard so a runaway stream can't exhaust memory; oversized
      // streams skip logging but still pass through fine.
      if (sseText.length < 20_000_000) sseText += chunk.toString("utf8");
    });
    upstream.on("end", () => {
      if (!anthropicRes.ok) return;
      try {
        const data = parseSseUsage(sseText);
        if (data) logUsage({ data, client, taskType, latencyMs: Date.now() - startedAt });
      } catch (err) {
        console.error("stream usage logging failed:", err);
      }
    });
    upstream.pipe(res);
    return;
  }

  let data;
  try {
    data = await anthropicRes.json();
  } catch (err) {
    console.error("failed to parse anthropic response:", err);
    const errBody = {
      type: "error",
      error: { type: "upstream_error", message: "Invalid response from Anthropic" },
    };
    settleInflight({ status: 502, ok: false, data: errBody });
    return res.status(502).json(errBody);
  }
  // Cascade (Level 3): a routed-down request that produced a weak answer is
  // retried once on the originally requested model, and the caller gets the
  // stronger answer. The cheap attempt is still logged (it was billed).
  if (anthropicRes.ok && routeInfo && cfg.autoRouter.cascade && isWeakAnswer(data)) {
    try {
      const retryRes = await fetch(ANTHROPIC_URL, {
        method: "POST",
        headers,
        body: JSON.stringify({ ...req.body, model: routeInfo.requested }),
      });
      if (retryRes.ok) {
        const retryData = await retryRes.json();
        logUsage({ data, client, taskType, latencyMs: Date.now() - startedAt }); // cheap attempt
        recordRoute({ client, task: taskType, from: routeInfo.routed, to: routeInfo.requested, tier: 3, decidedBy: "cascade" });
        data = retryData;
        anthropicRes = retryRes;
      }
    } catch (err) {
      console.error("cascade retry failed:", err.message ?? err); // keep the cheap answer
    }
  }

  // Failover: retries are exhausted and the error is transient — answer via
  // the configured backup model instead of failing the caller.
  if (!anthropicRes.ok && RETRYABLE.has(anthropicRes.status)) {
    recordError({
      provider: "anthropic", client, task: taskType,
      status: anthropicRes.status,
      message: (data?.error?.message ?? "transient error") + " (after retries)",
    });
    const failover = await tryFailover(req.body, client, taskType, startedAt, res, false);
    if (failover) {
      settleInflight({ status: 200, ok: true, data: failover });
      return;
    }
  }

  const latencyMs = Date.now() - startedAt;

  // Return Anthropic's response (including error responses) unmodified,
  // then log asynchronously without blocking the caller.
  res.status(anthropicRes.status).json(data);
  settleInflight({ status: anthropicRes.status, ok: anthropicRes.ok, data });

  if (anthropicRes.ok) {
    if (cacheKey) respCachePut(cacheKey, data);
    logUsage({ data, client, taskType, latencyMs });
    captureRequest(req.body, data, client, taskType);
  } else {
    recordError({
      provider: "anthropic", client, task: taskType,
      status: anthropicRes.status,
      message: data?.error?.message ?? data?.error?.type ?? "error",
    });
  }
});

// Live feed for the dashboard (SSE). No replay; the dashboard re-fetches on ping.
app.get("/dashboard/stream", (req, res) => {
  res.set({ "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
  res.flushHeaders?.();
  res.write("data: {\"hello\":true}\n\n");
  liveClients.add(res);
  const heartbeat = setInterval(() => {
    try { res.write(": hb\n\n"); } catch { /* closing */ }
  }, 25000);
  req.on("close", () => {
    clearInterval(heartbeat);
    liveClients.delete(res);
  });
});

// ---------------------------------------------------------------------------
// OpenAI (ChatGPT) pass-through — agents set OPENAI_BASE_URL to this proxy.
// v1 scope for non-Anthropic providers: pass-through + usage logging + budget
// guard. (Auto-cache/router/repeat-cache are Anthropic-only for now.)
// ---------------------------------------------------------------------------
function normalizeOpenAi(data) {
  const u = data?.usage ?? {};
  const cached = u.prompt_tokens_details?.cached_tokens ?? 0;
  return {
    id: data?.id ?? null,
    model: data?.model ?? null,
    usage: {
      input_tokens: Math.max(0, (u.prompt_tokens ?? 0) - cached),
      output_tokens: u.completion_tokens ?? 0,
      cache_read_input_tokens: cached,
      cache_creation_input_tokens: 0,
    },
  };
}

function parseOpenAiSse(sseText) {
  let id = null, model = null, usage = null;
  for (const line of sseText.split("\n")) {
    if (!line.startsWith("data:")) continue;
    const payload = line.slice(5).trim();
    if (payload === "[DONE]") continue;
    try {
      const evt = JSON.parse(payload);
      id ??= evt.id;
      model ??= evt.model;
      if (evt.usage) usage = evt.usage;
    } catch { /* partial chunk */ }
  }
  if (!usage) return null;
  return normalizeOpenAi({ id, model, usage });
}

app.post("/v1/chat/completions", async (req, res) => {
  const startedAt = Date.now();
  const client = req.get("x-sixvm-client") ?? null;
  const taskType = req.get("x-sixvm-task") ?? null;
  const isStream = req.body?.stream === true;

  if (isPlaceholder(process.env.OPENAI_API_KEY)) {
    return res.status(500).json({
      error: { type: "proxy_config_error", message: "OPENAI_API_KEY is not set — open /setup in a browser" },
    });
  }

  const over = overBudget(client);
  if (over) {
    return res.status(429).json({
      error: {
        type: "budget_exceeded",
        message: `Monthly budget of $${over.limit} for "${client ?? "untagged"}" is used up ($${over.used.toFixed(6)} spent). Raise or remove the limit on the proxy's /setup page.`,
      },
    });
  }

  // Ask OpenAI to include usage in the final stream chunk so it can be logged.
  if (isStream) req.body.stream_options = { include_usage: true, ...(req.body.stream_options ?? {}) };

  let upstream;
  try {
    upstream = await fetchWithRetry("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: "Bearer " + process.env.OPENAI_API_KEY,
      },
      body: JSON.stringify(req.body),
    });
  } catch (err) {
    console.error("openai request failed:", err);
    recordError({ provider: "openai", client, task: taskType, status: 502, message: "network: failed to reach OpenAI" });
    return res.status(502).json({ error: { type: "upstream_error", message: "Failed to reach OpenAI" } });
  }

  if (isStream) {
    res.status(upstream.status);
    res.set("content-type", upstream.headers.get("content-type") ?? "text/event-stream");
    const s = Readable.fromWeb(upstream.body);
    let sseText = "";
    s.on("data", (chunk) => { if (sseText.length < 20_000_000) sseText += chunk.toString("utf8"); });
    s.on("end", () => {
      if (!upstream.ok) return;
      try {
        const norm = parseOpenAiSse(sseText);
        if (norm) logUsage({ data: norm, client, taskType, latencyMs: Date.now() - startedAt, provider: "openai" });
      } catch (err) {
        console.error("openai stream logging failed:", err);
      }
    });
    s.pipe(res);
    return;
  }

  let data;
  try {
    data = await upstream.json();
  } catch {
    return res.status(502).json({ error: { type: "upstream_error", message: "Invalid response from OpenAI" } });
  }
  res.status(upstream.status).json(data);
  if (upstream.ok) {
    logUsage({ data: normalizeOpenAi(data), client, taskType, latencyMs: Date.now() - startedAt, provider: "openai" });
  } else {
    recordError({ provider: "openai", client, task: taskType, status: upstream.status, message: data?.error?.message ?? "error" });
  }
});

// ---------------------------------------------------------------------------
// Google Gemini pass-through — point the SDK's apiEndpoint/base URL here.
// ---------------------------------------------------------------------------
function normalizeGemini(data, modelFromPath) {
  const u = data?.usageMetadata ?? {};
  const cached = u.cachedContentTokenCount ?? 0;
  return {
    id: data?.responseId ?? null,
    model: data?.modelVersion ?? modelFromPath,
    usage: {
      input_tokens: Math.max(0, (u.promptTokenCount ?? 0) - cached),
      output_tokens: (u.candidatesTokenCount ?? 0) + (u.thoughtsTokenCount ?? 0),
      cache_read_input_tokens: cached,
      cache_creation_input_tokens: 0,
    },
  };
}

app.post(/^\/(v1beta|v1)\/models\/[^/]+:(generateContent|streamGenerateContent)$/, async (req, res) => {
  const startedAt = Date.now();
  const client = req.get("x-sixvm-client") ?? null;
  const taskType = req.get("x-sixvm-task") ?? null;
  const isStream = req.path.includes(":streamGenerateContent");
  const modelFromPath = req.path.match(/models\/([^:]+):/)?.[1] ?? null;

  if (isPlaceholder(process.env.GEMINI_API_KEY)) {
    return res.status(500).json({
      error: { type: "proxy_config_error", message: "GEMINI_API_KEY is not set — open /setup in a browser" },
    });
  }

  const over = overBudget(client);
  if (over) {
    return res.status(429).json({
      error: {
        type: "budget_exceeded",
        message: `Monthly budget of $${over.limit} for "${client ?? "untagged"}" is used up ($${over.used.toFixed(6)} spent). Raise or remove the limit on the proxy's /setup page.`,
      },
    });
  }

  let upstream;
  try {
    upstream = await fetchWithRetry("https://generativelanguage.googleapis.com" + req.path + (isStream ? "?alt=sse" : ""), {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-goog-api-key": process.env.GEMINI_API_KEY,
      },
      body: JSON.stringify(req.body),
    });
  } catch (err) {
    console.error("gemini request failed:", err);
    recordError({ provider: "gemini", client, task: taskType, status: 502, message: "network: failed to reach Google Gemini" });
    return res.status(502).json({ error: { type: "upstream_error", message: "Failed to reach Google Gemini" } });
  }

  if (isStream) {
    res.status(upstream.status);
    res.set("content-type", upstream.headers.get("content-type") ?? "text/event-stream");
    const s = Readable.fromWeb(upstream.body);
    let sseText = "";
    s.on("data", (chunk) => { if (sseText.length < 20_000_000) sseText += chunk.toString("utf8"); });
    s.on("end", () => {
      if (!upstream.ok) return;
      try {
        // Gemini streams cumulative usageMetadata — the last chunk wins.
        let last = null;
        for (const line of sseText.split("\n")) {
          if (!line.startsWith("data:")) continue;
          try {
            const evt = JSON.parse(line.slice(5).trim());
            if (evt.usageMetadata) last = evt;
          } catch { /* partial chunk */ }
        }
        if (last) logUsage({ data: normalizeGemini(last, modelFromPath), client, taskType, latencyMs: Date.now() - startedAt, provider: "gemini" });
      } catch (err) {
        console.error("gemini stream logging failed:", err);
      }
    });
    s.pipe(res);
    return;
  }

  let data;
  try {
    data = await upstream.json();
  } catch {
    return res.status(502).json({ error: { type: "upstream_error", message: "Invalid response from Google Gemini" } });
  }
  res.status(upstream.status).json(data);
  if (upstream.ok) {
    logUsage({ data: normalizeGemini(data, modelFromPath), client, taskType, latencyMs: Date.now() - startedAt, provider: "gemini" });
  } else {
    recordError({ provider: "gemini", client, task: taskType, status: upstream.status, message: data?.error?.message ?? "error" });
  }
});

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------
app.get("/dashboard", (_req, res) => {
  res.sendFile(path.join(APP_DIR, "dashboard.html"));
});

app.get("/compare", (_req, res) => {
  res.sendFile(path.join(APP_DIR, "compare.html"));
});

app.get("/pricing", (_req, res) => res.json(PRICING));

app.get("/dashboard/capture", localOnly, (req, res) => {
  if (!cfg.capture?.enabled) {
    return res.status(404).json({ error: "Request explorer is off · turn it on in Setup → Optimizations & limits" });
  }
  const entry = captures.get(String(req.query.id ?? ""));
  if (!entry) {
    return res.status(404).json({ error: "Not captured · only requests made while the explorer is on are kept (last 100, memory only)" });
  }
  res.json(entry);
});

app.get("/dashboard/data", async (_req, res) => {
  if (!storage) {
    return res.status(503).json({ error: "No usage storage configured — open /setup" });
  }
  try {
    const recent = await storage.recent(200);
    const daily = await storage.daily(90);
    // pricing is included so the UI can compute the "without proxy" baseline.
    res.json({
      recent,
      daily,
      pricing: PRICING,
      router: { mode: cfg.autoRouter?.mode ?? "off", recent: routerLog.slice(0, 50) },
      errors: errorsLog.slice(0, 50),
    });
  } catch (err) {
    res.status(500).json({ error: err.message ?? String(err) });
  }
});

// ---------------------------------------------------------------------------
// First-run setup (/setup) — configure keys and features from the browser.
// Write access is restricted to the machine the proxy runs on.
// ---------------------------------------------------------------------------
function localOnly(req, res, next) {
  const ip = req.socket.remoteAddress;
  if (ip === "127.0.0.1" || ip === "::1" || ip === "::ffff:127.0.0.1") return next();
  res.status(403).json({ error: "Setup is only available from the machine running the proxy" });
}

app.get("/", (_req, res) => res.redirect("/dashboard"));

app.get("/setup", (_req, res) => {
  res.sendFile(path.join(APP_DIR, "setup.html"));
});

app.get("/help", (_req, res) => {
  res.sendFile(path.join(APP_DIR, "help.html"));
});

// ---------------------------------------------------------------------------
// Token Doctor — analyzes recent usage and reports where money leaks.
// Numbers are based on the last 200 requests ("recent traffic"), so findings
// are observations, not projections.
// ---------------------------------------------------------------------------
function diagnose(recent, daily) {
  const findings = [];
  const byClient = new Map();
  for (const r of recent) {
    const c = r.client ?? "untagged";
    const a = byClient.get(c) ?? {
      n: 0, tin: 0, tout: 0, cr: 0, cw: 0, cost: 0, inCost: 0,
      hits: 0, hitSavedUsd: 0, expensiveSmall: 0,
    };
    const p = pricingFor(r.model);
    a.n++;
    a.tin += r.input_tokens ?? 0;
    a.tout += r.output_tokens ?? 0;
    a.cr += r.cache_read_tokens ?? 0;
    a.cw += r.cache_write_tokens ?? 0;
    a.cost += Number(r.est_cost) || 0;
    if (p) a.inCost += ((r.input_tokens ?? 0) * p.input) / 1_000_000;
    if (r.cache_hit) {
      a.hits++;
      if (p) a.hitSavedUsd += ((r.input_tokens ?? 0) * p.input + (r.output_tokens ?? 0) * p.output) / 1_000_000;
    }
    const model = String(r.model ?? "");
    const premium = model.startsWith("claude-opus") || model.startsWith("claude-fable");
    if (premium && (r.input_tokens ?? 0) < 1000 && (r.output_tokens ?? 0) < 300) a.expensiveSmall++;
    byClient.set(c, a);
  }

  for (const [c, a] of byClient) {
    if (a.n < 5) continue; // too little traffic to judge

    if (a.tin / a.n > 2000 && a.cr === 0) {
      findings.push({
        severity: "warn",
        title: `"${c}" sends big prompts with zero cache hits`,
        detail: `Averages ${Math.round(a.tin / a.n).toLocaleString()} input tokens per request with no cache reads. If part of its prompt is stable between calls, prompt caching cuts that cost ~90%. Auto-cache handles it when the stable part stays byte-identical — check for timestamps or random IDs in the prompt.`,
        saveUsd: a.inCost * 0.8,
      });
    }
    if (a.cw > 0 && a.cr < a.cw * 0.5) {
      findings.push({
        severity: "warn",
        title: `"${c}" writes to the prompt cache but rarely reads it back`,
        detail: `${a.cw.toLocaleString()} tokens written vs ${a.cr.toLocaleString()} read. Cache writes cost 1.25× — paying the premium without the payoff. Usual causes: the prompt changes slightly every call, or calls are more than 5 minutes apart (cache expires).`,
      });
    }
    if (a.tout / a.n > 1500) {
      findings.push({
        severity: "info",
        title: `"${c}" produces long answers (${Math.round(a.tout / a.n).toLocaleString()} output tokens avg)`,
        detail: `Output tokens cost 5× more than input. If the answers are longer than needed, set a max-output cap or an effort level for its tasks in Setup → Advanced token savers.`,
      });
    }
    if (a.expensiveSmall / a.n > 0.5) {
      findings.push({
        severity: "info",
        title: `"${c}" uses a premium model for small tasks`,
        detail: `${a.expensiveSmall} of its last ${a.n} requests were small (under 1K in / 300 out) on an Opus/Fable-class model. Routing these tasks to Haiku would cost ~80–96% less — add a rule in Setup → Advanced token savers.`,
        saveUsd: a.cost * 0.7,
      });
    }
    if (a.hits > 0) {
      findings.push({
        severity: "good",
        title: `"${c}" got ${a.hits} free answers from the repeat cache`,
        detail: `Those requests cost $0 instead of ~$${a.hitSavedUsd.toFixed(4)}.`,
      });
    }
  }

  const untagged = byClient.get("untagged");
  if (untagged && untagged.n >= 3) {
    findings.push({
      severity: "info",
      title: `${untagged.n} recent requests have no client tag`,
      detail: `Add the x-sixvm-client header to every agent so the dashboard and budgets can tell them apart.`,
    });
  }

  // Overall cache health across the recent window.
  const tot = [...byClient.values()].reduce((s, a) => ({ tin: s.tin + a.tin, cr: s.cr + a.cr }), { tin: 0, cr: 0 });
  if (tot.tin + tot.cr > 0) {
    const ratio = tot.cr / (tot.tin + tot.cr);
    if (ratio > 0.3) {
      findings.push({
        severity: "good",
        title: `Healthy cache usage: ${(ratio * 100).toFixed(0)}% of prompt tokens came from cache`,
        detail: `Cached tokens bill at ~10% of the normal price — this is exactly what saving looks like.`,
      });
    }
  }

  if (!findings.length) {
    findings.push({
      severity: "good",
      title: "No leaks found in recent traffic",
      detail: recent.length < 10
        ? "Not much traffic yet — connect an agent and check back after a day of real use."
        : "Usage looks efficient. Check back as traffic grows.",
    });
  }

  const order = { warn: 0, info: 1, good: 2 };
  findings.sort((x, y) => order[x.severity] - order[y.severity]);
  return findings;
}

app.get("/doctor", (_req, res) => {
  res.sendFile(path.join(APP_DIR, "doctor.html"));
});

app.get("/doctor/data", async (_req, res) => {
  if (!storage) {
    return res.status(503).json({ error: "No usage storage configured — open /setup" });
  }
  try {
    const recent = await storage.recent(200);
    const daily = await storage.daily(120);
    res.json({ findings: diagnose(recent, daily), sampleSize: recent.length, generatedAt: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ error: err.message ?? String(err) });
  }
});

app.get("/setup/status", localOnly, async (_req, res) => {
  const healthy = storage ? await storage.healthy() : false;
  res.json({
    anthropic: !isPlaceholder(process.env.ANTHROPIC_API_KEY),
    openai: !isPlaceholder(process.env.OPENAI_API_KEY),
    gemini: !isPlaceholder(process.env.GEMINI_API_KEY),
    supabase: storage?.kind === "supabase",
    table: storage?.kind === "supabase" ? healthy : storage?.kind === "sqlite",
    logging: Boolean(storage) && healthy,
    storage: storage?.kind ?? "none",
    port: process.env.PORT || 8787,
  });
});

app.get("/setup/config", localOnly, (_req, res) => res.json(cfg));

// Object keys that would tamper with JS prototypes are never accepted.
const isSafeKey = (k) => k && !["__proto__", "constructor", "prototype"].includes(k);

app.post("/setup/config", localOnly, (req, res) => {
  const b = req.body ?? {};
  if (typeof b.autoCache === "boolean") cfg.autoCache = b.autoCache;
  if (b.responseCache && typeof b.responseCache === "object") {
    if (typeof b.responseCache.enabled === "boolean") cfg.responseCache.enabled = b.responseCache.enabled;
    const ttl = Number(b.responseCache.ttlSeconds);
    if (ttl >= 60) cfg.responseCache.ttlSeconds = ttl;
  }
  if (b.budgets && typeof b.budgets === "object") {
    const clean = {};
    for (const [name, v] of Object.entries(b.budgets)) {
      const usd = Number(v?.monthlyUsd ?? v);
      if (isSafeKey(name.trim()) && usd > 0) clean[name.trim()] = { monthlyUsd: usd };
    }
    cfg.budgets = clean;
  }
  if (b.alerts && typeof b.alerts === "object") {
    if (typeof b.alerts.webhookUrl === "string") cfg.alerts.webhookUrl = b.alerts.webhookUrl.trim();
    const th = Number(b.alerts.dailyUsdThreshold);
    if (!Number.isNaN(th)) cfg.alerts.dailyUsdThreshold = Math.max(0, th);
    if (typeof b.alerts.weeklyReport === "boolean") cfg.alerts.weeklyReport = b.alerts.weeklyReport;
  }
  if (b.capture && typeof b.capture === "object") {
    if (typeof b.capture.enabled === "boolean") cfg.capture.enabled = b.capture.enabled;
  }
  if (b.routing && typeof b.routing === "object") {
    const clean = {};
    for (const [task, model] of Object.entries(b.routing)) {
      if (isSafeKey(task.trim()) && typeof model === "string" && model.trim()) clean[task.trim()] = model.trim();
    }
    cfg.routing = clean;
  }
  if (b.limits && typeof b.limits === "object") {
    const cap = Number(b.limits.maxOutputTokens);
    if (!Number.isNaN(cap)) cfg.limits.maxOutputTokens = Math.max(0, Math.floor(cap));
    if (b.limits.taskEffort && typeof b.limits.taskEffort === "object") {
      const clean = {};
      for (const [task, effort] of Object.entries(b.limits.taskEffort)) {
        if (isSafeKey(task.trim()) && ["low", "medium", "high"].includes(effort)) clean[task.trim()] = effort;
      }
      cfg.limits.taskEffort = clean;
    }
  }
  if (b.autoRouter && typeof b.autoRouter === "object") {
    if (["off", "rules", "smart"].includes(b.autoRouter.mode)) cfg.autoRouter.mode = b.autoRouter.mode;
    if (typeof b.autoRouter.allowUpgrade === "boolean") cfg.autoRouter.allowUpgrade = b.autoRouter.allowUpgrade;
    if (typeof b.autoRouter.cascade === "boolean") cfg.autoRouter.cascade = b.autoRouter.cascade;
  }
  if (b.reliability && typeof b.reliability === "object") {
    const rt = Number(b.reliability.retries);
    if (!Number.isNaN(rt)) cfg.reliability.retries = Math.max(0, Math.min(5, Math.floor(rt)));
    if (typeof b.reliability.failoverModel === "string") cfg.reliability.failoverModel = b.reliability.failoverModel.trim();
  }
  saveConfig(cfg);
  res.json({ ok: true, config: cfg });
});

app.post("/setup/save", localOnly, async (req, res) => {
  const { anthropicKey, openaiKey, geminiKey, supabaseUrl, supabaseKey, port } = req.body ?? {};

  // Each provided key is verified against its provider (free endpoints, no
  // tokens spent) before anything is saved.
  if (anthropicKey) {
    try {
      const r = await fetch("https://api.anthropic.com/v1/models?limit=1", {
        headers: {
          "x-api-key": anthropicKey,
          "anthropic-version": process.env.ANTHROPIC_VERSION || "2023-06-01",
        },
      });
      if (!r.ok) {
        return res.json({ ok: false, field: "anthropicKey", message: "Anthropic rejected this key (HTTP " + r.status + ")" });
      }
    } catch {
      return res.json({ ok: false, field: "anthropicKey", message: "Could not reach Anthropic to verify the key" });
    }
  }

  if (openaiKey) {
    try {
      const r = await fetch("https://api.openai.com/v1/models", {
        headers: { authorization: "Bearer " + openaiKey },
      });
      if (!r.ok) {
        return res.json({ ok: false, field: "openaiKey", message: "OpenAI rejected this key (HTTP " + r.status + ")" });
      }
    } catch {
      return res.json({ ok: false, field: "openaiKey", message: "Could not reach OpenAI to verify the key" });
    }
  }

  if (geminiKey) {
    try {
      const r = await fetch("https://generativelanguage.googleapis.com/v1beta/models?pageSize=1", {
        headers: { "x-goog-api-key": geminiKey },
      });
      if (!r.ok) {
        return res.json({ ok: false, field: "geminiKey", message: "Google rejected this key (HTTP " + r.status + ")" });
      }
    } catch {
      return res.json({ ok: false, field: "geminiKey", message: "Could not reach Google to verify the key" });
    }
  }

  // Optional: the user's OWN Supabase project for cloud usage storage.
  // Connection is tested first; a missing token_usage table is fine (the
  // setup page then shows the SQL to run once in their SQL Editor).
  let tableMissing = false;
  if (supabaseUrl && supabaseKey) {
    try {
      const { createClient } = await import("@supabase/supabase-js");
      const test = createClient(supabaseUrl, supabaseKey);
      const { error } = await test.from("token_usage").select("id").limit(1);
      if (error) {
        const msg = String(error.message ?? "");
        if (error.code === "42P01" || error.code === "PGRST205" || msg.includes("token_usage")) {
          tableMissing = true;
        } else {
          return res.json({ ok: false, field: "supabaseKey", message: "Supabase rejected the connection: " + msg });
        }
      }
    } catch (err) {
      return res.json({ ok: false, field: "supabaseUrl", message: "Could not connect to Supabase: " + (err.message ?? err) });
    }
  }

  if (anthropicKey) process.env.ANTHROPIC_API_KEY = anthropicKey;
  if (openaiKey) process.env.OPENAI_API_KEY = openaiKey;
  if (geminiKey) process.env.GEMINI_API_KEY = geminiKey;
  if (supabaseUrl) process.env.SUPABASE_URL = supabaseUrl;
  if (supabaseKey) process.env.SUPABASE_SERVICE_KEY = supabaseKey;
  if (port) process.env.PORT = String(port);

  const envContent = [
    "ANTHROPIC_API_KEY=" + (process.env.ANTHROPIC_API_KEY ?? ""),
    "ANTHROPIC_VERSION=" + (process.env.ANTHROPIC_VERSION || "2023-06-01"),
    "OPENAI_API_KEY=" + (process.env.OPENAI_API_KEY ?? ""),
    "GEMINI_API_KEY=" + (process.env.GEMINI_API_KEY ?? ""),
    "SUPABASE_URL=" + (process.env.SUPABASE_URL ?? ""),
    "SUPABASE_SERVICE_KEY=" + (process.env.SUPABASE_SERVICE_KEY ?? ""),
    "PORT=" + (process.env.PORT || 8787),
    "",
  ].join("\n");
  fs.writeFileSync(path.join(APP_DIR, ".env"), envContent);

  storage = await initStorage();
  await refreshSpend();
  res.json({ ok: true, tableMissing });
});

// ---------------------------------------------------------------------------
// Generic pass-through for every other Anthropic endpoint (count_tokens,
// models, batches, ...) so SDKs and Claude Code pointed at this proxy via
// ANTHROPIC_BASE_URL work fully. Only /v1/messages (above) logs usage.
// ---------------------------------------------------------------------------
app.all("/v1/*", async (req, res) => {
  if (isPlaceholder(process.env.ANTHROPIC_API_KEY)) {
    return res.status(500).json({
      type: "error",
      error: { type: "proxy_config_error", message: "ANTHROPIC_API_KEY is not set — open /setup in a browser" },
    });
  }
  const headers = {
    "x-api-key": process.env.ANTHROPIC_API_KEY,
    "anthropic-version": process.env.ANTHROPIC_VERSION || "2023-06-01",
  };
  if (req.get("anthropic-beta")) headers["anthropic-beta"] = req.get("anthropic-beta");
  const init = { method: req.method, headers };
  if (req.method !== "GET" && req.method !== "HEAD") {
    headers["content-type"] = "application/json";
    init.body = JSON.stringify(req.body ?? {});
  }
  try {
    const upstream = await fetch("https://api.anthropic.com" + req.originalUrl, init);
    res.status(upstream.status);
    res.set("content-type", upstream.headers.get("content-type") ?? "application/json");
    if (upstream.body) Readable.fromWeb(upstream.body).pipe(res);
    else res.end();
  } catch (err) {
    console.error("anthropic passthrough failed:", err);
    res.status(502).json({
      type: "error",
      error: { type: "upstream_error", message: "Failed to reach Anthropic" },
    });
  }
});

// Best-effort browser opening (set SIXVM_NO_OPEN=1 to suppress, e.g. in tests).
function openBrowser(url) {
  if (process.env.SIXVM_NO_OPEN) return;
  try {
    const cmd = process.platform === "win32" ? `start "" "${url}"`
      : process.platform === "darwin" ? `open "${url}"`
      : `xdg-open "${url}"`;
    exec(cmd);
  } catch { /* opening a browser is a convenience, never a requirement */ }
}

async function boot() {
  // No .env yet = a brand-new install: guide the user straight to Setup.
  const firstRun = !fs.existsSync(path.join(APP_DIR, ".env"));
  storage = await initStorage();
  await refreshSpend();
  const server = app.listen(PORT, HOST, () => {
    console.log(`SixVM Token Proxy running · dashboard: http://localhost:${PORT}/dashboard`);
    console.log(`Keep this window open — closing it stops the proxy.`);
    if (firstRun) {
      console.log("First run detected · opening the setup page in your browser…");
      openBrowser(`http://localhost:${PORT}/setup`);
    }
  });
  server.on("error", (err) => {
    if (err.code === "EADDRINUSE") {
      console.log("");
      console.log(`SixVM Token Proxy is already running (port ${PORT} is in use).`);
      console.log("Opening the dashboard of the running copy — this window will close.");
      openBrowser(`http://localhost:${PORT}/dashboard`);
      setTimeout(() => process.exit(0), 8000);
    } else {
      console.error("Failed to start:", err.message ?? err);
      console.log("This window stays open so you can read the error. Press Ctrl+C to close.");
      // keep the console window alive on double-click so the message is readable
      setInterval(() => {}, 60_000);
    }
  });
}
boot();
