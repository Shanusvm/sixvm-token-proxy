# ✳ SixVM Token Proxy

A tiny, self-hosted proxy for **Claude (Anthropic), ChatGPT (OpenAI), and Gemini (Google)** that shows you exactly where your tokens and money go — per agent, per task, per provider, per day — on a clean local dashboard. With automatic money-savers built in.

Point your agents at the proxy instead of the provider's API. They work exactly the same; the proxy passes every request through (streaming included) and logs the usage numbers on the side.

```
Claude agents  ──► http://localhost:8787/v1/messages          ──► api.anthropic.com
ChatGPT agents ──► http://localhost:8787/v1/chat/completions  ──► api.openai.com
Gemini agents  ──► http://localhost:8787/v1beta/models/...    ──► generativelanguage.googleapis.com
                                  │
                                  └──► local usage log → one dashboard for everything
```

API keys are **never in the code** — you add them on the setup page in your browser, they're verified with the provider, and stored only in a local `.env` file on your machine.

## ⬇ Download & run (no coding needed)

**Windows:** [**Download the app**](https://github.com/Shanusvm/sixvm-token-proxy/releases/latest/download/SixVM-Token-Proxy-win64.zip) → unzip → double-click **SixVM-Token-Proxy.exe**.

The first run opens the setup page in your browser automatically — paste your API key and you're done. No Node.js, no install, no terminal. (Windows may show a SmartScreen warning the first time because the app is new/unsigned — click **More info → Run anyway**.)

Prefer to run from source instead? See [Quick start](#quick-start) below.

## Features

- **Drop-in** — same request/response shape as the Anthropic Messages API; streaming (SSE) passes straight through; all other `/v1/*` endpoints (`count_tokens`, `models`, …) are forwarded too, so SDKs pointed at the proxy via `ANTHROPIC_BASE_URL` just work
- **Per-agent tracking** — tag each caller with two headers (`x-sixvm-client`, `x-sixvm-task`); the proxy strips them before forwarding
- **Automatic prompt caching** — large system prompts *and* long conversation histories get Anthropic's `cache_control` marker added automatically, so repeated content bills at ~10% of the normal price (toggle in settings)
- **Repeat-answer cache + in-flight dedup** — byte-identical repeat requests are answered locally, and identical *concurrent* requests share one upstream call — zero extra tokens either way
- **Smart model routing** — send tagged tasks to a cheaper model (e.g. `summarize: claude-haiku-4-5`) with one rule per line
- **Auto-router** — the proxy judges each request's difficulty (instant heuristics, plus an optional tiny AI-judge call for unclear cases) and picks the cheapest capable model itself; optional cascade retries weak answers on the bigger model. Downgrade-only by default, never switches mid-conversation, every decision visible on the dashboard
- **Output guards** — a hard cap on `max_tokens` per request, and per-task `effort` levels that make the model think and write less on routine work
- **Per-client budgets** — set monthly USD caps; a client over budget gets a clear `budget_exceeded` error instead of spending more
- **Spend alerts** — Discord/Slack webhook message when daily spend crosses your threshold or a client hits its budget
- **Dashboard** at `/dashboard` — live stats, 4 charts (cost/day, cost/client, tokens/day, requests/task), time-range + client filters, CSV export, savings view
- **Token Doctor** at `/doctor` — analyzes your recent traffic and reports where money leaks (uncached big prompts, premium models on small tasks, output-heavy agents) with estimated savings
- **Works with zero database setup** — logs to a local SQLite file out of the box (Node 22.5+); connect a free Supabase project for a shared/cloud database
- **Browser setup** at `/setup` — paste your API key, keys verified before saving, all features configurable in the UI
- **Never breaks your agents** — logging is fire-and-forget; if the database is down, requests still pass through fine

## Quick start

Requires **Node.js 18+**.

```bash
git clone https://github.com/Shanusvm/sixvm-token-proxy.git
cd sixvm-token-proxy
npm install
npm start
```

Then open **http://localhost:8787/setup**, paste the API keys for the providers you use (Anthropic / OpenAI / Gemini — each optional, each verified before saving), and you're done — open **http://localhost:8787/dashboard** and click **Send test request**. No database setup needed: usage is logged to a local file automatically.

## Point your agents at the proxy

**OpenAI SDK / any OpenAI-compatible tool:**

```
OPENAI_BASE_URL=http://localhost:8787/v1
```

**Gemini SDK:** set the client's API endpoint / base URL to `http://localhost:8787`.

**Anthropic SDK (Node / Python):**

```js
const client = new Anthropic({
  apiKey: "not-needed",                  // the proxy holds the real key
  baseURL: "http://localhost:8787",
  defaultHeaders: {
    "x-sixvm-client": "my-agent",        // who is calling — shows on the dashboard
    "x-sixvm-task": "summarize",         // what kind of work
  },
});
```

**Anything that reads env vars (including Claude Code):**

```
ANTHROPIC_BASE_URL=http://localhost:8787
```

**Raw HTTP:** replace `https://api.anthropic.com/v1/messages` with `http://localhost:8787/v1/messages`.

## What gets logged — and data privacy

One row per request — client, task type, model, input/output tokens, cache tokens, estimated cost, request id, latency. This powers your dashboard. **Prompt and answer content is never stored anywhere, by design, and nothing is ever reported to SixVM or any third party** — your usage data lives only in your local database file or your own Supabase project.

## Optional: your own Supabase (cloud storage for usage data)

The proxy logs to a local SQLite file with zero setup. To keep usage data in the cloud instead (survives reinstalls, queryable from anywhere), connect **your own free [Supabase](https://supabase.com) project**:

1. supabase.com → **New project** (free plan is fine)
2. **Project Settings → API keys** → copy the **Project URL** and **service_role** key
3. Open the proxy's **Setup page → section 3 · Your usage database**, paste both, click **Test & Save**
4. Setup shows the SQL to run once (also in [`schema.sql`](schema.sql)) — paste it into Supabase's **SQL Editor**, Run, then Test & Save again

> **Note on prices:** the `PRICING` table in [`server.js`](server.js) holds per-million-token rates used for `est_cost`. Verify them against [Anthropic's current pricing](https://platform.claude.com/docs/en/pricing) before trusting the dollar figures.

## Configuration

Set via `/setup` in the browser (recommended), or edit `.env` (see [`.env.example`](.env.example)):

| Variable | Purpose |
|---|---|
| `ANTHROPIC_API_KEY` | Your Anthropic key (Claude requests) |
| `ANTHROPIC_VERSION` | Anthropic API version header (default `2023-06-01`) |
| `OPENAI_API_KEY` | Your OpenAI key (ChatGPT requests) |
| `GEMINI_API_KEY` | Your Google key (Gemini requests) |
| `PORT` | Proxy port (default `8787`) |

Feature settings (caching, budgets, router, alerts, routing rules) live in `config.json`, managed from the setup page. Note: the advanced savers (auto-caching, repeat cache, auto-router, effort control) currently apply to **Claude** requests; OpenAI/Gemini get pass-through + usage logging + budgets.

## Security notes

- `.env` is gitignored — your keys never leave the machine
- The `/setup` write endpoint only accepts requests from localhost
- If you deploy the proxy on a public server, put it behind a firewall or auth layer — anyone who can reach it can spend your Anthropic credits

## Roadmap

- [x] Automatic prompt caching (system prompts + conversation history)
- [x] Repeat-answer cache (exact match — skip Anthropic entirely for repeated questions)
- [x] In-flight deduplication of identical concurrent requests
- [x] Budget alerts / hard limits per client
- [x] Model routing per task tag
- [x] Auto-router (difficulty-based model selection: heuristics + AI judge + cascade)
- [x] Output-token guards (max_tokens cap, per-task effort)
- [x] Token Doctor (automatic leak analysis)
- [x] SQLite fallback (no database setup needed)
- [ ] Semantic similarity cache (embeddings — catch *near*-identical questions too)
- [ ] Prompt compression
- [ ] Batch API routing for non-urgent requests (50% off)

## License

[MIT](LICENSE) — built by SixVM IT Solutions.
