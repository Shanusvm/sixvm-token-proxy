// Usage storage with two interchangeable backends:
//   - Supabase (preferred when configured) — shared, queryable from anywhere
//   - Local SQLite file (data/usage.db) — zero-setup fallback, needs Node 22.5+
// Both expose: kind, insertUsage(row), recent(limit), daily(limit), healthy().
import fs from "node:fs";
import path from "node:path";
import { createClient } from "@supabase/supabase-js";
import { BASE_DIR } from "./paths.js";

// Treat the .env.example placeholders the same as "not configured".
export function isPlaceholder(v) {
  return !v || v.includes("your-key-here") || v.includes("your-project") || v.includes("your-service-role-key");
}

export async function initStorage() {
  if (!isPlaceholder(process.env.SUPABASE_URL) && !isPlaceholder(process.env.SUPABASE_SERVICE_KEY)) {
    return supabaseBackend();
  }
  const sqlite = await sqliteBackend();
  if (sqlite) {
    console.warn("Supabase not configured — logging to local SQLite file (data/usage.db) instead");
    return sqlite;
  }
  console.warn("No usage storage available — logging disabled (open /setup, or use Node 22.5+ for the SQLite fallback)");
  return null;
}

function supabaseBackend() {
  const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
  return {
    kind: "supabase",
    async insertUsage(row) {
      const { error } = await sb.from("token_usage").insert(row);
      if (error) throw new Error(error.message);
    },
    async recent(limit) {
      const { data, error } = await sb.from("token_usage").select("*").order("id", { ascending: false }).limit(limit);
      if (error) throw new Error(error.message);
      return data;
    },
    async daily(limit) {
      const { data, error } = await sb.from("token_usage_daily").select("*").order("day", { ascending: false }).limit(limit);
      if (error) throw new Error(error.message);
      return data;
    },
    async healthy() {
      const { error } = await sb.from("token_usage").select("id").limit(1);
      return !error;
    },
  };
}

async function sqliteBackend() {
  let DatabaseSync;
  try {
    ({ DatabaseSync } = await import("node:sqlite"));
  } catch {
    return null; // Node older than 22.5 has no built-in SQLite
  }
  const dir = path.join(BASE_DIR, "data");
  fs.mkdirSync(dir, { recursive: true });
  const db = new DatabaseSync(path.join(dir, "usage.db"));
  db.exec(`
    create table if not exists token_usage (
      id integer primary key autoincrement,
      created_at text not null,
      client text,
      task_type text,
      model text,
      input_tokens integer,
      output_tokens integer,
      cache_read_tokens integer,
      cache_write_tokens integer,
      cache_hit integer default 0,
      est_cost real,
      request_id text,
      latency_ms integer
    );
    create index if not exists token_usage_created_at_idx on token_usage (created_at);
    create index if not exists token_usage_client_idx on token_usage (client);
  `);
  const ins = db.prepare(`
    insert into token_usage
      (created_at, client, task_type, model, input_tokens, output_tokens,
       cache_read_tokens, cache_write_tokens, cache_hit, est_cost, request_id, latency_ms)
    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  return {
    kind: "sqlite",
    async insertUsage(r) {
      ins.run(
        r.created_at, r.client, r.task_type, r.model,
        r.input_tokens, r.output_tokens, r.cache_read_tokens, r.cache_write_tokens,
        r.cache_hit ? 1 : 0, r.est_cost, r.request_id, r.latency_ms,
      );
    },
    async recent(limit) {
      return db.prepare("select * from token_usage order by id desc limit ?").all(limit);
    },
    async daily(limit) {
      return db.prepare(`
        select date(created_at) as day, client, model,
               count(*) as requests,
               sum(input_tokens) as input_tokens,
               sum(output_tokens) as output_tokens,
               sum(cache_read_tokens) as cache_read_tokens,
               sum(cache_write_tokens) as cache_write_tokens,
               avg(est_cost) as avg_est_cost,
               avg(latency_ms) as avg_latency_ms
        from token_usage
        group by 1, 2, 3
        order by day desc
        limit ?
      `).all(limit);
    },
    async healthy() {
      return true;
    },
  };
}
