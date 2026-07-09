-- SixVM Token Proxy — Supabase schema for usage logging (step 1)

create table if not exists token_usage (
  id bigint generated always as identity primary key,
  created_at timestamptz not null default now(),
  client text,
  task_type text,
  model text,
  input_tokens int,
  output_tokens int,
  cache_read_tokens int,
  cache_write_tokens int,
  cache_hit boolean default false, -- unused until the caching step
  est_cost numeric,
  request_id text,
  latency_ms int
);

create index if not exists token_usage_created_at_idx on token_usage (created_at);
create index if not exists token_usage_client_idx on token_usage (client);

-- Daily rollup per client/model: request counts, token sums, avg cost, avg latency.
create or replace view token_usage_daily as
select
  date_trunc('day', created_at) as day,
  client,
  model,
  count(*) as requests,
  sum(input_tokens) as input_tokens,
  sum(output_tokens) as output_tokens,
  sum(cache_read_tokens) as cache_read_tokens,
  sum(cache_write_tokens) as cache_write_tokens,
  avg(est_cost) as avg_est_cost,
  avg(latency_ms) as avg_latency_ms
from token_usage
group by 1, 2, 3;
