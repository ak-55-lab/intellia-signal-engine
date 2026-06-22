-- Intellia Signal Engine — Supabase schema
-- Run this in Supabase SQL Editor if tables don't exist yet

-- ── Accounts ─────────────────────────────────────────────────────────────────
create table if not exists accounts (
  id           text primary key,
  name         text not null,
  acct_group   text,
  segment      text[],
  description  text,
  owner        text default '',
  child        boolean default false,
  customer     boolean default false,
  crm_connected boolean default false,
  osha_search  text[],
  cms_search   text[],
  hq_state     text,
  osha_confirmed boolean default false,
  source       text default 'manual',   -- 'seed' | 'manual' | 'import'
  created_at   timestamptz default now()
);

-- ── OSHA Inspections warehouse ────────────────────────────────────────────────
-- Enable trigram extension for fast LIKE / ilike queries on estab_name
create extension if not exists pg_trgm;

create table if not exists osha_inspections (
  activity_nr    text primary key,
  estab_name     text,
  site_address   text,
  site_city      text,
  site_state     text,
  site_zip       text,
  naics_code     text,
  sic_code       text,
  insp_type      text,
  open_date      date,
  close_case_date date,
  nr_in_estab    text,
  owner_type     text,
  data_source    text default 'csv',    -- 'csv' | 'api_sync'
  loaded_at      timestamptz default now()
);

-- Fast substring search on establishment name (case-insensitive)
create index if not exists osha_estab_name_trgm_idx
  on osha_inspections using gin(lower(estab_name) gin_trgm_ops);
create index if not exists osha_open_date_idx
  on osha_inspections(open_date desc);

-- ── Signal Columns ───────────────────────────────────────────────────────────
create table if not exists signal_columns (
  key          text primary key,
  label        text not null,
  source_type  text default 'websearch',
  on_by_default boolean default true,
  segment      text[],
  prompt       text,
  threshold    integer default 6,
  cadence      text default 'Weekly',
  budget       integer default 5,
  builtin      boolean default false,
  sort_order   integer default 100,
  sources      jsonb,          -- [{type, label, target, query_hint}]
  column_type  text default 'signal',   -- 'signal' | 'enrichment'
  enrich_field text default '',         -- 'website' | 'employees' | 'annual_revenue' | 'industry' | 'hq_city' | 'founded' | 'custom'
  created_at   timestamptz default now()
);

-- Migration (run if table already exists):
-- alter table signal_columns add column if not exists column_type text default 'signal';
-- alter table signal_columns add column if not exists enrich_field text default '';

create table if not exists raw_signals (
  id          uuid primary key default gen_random_uuid(),
  account_id  text not null,
  source_type text not null,       -- 'osha' or 'cms'
  raw_data    jsonb not null,
  fetched_at  timestamptz not null default now()
);

create index if not exists raw_signals_account_id_idx on raw_signals(account_id);
create index if not exists raw_signals_fetched_at_idx on raw_signals(fetched_at desc);

create table if not exists scored_signals (
  id           uuid primary key default gen_random_uuid(),
  account_id   text not null,
  source_type  text not null,      -- signal column key e.g. 'osha', 'rfp', 'hiring'
  score        integer,
  summary      text,
  action       text,
  excerpt      text,
  model        text,
  verified     boolean default false,
  confidence   text,
  raw_count    integer default 0,
  signal_date  text,
  scored_at    timestamptz not null default now()
);

create index if not exists scored_signals_account_id_idx on scored_signals(account_id);
create index if not exists scored_signals_score_idx on scored_signals(score desc);
create index if not exists scored_signals_scored_at_idx on scored_signals(scored_at desc);
