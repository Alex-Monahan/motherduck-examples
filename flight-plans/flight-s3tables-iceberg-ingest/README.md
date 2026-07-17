---
title: Copy AWS S3 Tables (Iceberg) into MotherDuck With a Flight
id: flight-s3tables-iceberg-ingest
description: >-
  A reusable Flight that copies tables from an AWS S3 Tables (Iceberg) catalog
  into MotherDuck, one streaming full-refresh CREATE OR REPLACE per table, with
  config-driven namespace/table selection, retries with backoff, and a per-table
  audit log. Use it for a config-driven, re-runnable S3 Tables to MotherDuck ingest.
type: template
category: ingestion
features: [flights]
tags: [ingest, s3]
---

# Copy AWS S3 Tables (Iceberg) into MotherDuck With a Flight

Copies one or more tables from an AWS S3 Tables Iceberg warehouse into a MotherDuck database, so
queries read native tables instead of scanning Iceberg on every run. Point it at a bucket, pick
which namespaces and tables to copy, and re-run it on a schedule to keep the copies current.
Everything is driven by config, so you can reuse it without editing the code.

Each run attaches the S3 Tables catalog as a MotherDuck database (`TYPE ICEBERG`), discovers the
tables in scope, and moves each with one statement:
`CREATE OR REPLACE TABLE <target>."<namespace>"."<table>" AS SELECT * FROM <catalog>."<namespace>"."<table>"`.
That is the whole load: atomic (swaps in one step), idempotent (a re-run replaces), and streaming
(flat memory even on large tables). Tables land at `<target>.<namespace>.<table>`, preserving
source namespace names, with a per-table log in `<target>.main.flight_tracker`.

## Prerequisite: an S3 secret

The Flight holds no AWS keys. It references a MotherDuck S3 secret by name (`SECRET_NAME` in
`flight.py`, default `s3_tables_secret`), so create that secret once. The keys need S3 Tables
catalog access (`s3tables:*`) in the bucket's account, plus read on the data. Run this through the
DuckDB CLI so `getenv()` reads the keys from your shell instead of writing them into the statement
(drop `SESSION_TOKEN` for long-lived IAM keys):

```bash
motherduck_token="$YOUR_TOKEN" duckdb "md:" <<'SQL'
CREATE OR REPLACE SECRET s3_tables_secret IN MOTHERDUCK (
    TYPE S3,
    KEY_ID getenv('AWS_ACCESS_KEY_ID'),
    SECRET getenv('AWS_SECRET_ACCESS_KEY'),
    SESSION_TOKEN getenv('AWS_SESSION_TOKEN'),
    REGION 'us-east-1'
);
SQL
```

Temporary keys expire, so re-create the secret when they lapse or use long-lived IAM keys.

## What you'll adjust

No code edits are required. Everything is read from Flight config and the MotherDuck secret.

| Knob | Default | Purpose |
|---|---|---|
| `TABLE_BUCKET_ARN` | ClickBench sample bucket ARN | S3 Tables bucket to copy from (`arn:aws:s3tables:…:bucket/…`). |
| `TARGET_DATABASE` | `iceberg_ingest` | MotherDuck database for the copy (created if absent). Tables land at `<target>.<namespace>.<table>`. |
| `INCLUDED_SCHEMAS` | (all) | Comma-separated namespaces to include. Empty = all. |
| `EXCLUDED_SCHEMAS` | (none) | Comma-separated namespaces to drop. Exclude wins. |
| `INCLUDED_TABLES` | (all) | Comma-separated `namespace.table` to include. Empty = all in selected namespaces. |
| `EXCLUDED_TABLES` | (none) | Comma-separated `namespace.table` to drop. Exclude wins. |
| `MAX_RETRIES` | `5` | Per-table retry attempts on transient errors. |
| `RETRY_BASE_SECONDS` | `2` | Exponential-backoff multiplier (seconds). |
| `s3_tables_secret` **secret** | (required) | MotherDuck `TYPE S3` secret with the AWS keys. Rename it only if you also change `SECRET_NAME` in `flight.py`. |

Selection precedence: a table is copied only if its namespace passes the namespace gate and its
`namespace.table` passes the table gate; excludes always win.

To open the catalog, the attach needs one namespace that exists in the bucket. It comes from
`INCLUDED_SCHEMAS`, else the namespaces named in `INCLUDED_TABLES`, else the sample namespace. Set
`INCLUDED_SCHEMAS` (or `INCLUDED_TABLES`) when pointing at a non-sample bucket.

## Run it

With the secret in place, the Flight needs only a MotherDuck token; it reads no AWS env vars.

```bash
export MOTHERDUCK_TOKEN=your_token_here
# scope it so a first run is cheap (the sample `hits` table is ~100M rows):
export INCLUDED_TABLES=clickbench.probe,clickbench.ptest
uv run --with-requirements requirements.txt flight.py
```

This copies the selected tables into `iceberg_ingest`, one full-refresh `CREATE OR REPLACE` each,
and writes one `flight_tracker` row per table. One log line per table plus a summary; it exits
non-zero if any table failed after retries.

### Deploy as a Flight

Create it with `MD_CREATE_FLIGHT`, passing `name`, `source_code` ([`flight.py`](flight.py)),
`requirements_txt` ([`requirements.txt`](requirements.txt)), and a `config` with at least
`TABLE_BUCKET_ARN` plus any `INCLUDED_*`/`EXCLUDED_*` scoping and `TARGET_DATABASE`. No secret
arguments are needed: the Flight reads a stored `IN MOTHERDUCK` secret, and a MotherDuck token is
attached for you. Run it once with `MD_RUN_FLIGHT`, confirm `flight_tracker` has one row per table,
then add a schedule with `MD_UPDATE_FLIGHT`. Use long-lived IAM keys for scheduled runs.

## Caveats

- **Full refresh per table.** Cost scales with table size, not change volume. The sample `hits`
  table is about 100M rows, so scope runs with `INCLUDED_TABLES`/`INCLUDED_SCHEMAS`.
- **Dropped source tables are not removed** from the target; delete them yourself.
- **Credentials are the usual failure.** Without S3 Tables catalog access in the bucket's account,
  the attach fails right away with a clear message.
- Needs the MotherDuck client in `duckdb==1.5.4` (pinned in `requirements.txt`).

## Learn more

- Flights: the `get_flight_guide` MCP tool. S3 Tables, Iceberg, or secrets: `ask_docs_question`.
- Files here: [`flight.py`](flight.py) and [`requirements.txt`](requirements.txt).
