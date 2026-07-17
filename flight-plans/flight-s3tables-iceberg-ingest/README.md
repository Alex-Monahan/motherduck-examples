---
title: Copy an AWS S3 Tables Iceberg Table into MotherDuck
id: flight-s3tables-iceberg-ingest
description: >-
  A reusable Flight that copies a table from an AWS S3 Tables (Iceberg) catalog
  into a native MotherDuck table with one CREATE OR REPLACE TABLE AS SELECT. Use
  it for a config-driven, re-runnable S3 Tables to MotherDuck ingest.
type: template
category: ingestion
features: [flights]
tags: [ingest, s3]
---

# Copy an AWS S3 Tables Iceberg Table into MotherDuck

Copies a table from an AWS S3 Tables Iceberg warehouse into a native MotherDuck table, so queries
read a local table instead of scanning Iceberg on every run. Point it at any S3 Tables table, set
how many rows to copy, and re-run it on a schedule to keep the copy current. Everything is driven by
config, so you can reuse it without editing the code.

## Prerequisite: an S3 secret

The Flight holds no AWS keys of its own. It looks up a MotherDuck S3 secret by name
(`MD_SECRET_NAME`, default `s3_tables_secret`), so create that secret once. The keys need S3 Tables
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

Set these as Flight config, not by editing code.

| Config key | Default | Purpose |
|---|---|---|
| `TABLE_BUCKET_ARN` | ClickBench sample bucket ARN | S3 Tables bucket ARN. |
| `SOURCE_NAMESPACE` | `clickbench` | Iceberg namespace in the bucket. |
| `SOURCE_TABLE` | `hits` | Table to copy. |
| `MD_SECRET_NAME` | `s3_tables_secret` | The MotherDuck S3 secret to use. |
| `ICEBERG_DATABASE` | `iceberg_src` | Name for the attached catalog database. |
| `DESTINATION_DATABASE` | `from_iceberg` | Target database. Created if missing. |
| `DESTINATION_SCHEMA` | `main` | Target schema. Created if missing. |
| `DESTINATION_TABLE` | (= `SOURCE_TABLE`) | Target table name. |
| `ROW_LIMIT` | `100000` | Rows to copy. `0` copies the whole table. |
| `AUDIT_TABLE` | `<dest_db>.<dest_schema>.flight_tracker` | Per-run log. Empty to skip. |

## Run it

With the secret in place, the Flight needs only a MotherDuck token.

```bash
export MOTHERDUCK_TOKEN=your_token_here
ROW_LIMIT=1000 uv run --with duckdb==1.5.4 --no-project flight.py
```

This copies 1,000 rows into `from_iceberg.main.hits`. Change any default inline, for example
`ROW_LIMIT=0` for the whole table.

### Deploy as a Flight

Create it with `MD_CREATE_FLIGHT`, passing `name`, `source_code` ([`flight.py`](flight.py)),
`requirements_txt` ([`requirements.txt`](requirements.txt)), and any `config` overrides. No secret
arguments are needed: the Flight reads a stored `IN MOTHERDUCK` secret, and a MotherDuck token is
attached for you. Run it once with `MD_RUN_FLIGHT`, then add a schedule with `MD_UPDATE_FLIGHT`. Use
long-lived IAM keys for scheduled runs.

## Caveats

- If the secret's keys lack S3 Tables catalog access in the bucket's account, the run fails right
  away with a clear message.
- `ROW_LIMIT=0` reads the whole table (the sample `hits` is about 100M rows), so keep a limit for tests.
- Needs `duckdb==1.5.4` (pinned in `requirements.txt`).

## Learn more

- Flights: the `get_flight_guide` MCP tool. S3 Tables, Iceberg, or secrets: `ask_docs_question`.
- Files here: [`flight.py`](flight.py) and [`requirements.txt`](requirements.txt).
