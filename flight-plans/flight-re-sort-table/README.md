---
title: Re-Sort a Table for Fast Selective Queries
id: flight-re-sort-table
description: >-
  A reusable Flight that rewrites a MotherDuck table — in full, or just the
  slice matched by a WHERE clause — in sorted order, so DuckDB's min/max
  zonemaps can skip row groups on selective queries. Inputs are validated
  against SQL injection with DuckDB's own tokenizer and parser before any SQL
  runs. Use when a large table is filtered on columns the data is not
  physically clustered by.
type: template
category: automation
features: [flights]
tags: []
prompt: >-
  Selective queries on a large MotherDuck table are slow because the rows are
  not physically clustered by the columns I filter on, so DuckDB's min/max
  zonemaps can't skip row groups. Help me adapt the "Re-Sort a Table for Fast
  Selective Queries" recipe to re-sort my table (in full or in WHERE-matched
  chunks) to my own data and use case, using it as a guide:
  https://motherduck.com/docs/cookbook/flight-re-sort-table
published_date: 2026-07-06
---

# Re-Sort a Table for Fast Selective Queries

DuckDB keeps a min/max index (a *zonemap*) per row group per column. A
filtered query skips a whole row group when the filter value falls outside
that range — but only if rows are physically clustered on the filtered
column. Tables loaded by arrival time rarely are. This Flight rewrites a
table (or the slice matched by `WHERE_CLAUSE`) in `ORDER_BY_CLAUSE` order,
inside one transaction, so selective queries read only the row groups that
matter. Background:
[Sorting for Fast Selective Queries](https://duckdb.org/2025/05/14/sorting-for-fast-selective-queries).

## How it works

1. **Validate.** The three inputs are spliced into SQL, so each is validated
   first on a local in-memory DuckDB, using DuckDB itself rather than string
   matching:
   - `duckdb.tokenize` (DuckDB's SQL tokenizer) inspects the leading tokens of
     `ORDER_BY_CLAUSE` / `WHERE_CLAUSE`, so an optional leading `ORDER BY` or
     `WHERE` keyword — any case, any whitespace — is recognized and stripped
     exactly the way the parser would see it.
   - Each input is embedded in a dummy statement (`SELECT * FROM <table>`,
     `... ORDER BY <clause>`, `... WHERE (<clause>)`) and handed to
     `json_serialize_sql`, which must report exactly **one** statement, with no
     parse error — and it only succeeds for **SELECT** statements. A smuggled
     semicolon, second statement, or non-SELECT payload fails here.
   - The returned AST is checked structurally: the table must be a bare
     base-table reference (no alias, join, subquery, sample, or `AT` clause),
     the ORDER BY must contribute only sort expressions (no `LIMIT`/`OFFSET`),
     and the WHERE must be a lone predicate. The table name is rebuilt with
     proper identifier quoting from the AST's catalog/schema/table parts, so
     the raw input string never appears in executed SQL.
2. **Rewrite, atomically.** With a random-UUID scratch table in the same
   database and schema:

   ```sql
   BEGIN;
   CREATE OR REPLACE TABLE <table>_<uuid> AS
       SELECT * FROM <table> WHERE (<where>);
   DELETE FROM <table> WHERE (<where>);
   INSERT INTO <table> SELECT * FROM <table>_<uuid> ORDER BY <order_by>;
   DROP TABLE <table>_<uuid>;
   COMMIT;
   ```

   The copy, delete, and insert row counts must agree or everything rolls
   back. The scratch table is dropped inside the transaction, so a commit
   leaves nothing behind and a rollback undoes everything at once.
3. **Report.** The run logs each statement, the matched row count, and the
   total duration. `DRY_RUN=true` stops after validation and a zero-cost
   server-side bind check (`WHERE ... AND false`) that resolves the table and
   every referenced column without touching data.

## Questions to answer

- Which table needs re-sorting, and is it filtered often enough to justify
  rewriting it?
- What sort key? Lead with the most-filtered columns, lowest cardinality
  first. For timestamps, prefer a rounded bucket (`date_trunc('month', ts),
  other_col`) over the raw value; for long VARCHARs the first 8 bytes are all
  zonemaps see, so `col[:8]` sorts cheaper with the same effect.
- Whole table, or one slice at a time (`WHERE_CLAUSE`) for tables too large to
  rewrite in one pass?
- One-shot, or on a schedule (cron, UTC) to counter ongoing unsorted inserts?

## Caveats

- **The rewrite is a full copy of the matched slice.** Expect roughly 2× that
  slice's storage while the transaction runs, and compute proportional to the
  sort. Filter with `WHERE_CLAUSE` to work in chunks.
- **Rewritten rows land at the end of the table.** A partial re-sort produces
  a chunk-sorted table — each rewritten slice is internally sorted, which is
  what zonemaps need — not one global ordering.
- **Concurrent writes to the table can conflict.** MotherDuck resolves
  write-write conflicts by failing one transaction; the Flight then rolls back
  cleanly and can be re-run. Prefer quiet windows.
- **Ordered inserts rely on `preserve_insertion_order`** (DuckDB's default,
  `true`). Don't disable it in the target database.
- **Sorting erodes.** Trickle inserts after the rewrite are appended unsorted;
  schedule the Flight periodically if the table keeps growing.
- **Expressions are validated syntactically, not sandboxed.** `WHERE_CLAUSE`
  may contain subqueries that read other tables the Flight's token can read.
  Statement injection is blocked; treat the config values as operator input,
  not end-user input.
- The MCP `query` tool can verify results afterwards, e.g. compare
  `EXPLAIN ANALYZE` row-group scan counts before and after.

## What you'll adjust

| Knob | Where | Example | Purpose |
|---|---|---|---|
| `TABLE_TO_ORDER` | Flight config / env | `my_db.main.trips` | Table to rewrite; optionally qualified, quoted identifiers OK. |
| `ORDER_BY_CLAUSE` | Flight config / env | `station_id, ride_date DESC` | Sort expression(s); a leading `ORDER BY` is optional, any case. |
| `WHERE_CLAUSE` | Flight config / env | `ride_date >= '2024-01-01'` | Optional slice to rewrite; a leading `WHERE` is optional. Empty = whole table. |
| `DRY_RUN` | Flight config / env | `true` | Validate inputs and bind against the real table without changing data. |

## Run it

You need a MotherDuck account and an access token that can write the target
table. Point the knobs at any table you own:

```bash
export MOTHERDUCK_TOKEN=your_token_here
TABLE_TO_ORDER='my_db.main.trips' \
ORDER_BY_CLAUSE='station_id, ride_date' \
WHERE_CLAUSE='' \
uv run --with-requirements requirements.txt flight.py
```

Start with `DRY_RUN=true` to see the exact statements and matched row count,
then run for real. A non-zero exit means validation rejected an input or the
transaction rolled back; the table is unchanged either way.

### Deploy as a Flight

Deploy through the Flight SQL surface (`MD_CREATE_FLIGHT`, then
`MD_RUN_FLIGHT`) with:

- `source_code`: [`flight.py`](flight.py), unchanged — all knobs are config
- `requirements_txt`: [`requirements.txt`](requirements.txt)
- `config`: `TABLE_TO_ORDER`, `ORDER_BY_CLAUSE`, and optionally
  `WHERE_CLAUSE` / `DRY_RUN`

The Flight runtime injects `MOTHERDUCK_TOKEN`; make sure it can write the
target database. Create the Flight without a schedule and trigger one run with
`MD_RUN_FLIGHT` to confirm it works; per-run `config` overrides make one
deployed Flight reusable for ad-hoc re-sorts of different tables. Add a
`schedule_cron` (UTC) only when the table sees ongoing inserts worth
re-clustering on a cadence.

## Security

The three inputs end up inside SQL statements, so the Flight refuses to run
until each one round-trips through DuckDB's own tokenizer and parser as
exactly one SELECT-shaped dummy statement of the expected structure (see *How
it works*). The table name is rebuilt from parsed identifier parts with `"`
quoting, the WHERE predicate must parse both bare and parenthesized (which
rejects paren-rebalancing and trailing `--` comment tricks), and validation
happens on a local in-memory connection before anything reaches MotherDuck.
The Flight runs with the privileges of its MotherDuck token: scope the token
to the databases it should touch, and keep the config values under operator
control.

## Learn more

- Why sorting speeds up selective queries:
  [Sorting for Fast Selective Queries](https://duckdb.org/2025/05/14/sorting-for-fast-selective-queries)
- Flight mechanics (creating, running, scheduling, config): the MotherDuck MCP
  `get_flight_guide` tool.
- Deeper MotherDuck or DuckDB questions: the `ask_docs_question` MCP tool.
- Files in this template: [`flight.py`](flight.py) (the single-file Flight)
  and [`requirements.txt`](requirements.txt) (just `duckdb`).
