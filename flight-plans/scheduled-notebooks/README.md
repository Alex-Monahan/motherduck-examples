---
title: Schedule a MotherDuck Notebook
id: scheduled-notebooks
description: >-
  A Flight that runs every cell of a MotherDuck notebook, top to bottom, on one
  connection. Pass the notebook's title or UUID. Use to put an existing notebook
  on a schedule without copying its SQL into code.
type: template
category: automation
features: [flights]
tags: []
prompt: >-
  I want to run an existing MotherDuck notebook on a schedule without copying
  its SQL into code. Help me adapt the "Schedule a MotherDuck Notebook" recipe
  to my own data and use case, using it as a guide:
  https://motherduck.com/docs/cookbook/scheduled-notebooks
published_date: 2026-09-27
---

# Schedule a MotherDuck Notebook

Build a query pipeline in a MotherDuck notebook, then run it on a schedule. This
Flight fetches the notebook when each run starts, then runs its cells in order
on a single MotherDuck connection. Each cell runs against the database selected
for it in the UI. Edits you make to the notebook take effect on the next run,
with no redeploy.

## How it works

1. The notebook API is regional, so the Flight connects to MotherDuck first
   and reads the organization's region with
   `SELECT region FROM md_user_info();`. The API host is
   `https://api.<region>-aws.motherduck.com`.
2. `NOTEBOOK` is a notebook UUID or title. Anything that parses as a UUID is
   treated as one. Otherwise it is resolved as a title through
   `GET /mom/notebooks`, and it must match exactly one notebook.
3. `GET /mom/notebooks/<uuid>` returns the notebook. Its `json` field is a
   string holding `{"cells": [{"cellId", "query", "useDatabase", ...}]}`.
4. That same `duckdb.connect("md:")` connection runs every cell in order. Before each
   cell it runs `USE "<useDatabase>"`, the database selected for that cell in
   the UI. A cell with no selected database skips the `USE` and keeps the
   previous cell's database. State carries over between cells on that
   connection: temp tables, `SET` variables, and `USE`.
5. The first failing cell raises. The run stops and the Flight run fails
   (non-zero exit). Each cell's SQL is printed to the Flight logs before it runs.
   Cells that already ran stay committed.

The UUID is the last part of the notebook URL in the MotherDuck UI.

## Questions to answer

- Which notebook? Its UUID is safer than its title, because a title can be
  renamed or duplicated.
- Is the notebook safe to run unattended? Every cell runs, including
  exploratory `SELECT`s and any `DROP` or `CREATE OR REPLACE`.
- On what schedule (cron, UTC) should it run?

## Caveats

- **Undocumented API.** `/mom/notebooks` is the internal endpoint the MotherDuck
  UI uses, not a public API, so it can change without notice. If it moves, the
  Flight fails loudly at fetch time rather than running the wrong SQL.
- **Region-specific host.** The notebook API only answers on the regional host
  (for example `api.us-east-1-aws.motherduck.com`);
  `api.motherduck.com/mom/notebooks` returns 404. The region comes from
  `md_user_info()`, which needs a DuckDB 1.5.3 or later client.
- **Only the token owner's notebooks.** The endpoint lists notebooks owned by
  the user the Flight's token belongs to, so run the Flight as that user.
- **Every cell runs, and results are discarded.** Nothing is printed except the
  SQL. Persist anything you need with `CREATE TABLE ... AS` or `INSERT` in the
  notebook itself.
- **A failed run is not rolled back.** Writes from cells before the failing
  cell stay committed, and a rerun repeats them. Write cells so they can be
  rerun safely (`CREATE OR REPLACE`, `INSERT OR REPLACE`), or manage
  transactions in the notebook with `BEGIN` / `COMMIT`.
- **Selected databases must exist.** A cell whose selected database was dropped
  or detached fails at `USE`. Re-select a database for that cell in the UI.
- **UI-only cell settings are ignored.** Fields such as `runMode` and
  `isActive` have no effect here.

## What you'll adjust

| Knob | Where | Default | Purpose |
|---|---|---|---|
| `NOTEBOOK` | Flight config / env | required | Notebook UUID (e.g. `0110be63-b97c-462d-b9aa-f8f2b64eaaf9`) or exact title (e.g. `Daily rollup`). |
| `schedule_cron` | `MD_CREATE_FLIGHT` | none | When to run, e.g. `0 6 * * *` for daily at 06:00 UTC. |

## Run it

```bash
export MOTHERDUCK_TOKEN=your_token_here
NOTEBOOK='Daily rollup' uv run --with-requirements requirements.txt flight.py
```

The SQL of each cell is printed as it runs, then `Ran N cells`. A non-zero exit
means the run failed, either at fetch or lookup time or in a cell. The
traceback names the error.

### Deploy as a Flight

Deploy through the Flight SQL surface (`MD_CREATE_FLIGHT`, then
`MD_RUN_FLIGHT`) with:

- `source_code`: [`flight.py`](flight.py), unchanged
- `requirements_txt`: [`requirements.txt`](requirements.txt)
- `config`: `MAP {'NOTEBOOK': '<uuid or title>'}`

The Flight runtime injects `MOTHERDUCK_TOKEN`. That token's user must own the
notebook and have whatever access the notebook's SQL needs. Create the
Flight without a schedule and trigger one run with `MD_RUN_FLIGHT`. Check the
output with `MD_GET_FLIGHT_LOGS`, then add a `schedule_cron`. To run a
different notebook, change `config`; the code stays the same.

## Security

- The token is sent as a bearer header only to the regional
  `https://api.<region>-aws.motherduck.com` host.
- Each cell's SQL is written to the Flight logs. Keep secrets and sensitive
  literals out of the notebook; use `CREATE SECRET` once outside it, not in a
  scheduled cell.
- The notebook's SQL runs with the Flight token's full permissions. Anyone who
  can edit the notebook can change what the scheduled Flight executes, so
  schedule only notebooks whose editors you trust.
- The database name is quoted as an identifier in `USE`. Cell SQL runs as
  written, because running it is the purpose of the Flight.

## Learn more

- Flight mechanics (creating, running, scheduling, logs): the MotherDuck MCP
  `get_flight_guide` tool.
- Deeper MotherDuck or DuckDB questions: the `ask_docs_question` MCP tool.
- Files: [`flight.py`](flight.py) (the Flight) and
  [`requirements.txt`](requirements.txt) (`duckdb` only).
