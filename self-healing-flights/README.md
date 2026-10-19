---
title: Self-Healing Pipelines With MotherDuck Flights
id: self-healing-flights
description: >-
  Four MotherDuck Flights that run a pipeline, notice when it breaks, fix it with
  an AI coding agent in a zero-copy sandbox, get a second model to approve the
  fix, and ship it as a new Flight version, plus a Dive that shows every step.
  Use when you want pipelines that repair upstream changes on their own.
type: example
category: automation
features: [flights, dives]
tags: [python, openrouter]
prompt: >-
  I want my MotherDuck pipelines to detect their own failures and fix them with an
  AI agent, with a human-readable record of every fix. Help me adapt the
  "Self-Healing Pipelines With MotherDuck Flights" recipe to my pipelines, using it
  as a guide:
  https://motherduck.com/docs/cookbook/self-healing-flights
published_date: 2026-10-19
---

# Self-Healing Pipelines With MotherDuck Flights

A pipeline Flight loads weather data. When it breaks, a checker Flight opens an
incident, a fixer Flight repairs the code with an AI agent, and the fix ships as
a new version of the pipeline Flight. A Dive shows the pipeline, the runs, and
each step the fixer took.

## How it works

| File | What it does |
|---|---|
| `pipeline_flight.py` | Loads the NWS daily forecast and fails if a data test fails. |
| `hourly_flight.py` | A second pipeline (NWS hourly temperatures) that breaks the same way, so the fixer can reuse what it learned. |
| `checker_flight.py` | Finds failed runs, classifies them with `prompt_jev` (an LLM only when Jev is unsure), opens incidents, starts the fixer, confirms the fix on the next runs, and writes a MotherDuck Guide. |
| `fixer_flight.py` | Reproduces the failure in a zero-copy clone, runs opencode + Claude to edit the pipeline, asks GPT to approve, then saves the fix with `MD_UPDATE_FLIGHT`. |
| `watchdog_flight.py` | Restarts the checker if it stops, and cancels fixers when the kill switch is on. |
| `dive.tsx` | The status board: pipeline graph, run history, incidents, and the fixer's steps. |
| `schema.sql` | The `heal` database the Flights write and the Dive reads. |
| `deploy.py` | Creates or updates everything above. |

## Run it

1. Add your OpenRouter key as a Flight secret (DuckDB CLI):

   ```sql
   CREATE SECRET openrouter IN motherduck (TYPE flights, PARAMS MAP {'OPENROUTER_API_KEY': getenv('OPENROUTER_API_KEY')});
   ```

2. Deploy:

   ```bash
   MOTHERDUCK_TOKEN=... uv run --with duckdb==1.5.6 --with pytz deploy.py
   ```

3. Break the pipeline on purpose. This makes the weather API send a new payload shape:

   ```sql
   SET VARIABLE w = (SELECT flight_id FROM MD_LIST_FLIGHTS(owner_only := true) WHERE flight_name = 'heal-demo-weather');
   SELECT * FROM MD_UPDATE_FLIGHT(flight_id := getvariable('w'), config := MAP {'NWS_FEATURE_FLAGS': 'forecast_temperature_qv'});
   SELECT * FROM MD_RUN_FLIGHT(flight_id := getvariable('w'));
   ```

4. Open the **Self-healing Flights** Dive. Within a few checker runs the incident goes
   `open → investigating → merged → verifying → resolved`.

To undo the fix, roll the pipeline Flight back to an earlier version in the Flights UI.

## What you'll adjust

| Knob | Where | Default |
|---|---|---|
| Your pipeline | `PIPELINES` in `checker_flight.py`, `PIPELINE_FLIGHTS` in `fixer_flight.py` | `heal-demo-weather` |
| Fixer and approver models | Fixer config `MODEL`, `APPROVER_MODEL` | Claude Opus latest, GPT Sol latest |
| Budget per fix | Fixer config `BUDGET_USD` | `10` |
| Approver retries | Fixer config `APPROVER_ROUNDS` | `2` |
| Autonomy | `heal.main.control` key `autonomy_cap` (`L1` ships the fix, `L3` only proposes it) | `L1` |
| Kill switch, daily spend cap | `heal.main.control` keys `kill_switch`, `daily_spend_cap_usd` | `off`, `20` |
| Schedules | `deploy.py` (`--no-schedules` to skip) | checker every 5 min |

## Caveats

- The fixer edits pipeline code and ships it. Start with `autonomy_cap = 'L3'` so a
  human reviews each fix.
- Each fix costs real LLM money (about $0.20 to $2 here). Keep the budget and spend cap.
- Disable the schedules when you are done: `deploy.py --no-schedules`.
- DuckDB uses a lowercase `motherduck_token` before `MOTHERDUCK_TOKEN`; `deploy.py` stops if both are set to different tokens.

## Security

- The agent never holds a MotherDuck token. Its only tool is `verify`, served by the
  fixer, which runs the pipeline against a sandbox clone.
- A second model approves every fix from the diff and the test evidence alone.
- Upstream text is treated as data, never as instructions.

## Learn more

- Flights and Dives guides: `get_flight_guide`, `get_dive_guide`.
- Guides: resolved incidents are stored under the `self-healing/incidents/<category>` topic (`MD_LIST_GUIDES`).
