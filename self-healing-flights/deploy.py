"""Create or update the schema, the four Flights and the Dive (all resolved by name, so it is safe to re-run).

    MOTHERDUCK_TOKEN=... uv run --with duckdb==1.5.6 deploy.py [--no-schedules]
"""
import os
import pathlib
import sys

import duckdb

HERE = pathlib.Path(__file__).parent
REQS = "duckdb==1.5.6\npytz\nrequests\n"
SCHEDULES = "--no-schedules" not in sys.argv
FLIGHTS = {
    "heal-demo-weather": ("pipeline_flight.py", "0 * * * *", {"NWS_FEATURE_FLAGS": ""}, 600),
    "heal-demo-hourly": ("hourly_flight.py", "30 * * * *", {"NWS_FEATURE_FLAGS": ""}, 600),
    "heal-demo-checker": ("checker_flight.py", "*/5 * * * *", {"GREEN_RUNS": "2", "MAX_ATTEMPTS": "2"}, 600),
    "heal-demo-fixer": ("fixer_flight.py", None, {"INCIDENT_ID": "0", "ATTEMPT": "1", "BUDGET_USD": "10", "APPROVER_ROUNDS": "2",
                                                    "MODEL": "~anthropic/claude-opus-latest", "APPROVER_MODEL": "~openai/gpt-sol-latest"}, 2700),
    "heal-demo-watchdog": ("watchdog_flight.py", "7-59/15 * * * *", {"STALE_MINUTES": "30"}, 300),
}

# DuckDB prefers a lowercase motherduck_token over MOTHERDUCK_TOKEN, so refuse to guess which account to deploy to.
if os.environ.get("motherduck_token") and os.environ.get("MOTHERDUCK_TOKEN") and os.environ["motherduck_token"] != os.environ["MOTHERDUCK_TOKEN"]:
    sys.exit("motherduck_token and MOTHERDUCK_TOKEN are both set to different tokens; unset one so it's clear which account to deploy to.")
con = duckdb.connect("md:")
con.execute("SET TimeZone = 'UTC'")
for stmt in (HERE / "schema.sql").read_text().split(";\n"):
    if stmt.strip() and not all(l.strip().startswith("--") for l in stmt.strip().splitlines()):
        con.execute(stmt)
existing = {r[1]: (str(r[0]), r[2]) for r in con.execute("SELECT flight_id, flight_name, schedule_cron FROM MD_LIST_FLIGHTS(owner_only := true)").fetchall()}
for name, (file, cron, config, max_runtime) in FLIGHTS.items():
    cron = cron if SCHEDULES else None
    args = dict(source_code=(HERE / file).read_text(), requirements_txt=REQS, config=config, flight_secret_names=["openrouter"], max_runtime_sec=max_runtime)
    if name in existing:
        fid, old_cron = existing[name]
        if (cron or None) != (old_cron or None):
            args["schedule_cron"] = cron or ""
        con.execute(f"SELECT * FROM MD_UPDATE_FLIGHT(flight_id := ?::UUID, {', '.join(f'{k} := ?' for k in args)})", [fid, *args.values()]).fetchall()
        print(f"updated {name}")
    else:
        if cron:
            args["schedule_cron"] = cron
        con.execute(f"SELECT * FROM MD_CREATE_FLIGHT(name := ?, {', '.join(f'{k} := ?' for k in args)})", [name, *args.values()]).fetchall()
        print(f"created {name}")

title, content = "Self-healing Flights", (HERE / "dive.tsx").read_text()
dive = con.execute("SELECT id FROM MD_LIST_DIVES() WHERE title = ?", [title]).fetchone()
if dive:
    con.execute("SELECT * FROM MD_UPDATE_DIVE_CONTENT(id := ?::UUID, content := ?)", [str(dive[0]), content]).fetchall()
    print(f"updated Dive https://app.motherduck.com/dives/{dive[0]}")
else:
    new = con.execute("SELECT id FROM MD_CREATE_DIVE(title := ?, content := ?, description := ?, api_version := 1)",
                      [title, content, "Pipeline DAG, runs, incidents and the healing DAG of each automated fix"]).fetchone()[0]
    print(f"created Dive https://app.motherduck.com/dives/{new}")
