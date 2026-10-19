"""Watchdog Flight: keep the checker alive and enforce the kill switch and spend cap. No LLM on purpose."""
import json
import os

import duckdb

CHECKER, FIXER = "heal-demo-checker", "heal-demo-fixer"
STALE_MINUTES = int(os.environ.get("STALE_MINUTES", "30"))

con = duckdb.connect("md:")
con.execute("SET TimeZone = 'UTC'")
ids = {r[1]: str(r[0]) for r in con.execute("SELECT flight_id, flight_name FROM MD_LIST_FLIGHTS(owner_only := true)").fetchall()}
ctl = dict(con.execute("SELECT key, value FROM heal.main.control").fetchall())
log = []


def runs(name):
    return [(r[0], str(r[1]).replace("RUN_STATUS_", "")) for r in con.execute(
        'SELECT run_number, status FROM MD_LIST_FLIGHT_RUNS(flight_id := ?::UUID, "limit" := 10)', [ids[name]]).fetchall()]


# Kill switch or spend cap: cancel every running fixer and stop.
spent = con.execute("SELECT coalesce(sum(cost_usd), 0) FROM heal.main.fixer_runs WHERE started_at >= current_date").fetchone()[0]
if ctl.get("kill_switch") == "on" or spent > float(ctl.get("daily_spend_cap_usd", 20)):
    for rn, st in runs(FIXER):
        if st in ("PENDING", "RUNNING"):
            con.execute("SELECT * FROM MD_CANCEL_FLIGHT_RUN(flight_id := ?::UUID, run_number := ?)", [ids[FIXER], rn]).fetchall()
            log.append(f"cancelled fixer run {rn}")
    log.append(f"dispatch stopped (kill switch {ctl.get('kill_switch')}, spent ${spent:.2f})")
else:
    # Checker dead or silent: restart it, at most once an hour.
    last_beat = con.execute("SELECT date_diff('minute', max(ts), now()) FROM heal.main.heartbeats WHERE component = 'checker'").fetchone()[0]
    checker_runs = runs(CHECKER)
    failed = checker_runs and checker_runs[0][1] == "FAILED"
    recent = con.execute("""SELECT count(*) FROM heal.main.heartbeats WHERE component = 'watchdog' AND ts > now() - INTERVAL 1 HOUR
                            AND detail->>'restarted' = 'true'""").fetchone()[0]
    if (failed or last_beat is None or last_beat > STALE_MINUTES) and not recent:
        con.execute("SELECT * FROM MD_RUN_FLIGHT(flight_id := ?::UUID)", [ids[CHECKER]]).fetchall()
        log.append("restarted")
    # A fixer lease older than the runtime cap means the fixer died: let the checker retry it.
    con.execute("UPDATE heal.main.incidents SET lease_expires = now() WHERE status = 'investigating' AND lease_expires < now() - INTERVAL 1 HOUR")
con.execute("INSERT INTO heal.main.heartbeats (component, run_number, detail) VALUES ('watchdog', ?, ?::JSON)",
            [os.environ.get("MOTHERDUCK_FLIGHT_RUN_NUMBER"), json.dumps({"log": log, "restarted": "restarted" in log})])
print("watchdog:", log or "ok", flush=True)
