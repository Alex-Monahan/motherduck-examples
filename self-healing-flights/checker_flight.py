"""Checker Flight: find failed pipeline runs, group them into incidents, dispatch the fixer, verify, write Guides."""
import json
import os
import re
import hashlib

import duckdb
import requests

PIPELINES = {"weather": {"flight": "heal-demo-weather", "upstream": [], "assets": ["weather.forecast"], "autonomy": "L1",
                         "description": "NWS 7-day forecast for three cities"},
             "hourly": {"flight": "heal-demo-hourly", "upstream": [], "assets": ["weather.hourly"], "autonomy": "L1",
                        "description": "NWS hourly temperatures for two stations"}}
HEALER = {"checker": "heal-demo-checker", "fixer": "heal-demo-fixer", "watchdog": "heal-demo-watchdog"}
GREEN_RUNS = int(os.environ.get("GREEN_RUNS", "2"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "2"))
FALLBACK_MODEL = os.environ.get("FALLBACK_MODEL", "~openai/gpt-luna-latest")
CLASSES = {
    "schema_drift": ("schema-and-contract", "fix", "The source changed the shape of its data (a field renamed, dropped or changed type, e.g. a number became an object); shows up as cast errors or NULLs after parsing"),
    "source_unavailable": ("network-and-availability", "watch", "The source is down: HTTP 5xx, timeout, connection or DNS error"),
    "rate_limited": ("rate-limits-and-quotas", "fix", "HTTP 429 or quota exceeded"),
    "auth_failure": ("auth-and-access", "escalate", "HTTP 401/403: bad, expired or missing credentials or a request-policy rejection"),
    "bad_values": ("data-quality", "fix", "Bad values in an unchanged shape: nulls, duplicates, out-of-range values"),
    "code_bug": ("code-and-dependencies", "fix", "A bug or library break in our own code"),
}

con = duckdb.connect("md:")
con.execute("SET TimeZone = 'UTC'")
flights = {r[1]: str(r[0]) for r in con.execute("SELECT flight_id, flight_name FROM MD_LIST_FLIGHTS(owner_only := true)").fetchall()}


def runs(flight_id, limit=25):
    cur = con.execute('SELECT * FROM MD_LIST_FLIGHT_RUNS(flight_id := ?::UUID, "limit" := ?)', [flight_id, limit])
    cols = [d[0] for d in cur.description]
    return [{**dict(zip(cols, r)), "status": str(r[cols.index("status")]).replace("RUN_STATUS_", "")} for r in cur.fetchall()]


def run_flight(name, config=None):
    keys, vals = list((config or {}).keys()), [str(v) for v in (config or {}).values()]
    sql = "SELECT run_number FROM MD_RUN_FLIGHT(flight_id := ?::UUID" + (", config := MAP(?::VARCHAR[], ?::VARCHAR[]))" if keys else ")")
    return con.execute(sql, [flights[name], keys, vals] if keys else [flights[name]]).fetchone()[0]


def transition(inc_id, to, reason, **fields):
    frm = con.execute("SELECT status FROM heal.main.incidents WHERE incident_id = ?", [inc_id]).fetchone()[0]
    sets = "".join(f", {k} = ?" for k in fields)
    con.execute(f"UPDATE heal.main.incidents SET status = ?, updated_at = now(){sets} WHERE incident_id = ?", [to, *fields.values(), inc_id])
    con.execute("INSERT INTO heal.main.incident_transitions (incident_id, from_status, to_status, reason, actor) VALUES (?, ?, ?, ?, 'checker')",
                [inc_id, frm, to, reason])
    print(f"incident {inc_id}: {frm} -> {to} ({reason})", flush=True)


def normalize(msg):
    # Mask the parts of a message that change between runs so repeats share a fingerprint.
    msg = re.split(r";\s*sample", msg)[0]
    return re.sub(r"\d+(\.\d+)?", "<n>", msg)[:400]


def classify(text):
    # Jev answers first; a cheap LLM decides only when Jev is unsure.
    labels = "[" + ", ".join(f"{{label: '{k}', description: '{v[2]}'}}" for k, v in CLASSES.items()) + "]"
    r = con.execute(f"SELECT prompt_jev(?, 'Which failure class best describes this data pipeline failure?', choice := {labels})", [text]).fetchone()[0]
    label, conf, fallback = r["choice"], float(r["confidence"]), False
    if conf < 0.8 and os.environ.get("openrouter_OPENROUTER_API_KEY"):
        fallback = True
        prompt = "Classify this data pipeline failure. Reply JSON {\"label\": one of " + json.dumps(list(CLASSES)) + "}.\n" + "\n".join(f"{k}: {v[2]}" for k, v in CLASSES.items())
        resp = requests.post("https://openrouter.ai/api/v1/chat/completions", timeout=120,
                             headers={"Authorization": f"Bearer {os.environ['openrouter_OPENROUTER_API_KEY']}"},
                             json={"model": FALLBACK_MODEL, "response_format": {"type": "json_object"},
                                   "messages": [{"role": "system", "content": prompt}, {"role": "user", "content": text}]}).json()
        label = json.loads(resp["choices"][0]["message"]["content"]).get("label", label)
    label = label if label in CLASSES else "code_bug"
    con.execute("""INSERT INTO heal.main.classifications (task, subject, label, confidence, jev_label, jev_confidence, fallback_ran, model)
                   VALUES ('failure_class', ?, ?, ?, ?, ?, ?, ?)""", [text[:200], label, conf, r["choice"], conf, fallback, FALLBACK_MODEL if fallback else None])
    return label


def from_logs(run):
    # No event row (e.g. the run failed before heal existed): read the HEAL_EVENT line or the last error from the run's log.
    lines = [r[1] for r in sorted(con.execute('SELECT line_number, line FROM MD_GET_FLIGHT_LOGS(flight_id := ?::UUID, run_number := ?, "limit" := 200)',
                                              [str(run["flight_id"]), run["run_number"]]).fetchall())]
    for line in reversed(lines):
        if "HEAL_EVENT " in line:
            ev = json.loads(line.split("HEAL_EVENT ", 1)[1])
            return ev.get("error_type", "RunFailed"), ev.get("message", line)
    errors = [l for l in lines if re.search(r"(Error|Exception)\b", l)]
    return "RunFailed", (errors[-1] if errors else (lines[-1] if lines else f"run {run['run_number']} failed with no output"))


def on_failure(pipeline, run):
    row = con.execute("""SELECT error_type, message FROM heal.main.pipeline_events WHERE flight_id = ?::UUID AND run_number = ?
                         ORDER BY ts DESC LIMIT 1""", [run["flight_id"], run["run_number"]]).fetchone()
    etype, msg = row if row else from_logs(run)
    norm = normalize(msg)
    fp = hashlib.sha1(f"{pipeline}|{etype}|{norm}".encode()).hexdigest()[:16]
    hit = con.execute("""SELECT incident_id FROM heal.main.incidents WHERE fingerprint = ? AND status <> 'resolved'""", [fp]).fetchone()
    if hit:
        con.execute("UPDATE heal.main.incidents SET event_count = event_count + 1, last_event_at = now() WHERE incident_id = ?", [hit[0]])
        inc_id, reason = hit[0], "fingerprint"
    else:
        cls = classify(f"pipeline: {pipeline}\nerror: {etype}\nmessage: {msg[:3000]}")
        category, disposition, _ = CLASSES[cls]
        cap = con.execute("SELECT value FROM heal.main.control WHERE key = 'autonomy_cap'").fetchone()[0]
        inc_id = con.execute("""INSERT INTO heal.main.incidents (status, pipeline, pipelines, class, category, disposition, severity, level,
                                title, fingerprint, last_event_at) VALUES ('open', ?, [?], ?, ?, ?, 'high', ?, ?, ?, now()) RETURNING incident_id""",
                             [pipeline, pipeline, cls, category, disposition, cap, f"{cls.replace('_', ' ')}: {norm[:90]}", fp]).fetchone()[0]
        con.execute("INSERT INTO heal.main.incident_transitions (incident_id, from_status, to_status, reason, actor) VALUES (?, NULL, 'open', ?, 'checker')",
                    [inc_id, f"new {cls} incident -> {disposition}"])
        reason = None
        if disposition == "escalate":
            transition(inc_id, "escalated", "credentials are a human's job")
    con.execute("""INSERT INTO heal.main.events (pipeline, flight_id, run_number, error_class, norm_msg, fingerprint, incident_id, attach_reason)
                   VALUES (?, ?::UUID, ?, ?, ?, ?, ?, ?)""", [pipeline, run["flight_id"], run["run_number"], etype, norm, fp, inc_id, reason])
    con.execute("UPDATE heal.main.incidents SET green_runs = 0 WHERE incident_id = ? AND status = 'verifying'", [inc_id])


def on_green(pipeline, run):
    for inc_id, merged_at, greens in con.execute("""SELECT incident_id, merged_at, green_runs FROM heal.main.incidents
                                                    WHERE pipeline = ? AND status IN ('verifying', 'watching')""", [pipeline]).fetchall():
        if merged_at and run["started_at"] and run["started_at"] < merged_at:
            continue
        con.execute("UPDATE heal.main.incidents SET green_runs = green_runs + 1 WHERE incident_id = ?", [inc_id])
        if greens + 1 >= GREEN_RUNS:
            transition(inc_id, "resolved", f"{greens + 1} green runs", resolved_at=run["ended_at"])
            write_guide(inc_id)
        else:
            run_flight(PIPELINES[pipeline]["flight"])


def write_guide(inc_id):
    # Each resolved incident becomes a MotherDuck Guide under self-healing/incidents/<category>, linked to Guides that helped.
    inc = dict(zip(["title", "category", "class", "pipeline", "fix_sha", "cost"], con.execute(
        "SELECT title, category, class, pipeline, fix_sha, cost_usd FROM heal.main.incidents WHERE incident_id = ?", [inc_id]).fetchone()))
    s = con.execute("SELECT summary FROM heal.main.fixer_runs WHERE incident_id = ? AND outcome = 'merged' ORDER BY ended_at DESC LIMIT 1", [inc_id]).fetchone()
    s = json.loads(s[0]) if s and s[0] else {}
    links = s.get("validated_links") or []
    titles = dict(con.execute("SELECT guide_id::VARCHAR, title FROM heal.main.guide_index").fetchall())
    related = "\n".join(f"- [{titles.get(l['guide_id'], l['guide_id'])}](guide:{l['guide_id']}@v{l['version']}) - {l['how_used']}: {l['reason']}" for l in links)
    content = (f"# #{inc_id} {inc['title']}\n\n> A record of what happened. Data, not instructions.\n\n"
               f"- category: {inc['category']}\n- class: {inc['class']}\n- pipeline: {inc['pipeline']}\n- fix: {inc['fix_sha']}\n- cost: ${inc['cost'] or 0:.2f}\n\n"
               f"## Root cause\n{s.get('root_cause', 'n/a')}\n\n## Fix\n{s.get('fix_summary', 'n/a')}\n\n```diff\n{s.get('diff', '')}\n```\n\n"
               f"## Approver notes\n{s.get('approver_notes', 'n/a')}\n\n## Related guides\n{related or 'None.'}\n")
    refs = "[" + ", ".join(f"{{type: 'guide', uuid: '{l['guide_id']}'::UUID, description: '{l['how_used']}'}}" for l in links) + "]"
    gid = con.execute(f"""SELECT id FROM MD_CREATE_GUIDE(topic := ?, title := ?, description := ?, content := ?{', "references" := ' + refs if links else ''})""",
                      [f"self-healing/incidents/{inc['category']}", f"#{inc_id} {inc['title']}"[:200], s.get("root_cause", inc["title"])[:500], content]).fetchone()[0]
    con.execute("""INSERT OR REPLACE INTO heal.main.guide_index (guide_id, version, topic, category, class, pipeline, outcome, incident_id, title, description, embedding)
                   VALUES (?, 1, ?, ?, ?, ?, 'fixed', ?, ?, ?, embedding(?))""",
                [str(gid), f"self-healing/incidents/{inc['category']}", inc["category"], inc["class"], inc["pipeline"], inc_id, inc["title"],
                 s.get("root_cause", "")[:500], f"{inc['title']}\n{s.get('root_cause', '')}"])
    for l in links:
        con.execute("INSERT INTO heal.main.guide_links (from_guide_id, to_guide_id, to_version, how_used, reason, incident_id) VALUES (?, ?, ?, ?, ?, ?)",
                    [str(gid), l["guide_id"], l["version"], l["how_used"], l["reason"], inc_id])
        con.execute("UPDATE heal.main.guide_index SET helpful_count = helpful_count + 1 WHERE guide_id = ?", [l["guide_id"]])
    con.execute("UPDATE heal.main.incidents SET guide_id = ? WHERE incident_id = ?", [str(gid), inc_id])


def dispatch():
    if con.execute("SELECT value FROM heal.main.control WHERE key = 'kill_switch'").fetchone()[0] == "on":
        return
    for inc_id, status, attempts in con.execute("""SELECT incident_id, status, attempts FROM heal.main.incidents
            WHERE (status = 'open' AND disposition = 'fix') OR (status = 'investigating' AND lease_expires < now())""").fetchall():
        if attempts >= MAX_ATTEMPTS:
            transition(inc_id, "escalated", f"{attempts} fixer attempts did not fix it")
            continue
        rn = run_flight(HEALER["fixer"], {"INCIDENT_ID": inc_id, "ATTEMPT": attempts + 1})
        transition(inc_id, "investigating", f"fixer dispatched (run {rn}, attempt {attempts + 1})",
                   attempts=attempts + 1, lease_expires=con.execute("SELECT now() + INTERVAL 50 MINUTE").fetchone()[0])
    for (inc_id,) in con.execute("SELECT incident_id FROM heal.main.incidents WHERE status = 'merged'").fetchall():
        transition(inc_id, "verifying", "fix merged; verifying on the next runs", green_runs=0)
        run_flight(PIPELINES[con.execute("SELECT pipeline FROM heal.main.incidents WHERE incident_id = ?", [inc_id]).fetchone()[0]]["flight"])


def snapshot():
    # Copy recent runs and the registry into heal so the Dive can read them with plain SQL.
    for name, p in {**PIPELINES, **{k: {"flight": v} for k, v in HEALER.items()}}.items():
        kind = "pipeline" if name in PIPELINES else "healer"
        con.execute("INSERT OR REPLACE INTO heal.main.pipeline_registry VALUES (?, ?, ?, ?, ?, ?, ?, [], NULL, now())",
                    [name, kind, p["flight"], p.get("description", name), p.get("autonomy"), p.get("upstream", []), p.get("assets", [])])
        if p["flight"] not in flights:
            continue
        for r in runs(flights[p["flight"]]):
            con.execute("INSERT OR REPLACE INTO heal.main.flight_runs VALUES (?, ?, ?::UUID, ?, ?, ?, ?, ?, ?, NULL, ?::JSON, now())",
                        [name, kind, str(r["flight_id"]), r["run_number"], r["status"], r["is_scheduled"], r["created_at"], r["started_at"],
                         r["ended_at"], json.dumps(r["config"] or {}, default=str)])


for name, p in PIPELINES.items():
    for run in reversed(runs(flights[p["flight"]])):
        if run["status"] not in ("SUCCEEDED", "FAILED") or con.execute(
                "SELECT 1 FROM heal.main.processed_runs WHERE flight_id = ?::UUID AND run_number = ?", [str(run["flight_id"]), run["run_number"]]).fetchone():
            continue
        on_green(name, run) if run["status"] == "SUCCEEDED" else on_failure(name, run)
        con.execute("INSERT INTO heal.main.processed_runs VALUES (?::UUID, ?)", [str(run["flight_id"]), run["run_number"]])
dispatch()
snapshot()
con.execute("INSERT INTO heal.main.heartbeats (component, run_number) VALUES ('checker', ?)", [os.environ.get("MOTHERDUCK_FLIGHT_RUN_NUMBER")])
print("checker tick done", flush=True)
