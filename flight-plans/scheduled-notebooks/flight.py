import json
import os
import urllib.request
import uuid

import duckdb

con = duckdb.connect("md:")
REGION = con.execute("SELECT region FROM md_user_info()").fetchone()[0]
API = f"https://api.{REGION}-aws.motherduck.com/mom/notebooks"
HEADERS = {"Authorization": f"Bearer {os.environ['MOTHERDUCK_TOKEN']}"}


def get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=30) as resp:
        return json.load(resp)


def notebook_id(ref):
    try:
        return str(uuid.UUID(ref))
    except ValueError:
        ids = [n["id"] for n in get(API)["notebooks"] if n["title"] == ref]
        if len(ids) != 1:
            raise SystemExit(f"Expected 1 notebook titled {ref!r}, found {len(ids)}")
        return ids[0]


cells = json.loads(get(f"{API}/{notebook_id(os.environ['NOTEBOOK'])}")["notebook"]["json"])["cells"]
for i, cell in enumerate(cells, 1):
    sql, db = cell.get("query") or "", cell.get("useDatabase")
    print(f"--- cell {i}/{len(cells)} (database: {db}) ---\n{sql}", flush=True)
    if db:
        con.execute(f'USE "{db.replace(chr(34), chr(34) * 2)}"')
    con.execute(sql)
print(f"Ran {len(cells)} cells")
