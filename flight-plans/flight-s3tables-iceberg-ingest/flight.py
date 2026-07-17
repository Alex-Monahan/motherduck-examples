"""
AWS S3 Tables (Iceberg) -> MotherDuck batch copy flight.

Copies one or more tables from an AWS S3 Tables Iceberg catalog into a MotherDuck database.
Each table is moved by a single streaming SQL statement:

    CREATE OR REPLACE TABLE <target>."<namespace>"."<table>" AS
        SELECT * FROM <catalog>."<namespace>"."<table>";

That statement is the entire load. It is:
    ATOMIC (CREATE OR REPLACE swaps in one step)
    IDEMPOTENT (re-running fully replaces)
    STREAMING (DuckDB pipelines the scan into the write, bounded memory)

Tables land at <target>.<namespace>.<table>, preserving source namespace names. Per-table
logging lands in <target>.main.flight_tracker.

Credentials 
-------------------------------------------------
It references a persistent MotherDuck S3 secret by name (SECRET_NAME below), created once out
of band. The keys need S3 Tables catalog access (s3tables:*) in the bucket's account, plus read
on the data:

    CREATE SECRET s3_tables_secret IN MOTHERDUCK (
        TYPE S3, KEY_ID '...', SECRET '...', REGION 'us-east-1'
    );

Config (non-secret env vars)
----------------------------
    TABLE_BUCKET_ARN   S3 Tables bucket ARN (arn:aws:s3tables:<region>:<acct>:bucket/<name>).
    TARGET_DATABASE    MotherDuck database to write into (default: iceberg_ingest).
    INCLUDED_SCHEMAS / EXCLUDED_SCHEMAS   comma-separated Iceberg namespaces.
    INCLUDED_TABLES  / EXCLUDED_TABLES    comma-separated, fully qualified namespace.table.
    MAX_RETRIES (5)
    RETRY_BASE_SECONDS (2)

To open the catalog, the attach needs one namespace that exists in the bucket. It is taken from
INCLUDED_SCHEMAS, else the namespaces named in INCLUDED_TABLES, else the sample namespace. Set
INCLUDED_SCHEMAS (or INCLUDED_TABLES) when pointing at a non-sample bucket.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone

import duckdb
from tenacity import Retrying, stop_after_attempt, wait_exponential, wait_random

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("s3tables-iceberg-ingest")

# arn:aws:s3tables:<region>:<account-id>:bucket/<bucket-name>
ARN_RE = re.compile(r"arn:aws:s3tables:[a-z0-9-]+:\d+:bucket/[A-Za-z0-9][A-Za-z0-9_-]*")
IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
DEFAULT_ARN = "arn:aws:s3tables:us-east-1:114325331884:bucket/clickbench-iceberg"
SAMPLE_NAMESPACE = "clickbench"  # namespace in the default sample bucket

# Postgres catalog schemas that are never copied, should they surface via discovery.
SYSTEM_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast"}

# Persistent MotherDuck S3 secret the catalog attach references. Change it to point at another
# secret (it must be a TYPE S3 secret created IN MOTHERDUCK).
SECRET_NAME = "s3_tables_secret"

# Local catalog name the S3 Tables bucket is attached as (TYPE ICEBERG). One source of truth.
ICEBERG_ALIAS = "iceberg_src"


# --------------------------------------------------------------------------- #
# Small SQL / env helpers
# --------------------------------------------------------------------------- #
def quote_ident(ident: str) -> str:
    """Double-quote a SQL identifier, escaping embedded quotes, so names with special
    characters or reserved words are handled correctly and safely."""
    return '"' + ident.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """Single-quote a string literal, escaping embedded single quotes."""
    return "'" + value.replace("'", "''") + "'"


def qualified_name(*parts: str) -> str:
    """Join parts into a fully-qualified SQL name with every part quoted, so identifiers from
    config or catalog discovery cannot break out of their quotes (SQL-injection safe)."""
    return ".".join(quote_ident(part) for part in parts)


def validate_identifier(name: str, value: str) -> str:
    """Reject a config value that is not a plain SQL identifier before it is used as one
    (defense in depth alongside quote_ident)."""
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{name} must be a simple SQL identifier, got {value!r}")
    return value


def csv_set(name: str) -> frozenset[str]:
    """Turn a comma-separated env var into a clean set for membership filtering. Returns a set
    of trimmed, non-empty values (empty set if unset)."""
    raw = os.environ.get(name, "") or ""
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def validate_arn(value: str) -> str:
    if not ARN_RE.fullmatch(value):
        raise ValueError(f"TABLE_BUCKET_ARN is not a valid S3 Tables bucket ARN: {value!r}")
    return value


# --------------------------------------------------------------------------- #
# Table selection
# --------------------------------------------------------------------------- #
def is_selected(
    schema: str,
    table: str,
    included_schemas: frozenset[str],
    excluded_schemas: frozenset[str],
    included_tables: frozenset[str],
    excluded_tables: frozenset[str],
) -> bool:
    """Decide whether a discovered table is copied, applying the two include/exclude gates
    where exclude always wins and system schemas are excluded."""
    fqtn = f"{schema}.{table}"  # include/exclude membership only; never interpolated into SQL
    if schema in SYSTEM_SCHEMAS:
        return False
    if included_schemas and schema not in included_schemas:
        return False
    if schema in excluded_schemas:
        return False
    if included_tables and fqtn not in included_tables:
        return False
    if fqtn in excluded_tables:
        return False
    return True


def pick_attach_namespace(
    included_schemas: frozenset[str], included_tables: frozenset[str]
) -> str:
    """Choose one namespace to open the catalog with (Iceberg requires a default_schema that
    exists). Prefer INCLUDED_SCHEMAS, then the namespaces in INCLUDED_TABLES, then the sample."""
    if included_schemas:
        return sorted(included_schemas)[0]
    namespaces = sorted({fq.split(".", 1)[0] for fq in included_tables if "." in fq})
    if namespaces:
        return namespaces[0]
    return SAMPLE_NAMESPACE


# --------------------------------------------------------------------------- #
# Connection + setup
# --------------------------------------------------------------------------- #
def attach_iceberg(con: duckdb.DuckDBPyConnection, arn: str, attach_namespace: str) -> None:
    """Attach the S3 Tables bucket as a MotherDuck database of TYPE ICEBERG, using the
    persistent S3 secret. CREATE OR REPLACE makes it idempotent. Raises with a clear hint if
    the attach comes up as an error catalog (usually a missing/underprivileged secret or a
    default_schema namespace that does not exist)."""
    con.execute(
        f"CREATE OR REPLACE DATABASE {quote_ident(ICEBERG_ALIAS)} ("
        f"TYPE ICEBERG, "
        f"WAREHOUSE {quote_literal(arn)}, "
        f"ENDPOINT_TYPE 's3_tables', "
        f'"secret" {quote_ident(SECRET_NAME)}, '
        f"default_schema {quote_literal(attach_namespace)})"
    )
    is_error, message = con.execute(
        "SELECT is_error_catalog, error_message FROM MD_ATTACHED_DATABASES() WHERE alias = ?",
        [ICEBERG_ALIAS],
    ).fetchone()
    if is_error:
        raise RuntimeError(
            f"Iceberg catalog attached as an ERROR catalog: {message}\n"
            f"Check that secret {SECRET_NAME!r} exists with S3 Tables catalog (s3tables:*) "
            f"permissions in the bucket's account, and that namespace {attach_namespace!r} exists."
        )
    log.info("Attached %s as %s (TYPE ICEBERG) via secret %s", arn, ICEBERG_ALIAS, SECRET_NAME)


def ensure_target(con: duckdb.DuckDBPyConnection, target_db: str) -> None:
    """Create the target database and the audit logging table up front."""
    con.execute(f"CREATE DATABASE IF NOT EXISTS {quote_ident(target_db)}")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {qualified_name(target_db, 'main', 'flight_tracker')} ("
        "  run_id               VARCHAR,"
        "  flight_secret_name   VARCHAR,"
        "  source_schema        VARCHAR,"
        "  source_table         VARCHAR,"
        "  destination_database VARCHAR,"
        "  destination_schema   VARCHAR,"
        "  destination_table    VARCHAR,"
        "  rows_loaded          BIGINT,"
        "  attempts             INTEGER,"
        "  started_at           TIMESTAMP,"
        "  finished_at          TIMESTAMP,"
        "  update_ts            TIMESTAMP"
        ")"
    )


# --------------------------------------------------------------------------- #
# Discovery + per-table load
# --------------------------------------------------------------------------- #
def discover_tables(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    """List the candidate source tables across every namespace in the attached catalog."""
    rows = con.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() "
        "WHERE database_name = ? ORDER BY schema_name, table_name",
        [ICEBERG_ALIAS],
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def load_table(con: duckdb.DuckDBPyConnection, target_db: str, schema: str, table: str) -> int:
    """Move one table as a single atomic, idempotent, streaming CTAS. Returns the row count
    the CTAS reports as inserted."""
    tgt = qualified_name(target_db, schema, table)
    src = qualified_name(ICEBERG_ALIAS, schema, table)
    return con.execute(f"CREATE OR REPLACE TABLE {tgt} AS SELECT * FROM {src}").fetchone()[0]


def record_success(
    con: duckdb.DuckDBPyConnection, target_db: str, run_id: str,
    schema: str, table: str, rows_loaded: int, attempts: int,
    started_at: datetime, finished_at: datetime, update_ts: datetime,
) -> None:
    """After success, append a row to the audit table."""
    con.execute(
        f"INSERT INTO {qualified_name(target_db, 'main', 'flight_tracker')} "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [run_id, SECRET_NAME, schema, table, target_db, schema, table,
         rows_loaded, attempts, started_at, finished_at, update_ts],
    )


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main() -> None:
    """Orchestrate the copy: connect, attach, discover, then load each selected table
    sequentially with per-table retries/isolation and record results."""
    RUN_ID = str(uuid.uuid4())
    ARN = validate_arn(os.environ.get("TABLE_BUCKET_ARN", DEFAULT_ARN).strip())
    TARGET_DB = validate_identifier(
        "TARGET_DATABASE",
        os.environ.get("TARGET_DATABASE", "iceberg_ingest").strip() or "iceberg_ingest",
    )
    MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
    RETRY_BASE_SECONDS = float(os.environ.get("RETRY_BASE_SECONDS", "2"))
    INCLUDED_SCHEMAS = csv_set("INCLUDED_SCHEMAS")
    EXCLUDED_SCHEMAS = csv_set("EXCLUDED_SCHEMAS")
    INCLUDED_TABLES = csv_set("INCLUDED_TABLES")
    EXCLUDED_TABLES = csv_set("EXCLUDED_TABLES")

    log.info("Run %s -> target %r", RUN_ID, TARGET_DB)

    con = duckdb.connect("md:")
    attach_iceberg(con, ARN, pick_attach_namespace(INCLUDED_SCHEMAS, INCLUDED_TABLES))
    ensure_target(con, TARGET_DB)

    all_tables = discover_tables(con)
    selected = [
        (s, t) for (s, t) in all_tables
        if is_selected(s, t, INCLUDED_SCHEMAS, EXCLUDED_SCHEMAS, INCLUDED_TABLES, EXCLUDED_TABLES)
    ]
    log.info("Discovered %d table(s); %d selected after filters", len(all_tables), len(selected))

    if not selected:
        log.warning("No tables selected - nothing to do.")
        return

    # Pre-create the target schemas (mirroring source namespace names) once.
    for sch in sorted({s for (s, _) in selected}):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(TARGET_DB, sch)}")

    started_all = datetime.now(timezone.utc)
    failed: list[str] = []
    succeeded = 0
    rows_total = 0

    for schema, table in selected:
        fqtn = f"{schema}.{table}"  # logging only; SQL names go through qualified_name()
        started = datetime.now(timezone.utc)
        retryer = Retrying(
            stop=stop_after_attempt(MAX_RETRIES),
            wait=wait_exponential(multiplier=RETRY_BASE_SECONDS, max=60) + wait_random(0, 1),
            reraise=True,
        )
        try:
            rows = retryer(load_table, con, TARGET_DB, schema, table)
            attempts = retryer.statistics.get("attempt_number", 1)
            finished = datetime.now(timezone.utc)
            record_success(con, TARGET_DB, RUN_ID, schema, table, rows,
                           attempts, started, finished, datetime.now(timezone.utc))
            succeeded += 1
            rows_total += rows
            log.info("OK   %-50s %12d rows (attempts=%d)", fqtn, rows, attempts)
        except Exception as exc:  # noqa: BLE001 - per-table isolation is intentional
            attempts = retryer.statistics.get("attempt_number", 1)
            failed.append(fqtn)
            log.error("FAIL %-50s (attempts=%d) %s: %s", fqtn, attempts, type(exc).__name__, exc)

    total_seconds = (datetime.now(timezone.utc) - started_all).total_seconds()
    log.info("Summary: %d succeeded, %d failed, %d rows in %.1fs (run %s)",
             succeeded, len(failed), rows_total, total_seconds, RUN_ID)

    if failed:
        log.error("Failed tables: %s", ", ".join(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()
