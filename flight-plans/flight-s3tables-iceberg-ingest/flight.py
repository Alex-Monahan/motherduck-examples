"""
AWS S3 Tables (Iceberg) -> MotherDuck native storage flight.

Copies a table from an AWS S3 Tables Iceberg warehouse into MotherDuck native storage by
attaching the S3 Tables catalog as a first-class MotherDuck database and reading THROUGH it:

    CREATE OR REPLACE DATABASE <ice_db> (
        TYPE ICEBERG,
        WAREHOUSE '<s3tables-bucket-arn>',
        ENDPOINT_TYPE 's3_tables',   -- derives the REST endpoint + SigV4 auth from the ARN
        "secret" <secret>,           -- a pre-existing MotherDuck S3 secret (see below)
        default_schema '<namespace>' -- the Iceberg namespace; REQUIRED
    );
    CREATE OR REPLACE TABLE <dest_db>."<schema>"."<table>" AS
        SELECT * FROM <ice_db>."<namespace>"."<table>" [LIMIT <n>];

`CREATE OR REPLACE` on both statements makes the load atomic and idempotent (a re-run refreshes
the catalog attach and fully replaces the destination). The default config copies the first 100k
rows of the public ClickBench `hits` table into a `from_iceberg` database.

Credentials -- the flight uses NO AWS credentials
-------------------------------------------------
This flight assumes a **persistent MotherDuck S3 secret already exists** and simply references it
by name (MD_SECRET_NAME). Create it once before the flight runs (see the README):

    CREATE SECRET s3_tables_secret IN MOTHERDUCK (
        TYPE S3, KEY_ID '...', SECRET '...', SESSION_TOKEN '...', REGION 'us-east-1'
    );

The secret's credentials must reach the S3 Tables catalog (s3tables:* actions) in the bucket's
AWS account, plus the underlying data. Temporary (session-token) credentials expire with the
token -- re-create the secret before a run when they lapse.

Config (non-secret env vars)
----------------------------
  TABLE_BUCKET_ARN      S3 Tables bucket ARN (arn:aws:s3tables:<region>:<acct>:bucket/<name>).
  SOURCE_NAMESPACE      Iceberg namespace in the bucket (default 'clickbench').
  SOURCE_TABLE          Iceberg table to copy (default 'hits').
  MD_SECRET_NAME        Name of the pre-existing MotherDuck S3 secret (default 's3_tables_secret').
  ICEBERG_DATABASE      Name for the attached Iceberg catalog database (default 'iceberg_src').
  DESTINATION_DATABASE  MotherDuck database to create/write (default 'from_iceberg').
  DESTINATION_SCHEMA    Destination schema (default 'main').
  DESTINATION_TABLE     Destination table (default '' -> same as SOURCE_TABLE).
  ROW_LIMIT             Rows to copy (default '100000'; '0' or '' = entire table).
  AUDIT_TABLE           Audit ledger 'db.schema.table' (default '<dest_db>.<dest_schema>.flight_tracker'; '' to skip).

Requires the MotherDuck client extension that supports server-side Iceberg attach (duckdb 1.5.4);
a 1.5.3 client can run count(*) through the attach but cannot project columns from it.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import uuid

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("s3tables-iceberg-ingest")

IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# arn:aws:s3tables:<region>:<account-id>:bucket/<bucket-name>
ARN_RE = re.compile(r"arn:aws:s3tables:[a-z0-9-]+:\d+:bucket/[A-Za-z0-9][A-Za-z0-9_-]*")

DEFAULT_ARN = "arn:aws:s3tables:us-east-1:114325331884:bucket/clickbench-iceberg"


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def validate_identifier(name: str, value: str) -> str:
    """Reject anything that is not a plain SQL identifier before it reaches SQL that cannot be
    parameterized (database / schema / table / secret names)."""
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{name} must be a simple SQL identifier, got {value!r}")
    return value


def validate_arn(value: str) -> str:
    if not ARN_RE.fullmatch(value):
        raise ValueError(f"TABLE_BUCKET_ARN is not a valid S3 Tables bucket ARN: {value!r}")
    return value


def quote_ident(ident: str) -> str:
    """Double-quote a SQL identifier, escaping embedded quotes (defense in depth alongside
    validate_identifier)."""
    return '"' + ident.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """Single-quote a string literal, escaping embedded single quotes."""
    return "'" + value.replace("'", "''") + "'"


# --------------------------------------------------------------------------- #
# Connection + setup
# --------------------------------------------------------------------------- #
def connect_motherduck() -> duckdb.DuckDBPyConnection:
    return duckdb.connect("md:")


def attach_iceberg_catalog(
    con: duckdb.DuckDBPyConnection,
    ice_db: str,
    arn: str,
    namespace: str,
    secret_name: str,
) -> None:
    """Attach the S3 Tables catalog as a MotherDuck database of TYPE ICEBERG, using a pre-existing
    MotherDuck S3 secret. CREATE OR REPLACE makes this idempotent and refreshes the attach (and
    thus any rotated secret) on every run. Raises with a clear hint if the attach comes up as an
    error catalog -- almost always a missing/underprivileged secret."""
    con.execute(
        f"CREATE OR REPLACE DATABASE {quote_ident(ice_db)} ("
        f"TYPE ICEBERG, "
        f"WAREHOUSE {quote_literal(arn)}, "
        f"ENDPOINT_TYPE 's3_tables', "
        f'"secret" {quote_ident(secret_name)}, '
        f"default_schema {quote_literal(namespace)})"
    )
    is_error, message = con.execute(
        "SELECT is_error_catalog, error_message FROM MD_ATTACHED_DATABASES() WHERE alias = ?",
        [ice_db],
    ).fetchone()
    if is_error:
        raise RuntimeError(
            f"Iceberg catalog '{ice_db}' attached as an ERROR catalog: {message}\n"
            f"Ensure the MotherDuck secret '{secret_name}' exists and its AWS credentials have "
            f"S3 Tables catalog (s3tables:*) permissions in the bucket's account. Create it with "
            f"CREATE SECRET {secret_name} IN MOTHERDUCK (TYPE S3, ...); see the README."
        )
    log.info("Attached S3 Tables catalog as %s (TYPE ICEBERG) via secret %s", ice_db, secret_name)


def ensure_audit_table(con: duckdb.DuckDBPyConnection, audit: str) -> None:
    db, schema, _ = audit.split(".")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(db)}.{quote_ident(schema)}")
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {audit_fqtn(audit)} (
            run_id            VARCHAR,
            run_at            TIMESTAMPTZ,
            table_bucket_arn  VARCHAR,
            source_namespace  VARCHAR,
            source_table      VARCHAR,
            iceberg_source    VARCHAR,
            destination_table VARCHAR,
            row_limit         BIGINT,
            rows_loaded       BIGINT
        )
        """
    )


def audit_fqtn(audit: str) -> str:
    return ".".join(quote_ident(p) for p in audit.split("."))


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> None:
    arn = validate_arn(env("TABLE_BUCKET_ARN", DEFAULT_ARN))
    namespace = validate_identifier("SOURCE_NAMESPACE", env("SOURCE_NAMESPACE", "clickbench"))
    source_table = validate_identifier("SOURCE_TABLE", env("SOURCE_TABLE", "hits"))

    secret_name = validate_identifier("MD_SECRET_NAME", env("MD_SECRET_NAME", "s3_tables_secret"))
    ice_db = validate_identifier("ICEBERG_DATABASE", env("ICEBERG_DATABASE", "iceberg_src"))

    dest_db = validate_identifier("DESTINATION_DATABASE", env("DESTINATION_DATABASE", "from_iceberg"))
    dest_schema = validate_identifier("DESTINATION_SCHEMA", env("DESTINATION_SCHEMA", "main"))
    dest_table = validate_identifier(
        "DESTINATION_TABLE", env("DESTINATION_TABLE", "") or source_table
    )
    destination = f"{quote_ident(dest_db)}.{quote_ident(dest_schema)}.{quote_ident(dest_table)}"

    raw_limit = env("ROW_LIMIT", "100000")
    row_limit = int(raw_limit) if raw_limit else 0
    if row_limit < 0:
        raise ValueError(f"ROW_LIMIT must be >= 0, got {row_limit}")

    audit = env("AUDIT_TABLE", f"{dest_db}.{dest_schema}.flight_tracker")

    con = connect_motherduck()

    # Attach the S3 Tables catalog as a TYPE ICEBERG database, then read the source table THROUGH
    # the catalog (no iceberg_scan, no metadata pointers, no AWS credentials in this flight).
    attach_iceberg_catalog(con, ice_db, arn, namespace, secret_name)
    iceberg_source = (
        f"{quote_ident(ice_db)}.{quote_ident(namespace)}.{quote_ident(source_table)}"
    )

    con.execute(f"CREATE DATABASE IF NOT EXISTS {quote_ident(dest_db)}")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(dest_db)}.{quote_ident(dest_schema)}")

    limit_clause = f" LIMIT {row_limit}" if row_limit > 0 else ""
    log.info("Copying %s -> %s%s", iceberg_source, destination,
             f" (first {row_limit} rows)" if row_limit > 0 else " (entire table)")
    con.execute(
        f"CREATE OR REPLACE TABLE {destination} AS "
        f"SELECT * FROM {iceberg_source}{limit_clause}"
    )

    rows_loaded = con.execute(f"SELECT count(*) FROM {destination}").fetchone()[0]
    log.info("Loaded %s rows into %s", rows_loaded, destination)

    if audit:
        ensure_audit_table(con, audit)
        con.execute(
            f"INSERT INTO {audit_fqtn(audit)} VALUES (?, current_timestamp, ?, ?, ?, ?, ?, ?, ?)",
            [
                str(uuid.uuid4()),
                arn,
                namespace,
                source_table,
                f"{ice_db}.{namespace}.{source_table}",
                f"{dest_db}.{dest_schema}.{dest_table}",
                row_limit,
                rows_loaded,
            ],
        )

    print(f"copied {namespace}.{source_table} -> {dest_db}.{dest_schema}.{dest_table}: {rows_loaded} rows")


if __name__ == "__main__":
    main()
