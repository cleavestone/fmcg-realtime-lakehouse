"""Reconcile Silver facts against Postgres, row by row and column by column.

Run with the simulator stopped. Silver lags Postgres by up to ~2 trigger intervals, so the
check retries until everything matches or RECONCILE_TIMEOUT_S passes. Postgres is read with
the read-only Debezium user. Exit code 0 = Silver equals the source.

    docker compose run --rm spark-tools python -m streaming.tools.reconcile
"""

import os
import sys
import time
from datetime import UTC, datetime

import psycopg

from streaming.common.schemas import FACTS, TableSpec
from streaming.common.spark import build_session

SAMPLE = 5


def _norm(value):
    """Spark returns naive UTC datetimes, psycopg returns aware ones: compare in UTC."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return value


def _rows(records, spec: TableSpec) -> dict[tuple, tuple]:
    n = len(spec.keys)
    return {tuple(r[:n]): tuple(_norm(v) for v in r) for r in records}


def postgres_rows(conn: psycopg.Connection, spec: TableSpec) -> dict[tuple, tuple]:
    cols = list(spec.keys) + [c for c in spec.column_names if c not in spec.keys]
    records = conn.execute(f"SELECT {', '.join(cols)} FROM {spec.name}").fetchall()
    return _rows(records, spec)


def silver_rows(spark, spec: TableSpec) -> dict[tuple, tuple]:
    cols = list(spec.keys) + [c for c in spec.column_names if c not in spec.keys]
    records = spark.table(f"silver.{spec.name}").filter("NOT is_deleted").select(*cols).collect()
    return _rows([tuple(r) for r in records], spec)


def compare(pg: dict, silver: dict) -> tuple[list, list, list]:
    missing = sorted(set(pg) - set(silver))
    extra = sorted(set(silver) - set(pg))
    differs = sorted(k for k in set(pg) & set(silver) if pg[k] != silver[k])
    return missing, extra, differs


def main() -> int:
    timeout = int(os.environ.get("RECONCILE_TIMEOUT_S", "180"))
    spark = build_session("reconcile")
    conninfo = psycopg.conninfo.make_conninfo(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["PG_READ_USER"],
        password=os.environ["PG_READ_PASSWORD"],
        options="-c timezone=UTC",
    )
    deadline = time.monotonic() + timeout
    attempt = 0
    while True:
        attempt += 1
        results = {}
        with psycopg.connect(conninfo) as conn:
            for spec in FACTS.values():
                pg, silver = postgres_rows(conn, spec), silver_rows(spark, spec)
                results[spec.name] = (len(pg), len(silver), *compare(pg, silver), pg, silver)
        ok = all(not (m or e or d) for _, _, m, e, d, _, _ in results.values())
        if ok or time.monotonic() >= deadline:
            break
        lagging = [t for t, (_, _, m, e, d, _, _) in results.items() if m or e or d]
        print(f"attempt {attempt}: not yet matching ({', '.join(lagging)}); retrying in 15s")
        time.sleep(15)

    print(f"{'table':<12} {'postgres':>9} {'silver':>9} {'missing':>8} {'extra':>6} {'differ':>7}")
    for table, (n_pg, n_silver, missing, extra, differs, pg, silver) in results.items():
        print(
            f"{table:<12} {n_pg:>9} {n_silver:>9} {len(missing):>8} {len(extra):>6} "
            f"{len(differs):>7}"
        )
        for key in differs[:SAMPLE]:
            print(f"    differs {key}:\n      postgres {pg[key]}\n      silver   {silver[key]}")
        for key in missing[:SAMPLE]:
            print(f"    missing in silver {key}")
        for key in extra[:SAMPLE]:
            print(f"    extra in silver {key}")
    print("RECONCILED: silver matches postgres" if ok else "MISMATCH")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
