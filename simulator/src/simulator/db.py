"""Postgres connection helpers. Settings come from the environment (.env via Compose)."""

import os

import psycopg


def conninfo() -> str:
    return psycopg.conninfo.make_conninfo(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        application_name="fmcg-simulator",
    )


def connect() -> psycopg.Connection:
    """Open a connection. Callers wrap writes in `with conn.transaction():`."""
    return psycopg.connect(conninfo())
