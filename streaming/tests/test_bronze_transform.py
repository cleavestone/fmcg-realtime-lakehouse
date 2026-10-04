"""Bronze parsing, routing and quarantine rules."""

import json
from datetime import datetime

from streaming.bronze.transform import BRONZE_COLUMNS, QUARANTINE_COLUMNS, parse_cdc

TS = datetime(2026, 10, 4, 9, 0, 0)


def _record(value, topic="fmcg.public.orders", offset=0, key='{"order_id":1}'):
    payload = value if value is None or isinstance(value, str) else json.dumps(value)
    return (
        key.encode() if key else None,
        payload.encode() if payload is not None else None,
        topic,
        0,
        offset,
        TS,
    )


KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset bigint, timestamp timestamp"
)


def _parse(spark, *records):
    df = spark.createDataFrame(list(records), KAFKA_SCHEMA)
    return {r["kafka_offset"]: r for r in parse_cdc(df).collect()}


GOOD = {
    "order_id": 1,
    "status": "placed",
    "__op": "c",
    "__source_ts_ms": 1791036116348,
    "__source_lsn": 31678552,
    "__deleted": "false",
}


def test_valid_record_is_parsed_and_routed(spark):
    row = _parse(spark, _record(GOOD))[0]
    assert row["quarantine_reason"] is None
    assert row["table"] == "orders"
    assert (row["op"], row["source_lsn"], row["source_ts_ms"]) == ("c", 31678552, 1791036116348)
    assert row["is_deleted"] is False
    assert row["kafka_key"] == '{"order_id":1}'
    assert json.loads(row["payload"]) == GOOD, "payload is kept verbatim"
    assert row["ingested_at"] is not None and row["ingest_date"] is not None


def test_delete_flag(spark):
    row = _parse(spark, _record({**GOOD, "__op": "d", "__deleted": "true"}))[0]
    assert (row["op"], row["is_deleted"], row["quarantine_reason"]) == ("d", True, None)


def test_snapshot_read_is_valid(spark):
    assert _parse(spark, _record({**GOOD, "__op": "r"}))[0]["quarantine_reason"] is None


def test_quarantine_reasons(spark):
    rows = _parse(
        spark,
        _record(None, offset=1),
        _record("{not json", offset=2),
        _record({k: v for k, v in GOOD.items() if k != "__op"}, offset=3),
        _record({**GOOD, "__op": "x"}, offset=4),
        _record({k: v for k, v in GOOD.items() if k != "__source_lsn"}, offset=5),
        _record({k: v for k, v in GOOD.items() if k != "__source_ts_ms"}, offset=6),
        _record(GOOD, topic="fmcg.public.Bad-Name", offset=7),
    )
    assert rows[1]["quarantine_reason"] == "null value (tombstone)"
    assert rows[2]["quarantine_reason"] == "invalid json"
    assert rows[3]["quarantine_reason"] == "missing or unknown __op"
    assert rows[4]["quarantine_reason"] == "missing or unknown __op"
    assert rows[5]["quarantine_reason"] == "missing __source_lsn"
    assert rows[6]["quarantine_reason"] == "missing __source_ts_ms"
    assert rows[7]["quarantine_reason"] == "invalid table name"


def test_output_has_every_bronze_and_quarantine_column(spark):
    df = parse_cdc(spark.createDataFrame([_record(GOOD)], KAFKA_SCHEMA))
    assert set(BRONZE_COLUMNS) <= set(df.columns)
    assert set(QUARANTINE_COLUMNS) <= set(df.columns)
