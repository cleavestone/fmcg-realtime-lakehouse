"""Silver facts: typed parsing, latest-per-key by LSN, and the LSN-guarded MERGE."""

import json
import uuid
from datetime import datetime
from decimal import Decimal

import pytest
from delta.tables import DeltaTable
from streaming.common.schemas import FACTS
from streaming.silver.facts import latest_per_key, merge_latest, parse, silver_ddl

ORDERS = FACTS["orders"]
ITEMS = FACTS["order_items"]
INVENTORY = FACTS["inventory"]
BRONZE_SCHEMA = (
    "payload string, op string, source_lsn bigint, source_ts_ms bigint, is_deleted boolean, "
    "kafka_offset bigint"
)


def order(order_id, status, lsn, offset=None, deleted=False, store_id=3):
    payload = {
        "order_id": order_id,
        "store_id": store_id,
        "rep_id": 5,
        "order_ts": "2026-10-03T14:01:56.314044Z",
        "status": status,
        "updated_at": "2026-10-03T14:01:56.352792Z",
    }
    op = "d" if deleted else "u"
    return (
        json.dumps(payload),
        op,
        lsn,
        lsn // 1000,
        deleted,
        offset if offset is not None else lsn,
    )


def bronze(spark, *rows):
    return spark.createDataFrame(list(rows), BRONZE_SCHEMA)


def apply(spark, target, spec, *rows):
    merge_latest(target, latest_per_key(parse(bronze(spark, *rows), spec), spec), spec)


def state(target, spec):
    return {tuple(r[k] for k in spec.keys): r.asDict() for r in target.toDF().collect()}


@pytest.fixture
def orders_table(spark, tmp_path):
    name = f"orders_{uuid.uuid4().hex[:8]}"
    spark.sql(
        f"CREATE TABLE {name} ({silver_ddl(ORDERS)}) USING DELTA LOCATION '{tmp_path / name}'"
    )
    return DeltaTable.forName(spark, name)


# --- parsing -------------------------------------------------------------------------


def test_parse_casts_money_and_timestamps(spark):
    payload = {
        "order_item_id": 10577,
        "order_id": 2427,
        "product_id": 112,
        "quantity": 1,
        "unit_price": "3192.00",
        "discount_pct": "2.50",
        "updated_at": "2026-10-03T14:03:34.370819Z",
    }
    row = parse(bronze(spark, (json.dumps(payload), "r", 1, 1, False, 0)), ITEMS).first()
    assert row["unit_price"] == Decimal("3192.00")
    assert row["discount_pct"] == Decimal("2.50")
    assert row["updated_at"] == datetime(2026, 10, 3, 14, 3, 34, 370819)
    assert row["order_item_id"] == 10577 and row["quantity"] == 1


def test_parse_composite_key_table(spark):
    payload = {
        "product_id": 7,
        "warehouse": "nairobi_dc",
        "qty_on_hand": 42,
        "updated_at": "2026-10-03T14:03:34Z",
    }
    row = parse(bronze(spark, (json.dumps(payload), "r", 1, 1, False, 0)), INVENTORY).first()
    assert (row["product_id"], row["warehouse"], row["qty_on_hand"]) == (7, "nairobi_dc", 42)


# --- latest per key ------------------------------------------------------------------


def test_latest_per_key_uses_lsn_not_arrival_order(spark):
    # Arrival (offset) order is deliberately scrambled relative to LSN order.
    df = parse(
        bronze(
            spark,
            order(1, "shipped", lsn=300, offset=1),
            order(1, "placed", lsn=100, offset=2),
            order(1, "confirmed", lsn=200, offset=3),
            order(2, "placed", lsn=150, offset=4),
        ),
        ORDERS,
    )
    latest = {r["order_id"]: r["status"] for r in latest_per_key(df, ORDERS).collect()}
    assert latest == {1: "shipped", 2: "placed"}


# --- MERGE ---------------------------------------------------------------------------


def test_insert_then_newer_update(spark, orders_table):
    apply(spark, orders_table, ORDERS, order(1, "placed", lsn=100))
    apply(spark, orders_table, ORDERS, order(1, "confirmed", lsn=200))
    row = state(orders_table, ORDERS)[(1,)]
    assert (row["status"], row["source_lsn"], row["is_deleted"]) == ("confirmed", 200, False)


def test_older_or_replayed_events_are_ignored(spark, orders_table):
    apply(spark, orders_table, ORDERS, order(1, "shipped", lsn=300))
    apply(spark, orders_table, ORDERS, order(1, "placed", lsn=100))  # out of order
    apply(spark, orders_table, ORDERS, order(1, "shipped", lsn=300))  # replayed batch
    row = state(orders_table, ORDERS)[(1,)]
    assert (row["status"], row["source_lsn"]) == ("shipped", 300)
    assert orders_table.toDF().count() == 1


def test_many_changes_in_one_batch_land_as_final_state(spark, orders_table):
    apply(
        spark,
        orders_table,
        ORDERS,
        order(1, "placed", lsn=100),
        order(1, "confirmed", lsn=200),
        order(1, "shipped", lsn=300),
        order(1, "delivered", lsn=400),
    )
    assert state(orders_table, ORDERS)[(1,)]["status"] == "delivered"


def test_delete_sets_flag_only(spark, orders_table):
    apply(spark, orders_table, ORDERS, order(1, "cancelled", lsn=100))
    # A delete event whose payload carries placeholder values must not overwrite the row.
    apply(spark, orders_table, ORDERS, order(1, "placed", lsn=200, deleted=True, store_id=0))
    row = state(orders_table, ORDERS)[(1,)]
    assert row["is_deleted"] is True
    assert (row["status"], row["store_id"], row["source_lsn"]) == ("cancelled", 3, 200)


def test_update_and_delete_in_same_batch_keep_the_update(spark, orders_table):
    # Regression: insert lands in one batch; the cancel and the hard delete land together in
    # the next. The delete is the newest event, but the cancel's values must still apply.
    apply(spark, orders_table, ORDERS, order(1, "placed", lsn=100))
    apply(
        spark,
        orders_table,
        ORDERS,
        order(1, "cancelled", lsn=200),
        order(1, "placed", lsn=300, deleted=True, store_id=0),  # placeholder-style payload
    )
    row = state(orders_table, ORDERS)[(1,)]
    assert (row["status"], row["store_id"], row["is_deleted"]) == ("cancelled", 3, True)
    assert row["source_lsn"] == 300


def test_insert_update_delete_all_in_one_batch(spark, orders_table):
    apply(
        spark,
        orders_table,
        ORDERS,
        order(1, "placed", lsn=100),
        order(1, "cancelled", lsn=200),
        order(1, "placed", lsn=300, deleted=True, store_id=0),
    )
    row = state(orders_table, ORDERS)[(1,)]
    assert (row["status"], row["store_id"], row["is_deleted"]) == ("cancelled", 3, True)


def test_delete_for_unseen_key_is_kept_as_deleted_row(spark, orders_table):
    apply(spark, orders_table, ORDERS, order(9, "cancelled", lsn=500, deleted=True))
    row = state(orders_table, ORDERS)[(9,)]
    assert (row["status"], row["is_deleted"]) == ("cancelled", True)


def _without_timestamps(rows):
    return {k: {c: v for c, v in r.items() if c != "silver_updated_at"} for k, r in rows.items()}


def test_replayed_delete_after_rebuild_is_idempotent(spark, orders_table):
    events = [
        order(1, "placed", lsn=100),
        order(1, "cancelled", lsn=200),
        order(1, "cancelled", lsn=300, deleted=True),
    ]
    apply(spark, orders_table, ORDERS, *events)
    first = state(orders_table, ORDERS)
    apply(spark, orders_table, ORDERS, *events)  # full replay, e.g. Silver rebuilt from Bronze
    second = state(orders_table, ORDERS)
    assert _without_timestamps(first) == _without_timestamps(second)
    assert first[(1,)]["is_deleted"] is True
