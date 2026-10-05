"""SCD Type 2 versioning: the cases from the build plan plus edge cases.

Each test feeds Bronze-shaped events through parse -> build_changes -> merge_changes into a
local Delta table, exactly as the streaming job does per micro-batch.
"""

import json
import uuid
from datetime import UTC, datetime
from itertools import pairwise

import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from streaming.common.schemas import DIMENSIONS
from streaming.silver.facts import parse
from streaming.silver.scd2 import (
    BEGINNING_OF_TIME,
    END_OF_TIME,
    build_changes,
    dimension_ddl,
    merge_changes,
)

STORES = DIMENSIONS["stores"]
BRONZE_SCHEMA = (
    "payload string, op string, source_lsn bigint, source_ts_ms bigint, is_deleted boolean, "
    "kafka_offset bigint"
)
T0 = 1_791_000_000_000  # an epoch-millis commit time


def store(
    lsn, tier="bronze", op="u", credit="20000.00", name="Duka la Joseph", deleted=False, store_id=1
):
    payload = {
        "store_id": store_id,
        "name": name,
        "channel": "kiosk",
        "region_id": 4,
        "tier": tier,
        "credit_limit": credit,
        # updated_at changes on every UPDATE (trigger), even no-ops; it isn't tracked.
        "updated_at": f"2026-10-05T09:00:{lsn % 60:02d}Z",
    }
    return (json.dumps(payload), "d" if deleted else op, lsn, T0 + lsn, deleted, lsn)


def ms(lsn):
    return datetime.fromtimestamp((T0 + lsn) / 1000, tz=UTC).replace(tzinfo=None)


@pytest.fixture
def dim(spark, tmp_path):
    name = f"dim_store_{uuid.uuid4().hex[:8]}"
    spark.sql(
        f"CREATE TABLE {name} ({dimension_ddl(STORES)}) USING DELTA LOCATION '{tmp_path / name}'"
    )
    return DeltaTable.forName(spark, name)


def batch(spark, target, *events):
    df = parse(spark.createDataFrame(list(events), BRONZE_SCHEMA), STORES)
    keys = df.select(STORES.key).distinct()
    existing = target.toDF().join(keys, STORES.key, "left_semi")
    merge_changes(target, build_changes(df, existing, STORES), STORES)


def versions(target, store_id=1):
    rows = target.toDF().filter(F.col("store_id") == store_id).orderBy("valid_from", "source_lsn")
    return [r.asDict() for r in rows.collect()]


def assert_well_formed(vs, expect_current=True):
    """Contiguous, non-overlapping windows, and the right number of current versions."""
    for a, b in pairwise(vs):
        assert a["valid_to"] == b["valid_from"], "windows must be contiguous"
        assert not a["is_current"]
    for v in vs:
        assert v["valid_from"] <= v["valid_to"]
    assert sum(v["is_current"] for v in vs) == (1 if expect_current else 0)
    if expect_current:
        assert vs[-1]["is_current"] and vs[-1]["valid_to"] == END_OF_TIME
    assert len({v["store_sk"] for v in vs}) == len(vs), "one surrogate key per version"


# --- new keys ---------------------------------------------------------------------------


def test_snapshot_row_becomes_first_version_valid_from_beginning_of_time(spark, dim):
    batch(spark, dim, store(100, op="r"))
    (v,) = versions(dim)
    assert (v["valid_from"], v["valid_to"], v["is_current"]) == (
        BEGINNING_OF_TIME,
        END_OF_TIME,
        True,
    )
    assert v["tier"] == "bronze" and v["source_lsn"] == 100 and not v["is_deleted"]


def test_live_insert_is_valid_from_its_commit_time(spark, dim):
    batch(spark, dim, store(500, op="c", store_id=7))
    (v,) = versions(dim, store_id=7)
    assert v["valid_from"] == ms(500) and v["is_current"]


# --- changes ------------------------------------------------------------------------------


def test_attribute_change_closes_old_version_and_opens_new(spark, dim):
    batch(spark, dim, store(100, op="r"))
    batch(spark, dim, store(200, tier="silver"))
    vs = versions(dim)
    assert [v["tier"] for v in vs] == ["bronze", "silver"]
    assert vs[0]["valid_to"] == ms(200) == vs[1]["valid_from"]
    assert_well_formed(vs)


def test_noop_update_creates_no_version(spark, dim):
    batch(spark, dim, store(100, op="r"))
    batch(spark, dim, store(200))  # only updated_at differs
    (v,) = versions(dim)
    assert v["source_lsn"] == 100 and v["is_current"]


def test_multiple_changes_in_one_batch_each_become_a_version(spark, dim):
    # scd2_burst_test: three tier changes within a second -> one micro-batch.
    batch(spark, dim, store(100, op="r"))
    batch(
        spark,
        dim,
        store(300, tier="gold"),
        store(200, tier="silver"),  # arrival order scrambled: LSN decides
        store(400, tier="silver"),
    )
    vs = versions(dim)
    assert [v["tier"] for v in vs] == ["bronze", "silver", "gold", "silver"]
    assert [v["valid_from"] for v in vs[1:]] == [ms(200), ms(300), ms(400)]
    assert_well_formed(vs)


def test_noops_inside_a_batch_are_skipped(spark, dim):
    batch(spark, dim, store(100, op="r"))
    batch(spark, dim, store(200, tier="silver"), store(250, tier="silver"), store(300, tier="gold"))
    vs = versions(dim)
    assert [(v["tier"], v["source_lsn"]) for v in vs] == [
        ("bronze", 100),
        ("silver", 200),
        ("gold", 300),
    ]
    assert_well_formed(vs)


def test_snapshot_and_changes_in_the_same_first_batch(spark, dim):
    # Fresh start: Bronze's first batch holds the snapshot row and later live changes.
    batch(spark, dim, store(100, op="r"), store(200, tier="silver"))
    vs = versions(dim)
    assert [(v["tier"], v["valid_from"]) for v in vs] == [
        ("bronze", BEGINNING_OF_TIME),
        ("silver", ms(200)),
    ]
    assert_well_formed(vs)


def test_credit_limit_change_creates_a_version(spark, dim):
    batch(spark, dim, store(100, op="r"))
    batch(spark, dim, store(200, credit="25000.00"))
    vs = versions(dim)
    assert [str(v["credit_limit"]) for v in vs] == ["20000.00", "25000.00"]


# --- deletes -------------------------------------------------------------------------------


def test_delete_closes_current_version_and_marks_it_deleted(spark, dim):
    batch(spark, dim, store(100, op="r"))
    batch(spark, dim, store(200, deleted=True))
    (v,) = versions(dim)
    assert (v["is_current"], v["is_deleted"], v["valid_to"]) == (False, True, ms(200))


def test_change_then_delete_in_one_batch(spark, dim):
    batch(spark, dim, store(100, op="r"))
    batch(spark, dim, store(200, tier="gold"), store(300, tier="gold", deleted=True))
    vs = versions(dim)
    assert [(v["tier"], v["is_current"], v["is_deleted"]) for v in vs] == [
        ("bronze", False, False),
        ("gold", False, True),
    ]
    assert vs[1]["valid_to"] == ms(300)
    assert_well_formed(vs, expect_current=False)


# --- replays ---------------------------------------------------------------------------------


def test_replaying_an_old_batch_changes_nothing(spark, dim):
    first = [store(100, op="r")]
    second = [store(200, tier="silver"), store(300, tier="gold")]
    batch(spark, dim, *first)
    batch(spark, dim, *second)
    before = versions(dim)
    batch(spark, dim, *second)  # crash before checkpoint -> same batch again
    batch(spark, dim, *first, *second)  # full rebuild-style replay
    after = versions(dim)

    def strip(vs):
        return [{k: v for k, v in r.items() if k != "silver_updated_at"} for r in vs]

    assert strip(after) == strip(before)
    assert_well_formed(after)


def test_replayed_delete_is_idempotent(spark, dim):
    batch(spark, dim, store(100, op="r"))
    batch(spark, dim, store(200, deleted=True))
    batch(spark, dim, store(200, deleted=True))
    (v,) = versions(dim)
    assert (v["is_current"], v["is_deleted"]) == (False, True)


def test_keys_are_versioned_independently(spark, dim):
    batch(spark, dim, store(100, op="r", store_id=1), store(101, op="r", store_id=2))
    batch(spark, dim, store(200, tier="gold", store_id=1))
    assert len(versions(dim, store_id=1)) == 2
    assert len(versions(dim, store_id=2)) == 1
