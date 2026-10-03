"""Seed generation is deterministic and respects the schema's business rules (no DB needed)."""

from collections import Counter
from datetime import UTC, datetime, timedelta

import pytest
from simulator.catalog import build_products
from simulator.config import load_config
from simulator.seed import generate

NOW = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def cfg():
    return load_config()["seed"]


@pytest.fixture(scope="module")
def data(cfg):
    return generate(cfg, NOW)


def test_generate_is_deterministic(cfg, data):
    again = generate(cfg, NOW)
    assert again == data


def test_different_seed_changes_data(cfg, data):
    other = generate({**cfg, "random_seed": cfg["random_seed"] + 1}, NOW)
    assert other.stores != data.stores
    assert other.orders != data.orders


def test_master_data_counts(cfg, data):
    assert len(data.regions) == len(cfg["regions"]) == 5
    assert len(data.stores) == cfg["stores"]
    assert len(data.sales_reps) == cfg["sales_reps"]
    assert len(data.products) == 200
    assert len(data.inventory) == len(data.products) * len(cfg["warehouses"])


def test_catalog_skus_unique_and_realistic():
    products = build_products()
    assert len({p.sku for p in products}) == len(products)
    assert {p.category for p in products} == {
        "beverages",
        "dairy",
        "snacks",
        "personal_care",
        "household",
    }
    assert all(p.unit_price > 0 for p in products)


def test_orders_span_history_window(cfg, data):
    timestamps = [o[3] for o in data.orders]
    assert max(timestamps) <= NOW
    assert NOW - min(timestamps) >= timedelta(days=cfg["history_days"] - 1)
    assert timestamps == sorted(timestamps), "order ids follow order time"


def test_order_items_reference_valid_rows_and_copy_price(data):
    order_ids = {o[0] for o in data.orders}
    price = {p[0]: p[6] for p in data.products}
    lines_per_order = Counter()
    for _, order_id, product_id, qty, unit_price, discount in data.order_items:
        assert order_id in order_ids
        assert unit_price == price[product_id]
        assert qty > 0
        assert 0 <= discount <= 100
        lines_per_order[order_id] += 1
    assert set(lines_per_order) == order_ids, "every order has at least one line"
    assert max(lines_per_order.values()) <= 8


def test_no_duplicate_product_per_order(data):
    pairs = [(i[1], i[2]) for i in data.order_items]
    assert len(pairs) == len(set(pairs))


def test_reps_match_store_region(data):
    store_region = {s[0]: s[3] for s in data.stores}
    rep_region = {r[0]: r[2] for r in data.sales_reps}
    assert all(store_region[o[1]] == rep_region[o[2]] for o in data.orders)


def test_inventory_non_negative(data):
    assert all(qty >= 0 for _, _, qty in data.inventory)


def test_recent_orders_are_open_and_old_orders_are_closed(data):
    for _, _, _, ts, status in data.orders:
        age = NOW - ts
        if age > timedelta(days=3):
            assert status in {"delivered", "cancelled"}
        if age < timedelta(hours=6):
            assert status in {"placed", "confirmed", "cancelled"}


def test_master_data_and_inventory_independent_of_clock(cfg, data):
    later = generate(cfg, NOW + timedelta(hours=5, minutes=17))
    assert later.regions == data.regions
    assert later.stores == data.stores
    assert later.products == data.products
    assert later.sales_reps == data.sales_reps
    assert later.inventory == data.inventory
