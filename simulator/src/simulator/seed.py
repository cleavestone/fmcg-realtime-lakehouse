"""Deterministic seed: master data plus ~30 days of historical orders.

`generate()` is pure (config + clock in, rows out) so it can be unit-tested without a
database. `run_seed()` loads the rows in a single transaction and is a no-op when the
database is already populated.

Determinism: master data, the product popularity ranking and inventory are identical for
a given random_seed. Historical orders are laid out relative to `now` so the history always
ends today; for the same seed and clock time they are identical too. Each section draws from
its own random stream, so a different order count never shifts master data or inventory.
"""

import logging
import random
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import psycopg
from faker import Faker

from simulator import db
from simulator.catalog import Product, build_products

log = logging.getLogger(__name__)


@dataclass
class SeedData:
    """Seed rows as tuples in insert-column order (see _INSERTS)."""

    regions: list[tuple] = field(default_factory=list)
    stores: list[tuple] = field(default_factory=list)
    products: list[tuple] = field(default_factory=list)
    sales_reps: list[tuple] = field(default_factory=list)
    orders: list[tuple] = field(default_factory=list)
    order_items: list[tuple] = field(default_factory=list)
    inventory: list[tuple] = field(default_factory=list)


def _weighted(rng: random.Random, weights: dict[str, float]) -> str:
    keys = list(weights)
    return rng.choices(keys, weights=[weights[k] for k in keys])[0]


def _store_name(fake: Faker, rng: random.Random, channel: str) -> str:
    if channel == "kiosk":
        return rng.choice([f"{fake.first_name()}'s Kiosk", f"Duka la {fake.first_name()}"])
    if channel == "supermarket":
        return f"{fake.last_name()} Supermarket"
    return f"{fake.last_name()} Wholesalers"


def _status_for_age(cfg: dict, age_hours: float, rng: random.Random) -> str:
    for bucket in cfg["status_by_age"]:
        if age_hours <= bucket["max_age_hours"]:
            return _weighted(rng, bucket["weights"])
    return _weighted(rng, cfg["status_by_age"][-1]["weights"])


def _pick_products(
    rng: random.Random, product_ids: list[int], weights: list[float], k: int
) -> list[int]:
    """Weighted sample of k distinct products (popular SKUs appear far more often)."""
    chosen: list[int] = []
    while len(chosen) < k:
        pid = rng.choices(product_ids, weights=weights)[0]
        if pid not in chosen:
            chosen.append(pid)
    return chosen


def generate(cfg: dict[str, Any], now: datetime) -> SeedData:
    """Build every seed row in memory. Same cfg + same `now` -> identical output."""
    seed = cfg["random_seed"]
    rng = random.Random(f"{seed}:master")
    order_rng = random.Random(f"{seed}:orders")
    inv_rng = random.Random(f"{seed}:inventory")
    fake = Faker("en_KE")
    fake.seed_instance(cfg["random_seed"])
    tz = timezone(timedelta(hours=cfg["timezone_offset_hours"]))
    data = SeedData()

    # Regions
    region_cfg = cfg["regions"]
    for i, r in enumerate(region_cfg, start=1):
        data.regions.append((i, r["name"], cfg["country"]))
    region_ids = [r[0] for r in data.regions]
    region_weights = [r["weight"] for r in region_cfg]

    # Stores
    channels = cfg["channels"]
    channel_shares = {name: c["share"] for name, c in channels.items()}
    for store_id in range(1, cfg["stores"] + 1):
        channel = _weighted(rng, channel_shares)
        lo, hi = channels[channel]["credit_limit"]
        data.stores.append(
            (
                store_id,
                _store_name(fake, rng, channel),
                channel,
                rng.choices(region_ids, weights=region_weights)[0],
                _weighted(rng, cfg["tiers"]),
                Decimal(rng.randrange(lo, hi + 1, 1000)).quantize(Decimal("0.01")),
            )
        )

    # Products, with a shuffled Zipf popularity ranking
    catalog: list[Product] = build_products()
    for pid, p in enumerate(catalog, start=1):
        data.products.append((pid, p.sku, p.name, p.brand, p.category, p.pack_size, p.unit_price))
    product_ids = [p[0] for p in data.products]
    ranking = product_ids[:]
    rng.shuffle(ranking)
    popularity = {
        pid: 1 / (rank ** cfg["popularity_alpha"]) for rank, pid in enumerate(ranking, start=1)
    }
    product_weights = [popularity[pid] for pid in product_ids]
    price_of = {p[0]: p[6] for p in data.products}
    is_case = {p[0]: " x" in p[5] for p in data.products}

    # Sales reps, round-robin across regions
    for rep_id in range(1, cfg["sales_reps"] + 1):
        region_id = region_ids[(rep_id - 1) % len(region_ids)]
        data.sales_reps.append((rep_id, f"{fake.first_name()} {fake.last_name()}", region_id))
    reps_by_region: dict[int, list[int]] = {}
    for rep_id, _, region_id in data.sales_reps:
        reps_by_region.setdefault(region_id, []).append(rep_id)

    # Historical orders: volume by weekday, store choice by region weight x channel frequency
    store_ids = [s[0] for s in data.stores]
    store_weights = [
        region_weights[s[3] - 1] * channels[s[2]]["order_frequency"] for s in data.stores
    ]
    store_by_id = {s[0]: s for s in data.stores}
    open_h, close_h = cfg["business_hours"]
    local_now = now.astimezone(tz)
    first_day = (local_now - timedelta(days=cfg["history_days"])).date()

    timestamps: list[datetime] = []
    for offset in range(cfg["history_days"] + 1):
        day = first_day + timedelta(days=offset)
        factor = cfg["weekday_factors"][day.weekday()]
        n_orders = round(cfg["orders_per_day"] * factor * order_rng.uniform(0.85, 1.15))
        day_start = datetime(day.year, day.month, day.day, open_h, tzinfo=tz)
        for _ in range(n_orders):
            ts = day_start + timedelta(seconds=order_rng.randrange((close_h - open_h) * 3600))
            if ts <= local_now:
                timestamps.append(ts)
    timestamps.sort()

    item_id = 0
    for order_id, ts in enumerate(timestamps, start=1):
        store = store_by_id[order_rng.choices(store_ids, weights=store_weights)[0]]
        _, _, channel, region_id, tier, _ = store
        status = _status_for_age(cfg, (local_now - ts).total_seconds() / 3600, order_rng)
        data.orders.append(
            (order_id, store[0], order_rng.choice(reps_by_region[region_id]), ts, status)
        )

        ch = channels[channel]
        n_lines = order_rng.randint(*ch["lines"])
        for pid in _pick_products(order_rng, product_ids, product_weights, n_lines):
            qty = order_rng.randint(*ch["qty"])
            if is_case[pid]:
                qty = max(1, qty // cfg["case_qty_divisor"])
            discount = Decimal(str(order_rng.choice(cfg["tier_discounts"][tier]))).quantize(
                Decimal("0.01")
            )
            item_id += 1
            data.order_items.append((item_id, order_id, pid, qty, price_of[pid], discount))

    # Inventory: current stock per product and warehouse; a slice starts low for restocks
    inv = cfg["inventory"]
    for pid in product_ids:
        for wh in cfg["warehouses"]:
            low = inv_rng.random() < inv["low_stock_share"]
            qty = inv_rng.randint(*(inv["low_stock_qty"] if low else inv["qty"]))
            data.inventory.append((pid, wh, qty))

    return data


_INSERTS = {
    "regions": "INSERT INTO regions (region_id, name, country) OVERRIDING SYSTEM VALUE "
    "VALUES (%s, %s, %s)",
    "stores": "INSERT INTO stores (store_id, name, channel, region_id, tier, credit_limit) "
    "OVERRIDING SYSTEM VALUE VALUES (%s, %s, %s, %s, %s, %s)",
    "products": "INSERT INTO products "
    "(product_id, sku, name, brand, category, pack_size, unit_price) "
    "OVERRIDING SYSTEM VALUE VALUES (%s, %s, %s, %s, %s, %s, %s)",
    "sales_reps": "INSERT INTO sales_reps (rep_id, name, region_id) OVERRIDING SYSTEM VALUE "
    "VALUES (%s, %s, %s)",
    "orders": "INSERT INTO orders (order_id, store_id, rep_id, order_ts, status) "
    "OVERRIDING SYSTEM VALUE VALUES (%s, %s, %s, %s, %s)",
    "order_items": "INSERT INTO order_items "
    "(order_item_id, order_id, product_id, quantity, unit_price, discount_pct) "
    "OVERRIDING SYSTEM VALUE VALUES (%s, %s, %s, %s, %s, %s)",
    "inventory": "INSERT INTO inventory (product_id, warehouse, qty_on_hand) VALUES (%s, %s, %s)",
}

# Identity columns to move past the explicit seed ids, so later inserts don't collide.
_IDENTITIES = {
    "regions": "region_id",
    "stores": "store_id",
    "products": "product_id",
    "sales_reps": "rep_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}


def load(conn: psycopg.Connection, data: SeedData) -> None:
    with conn.cursor() as cur:
        for table, sql in _INSERTS.items():
            cur.executemany(sql, getattr(data, table))
        for table, column in _IDENTITIES.items():
            cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}', '{column}'), "
                f"(SELECT max({column}) FROM {table}))"
            )


def run_seed(cfg: dict[str, Any]) -> bool:
    """Seed an empty database. Returns False (and changes nothing) if already populated."""
    started = time.monotonic()
    with db.connect() as conn, conn.transaction():
        # Serialise concurrent seed runs, then check for existing data.
        conn.execute("LOCK TABLE regions IN EXCLUSIVE MODE")
        if conn.execute("SELECT EXISTS (SELECT 1 FROM regions)").fetchone()[0]:
            log.info("seed skipped: database already populated")
            return False
        data = generate(cfg, datetime.now(UTC))
        load(conn, data)

    counts = " ".join(f"{t}={len(getattr(data, t))}" for t in _INSERTS)
    log.info("seed complete in %.1fs: %s", time.monotonic() - started, counts)
    return True
