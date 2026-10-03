"""Deterministic scenarios with known expected outcomes, used by the Verify steps.

Each scenario commits every change in its own transaction (so each one gets its own
LSN in the WAL), prints the ids it touched and the expected end state downstream, and
reads Postgres back to confirm the source side matches.
"""

import time
from collections.abc import Callable
from typing import Any

import psycopg

from simulator import db, rules
from simulator.events import (
    Context,
    EventResult,
    Skip,
    advance_order,
    build_context,
    cancel_order,
    hard_delete,
    new_order,
    price_change,
    store_change,
)

SCENARIO_SEED = 7  # fixed rng for scenario choices (tiers, lines), independent of the simulator


def _say(msg: str = "") -> None:
    print(msg, flush=True)


def _check(label: str, ok: bool) -> bool:
    _say(f"  [{'OK' if ok else 'MISMATCH'}] {label}")
    return ok


def _commit(conn: psycopg.Connection, fn: Callable[[], EventResult]) -> EventResult:
    with conn.transaction():
        return fn()


def _create_order(conn: psycopg.Connection, ctx: Context, store_id: int) -> EventResult:
    """new_order can skip when the chosen lines are out of stock; retry a few times."""
    for _ in range(10):
        try:
            return _commit(conn, lambda: new_order(conn, ctx, store_id=store_id))
        except Skip:
            continue
    raise SystemExit(f"could not create an order for store {store_id}: no stock")


def scd2_price_test(conn: psycopg.Connection, ctx: Context, cfg: dict[str, Any]) -> bool:
    product_id = cfg["product_id"]
    row = conn.execute(
        "SELECT sku, name, unit_price FROM products WHERE product_id = %s", (product_id,)
    ).fetchone()
    if row is None:
        raise SystemExit(f"product {product_id} not found")
    sku, name, start_price = row
    _say(f"scd2_price_test: product_id={product_id} sku={sku} name={name!r}")
    _say(f"  starting price: {start_price}")

    prices = []
    for i, pct in enumerate(cfg["price_changes_pct"], start=1):
        r = _commit(conn, lambda pct=pct: price_change(conn, ctx, product_id=product_id, pct=pct))
        prices.append(r.detail["new"])
        _say(f"  change {i}: {r.detail['old']} -> {r.detail['new']} ({pct:+}%)")
        time.sleep(cfg["pause_seconds"])

    _say("expected downstream (dim_product, Phase 7):")
    _say(f"  the last {len(prices) + 1} versions of product_id={product_id} have unit_price")
    _say(f"  {[str(start_price), *map(str, prices)]} in that order, with contiguous")
    _say(f"  validity windows, and exactly one is_current = true (unit_price={prices[-1]}).")
    _say("source check:")
    now_price = conn.execute(
        "SELECT unit_price FROM products WHERE product_id = %s", (product_id,)
    ).fetchone()[0]
    return _check(
        f"products.unit_price = {now_price} (expected {prices[-1]})", now_price == prices[-1]
    )


def scd2_burst_test(conn: psycopg.Connection, ctx: Context, cfg: dict[str, Any]) -> bool:
    store_id = cfg["store_id"]
    row = conn.execute("SELECT name, tier FROM stores WHERE store_id = %s", (store_id,)).fetchone()
    if row is None:
        raise SystemExit(f"store {store_id} not found")
    name, start_tier = row
    _say(f"scd2_burst_test: store_id={store_id} name={name!r} starting tier={start_tier}")

    tiers = []
    started = time.monotonic()
    for i in range(1, cfg["changes"] + 1):
        r = _commit(conn, lambda: store_change(conn, ctx, store_id=store_id, tier_only=True))
        tiers.append(r.detail["tier"].split("->")[1])
        _say(f"  change {i}: {r.detail['tier']}  (t+{time.monotonic() - started:.2f}s)")
        time.sleep(cfg["pause_seconds"])

    _say("expected downstream (dim_store, Phase 7):")
    _say(f"  {len(tiers)} new versions for store_id={store_id} with tiers {tiers},")
    _say("  even though all changes land in a single Spark micro-batch;")
    _say(f"  exactly one is_current = true (tier={tiers[-1]}).")
    _say("source check:")
    now_tier = conn.execute("SELECT tier FROM stores WHERE store_id = %s", (store_id,)).fetchone()[
        0
    ]
    return _check(f"stores.tier = {now_tier} (expected {tiers[-1]})", now_tier == tiers[-1])


def order_lifecycle_test(conn: psycopg.Connection, ctx: Context, cfg: dict[str, Any]) -> bool:
    store_id, pause = cfg["store_id"], cfg["pause_seconds"]
    _say(f"order_lifecycle_test: store_id={store_id}")

    first = _create_order(conn, ctx, store_id)
    first_id = first.entity_id
    _say(f"  order {first_id} created: {first.detail}")
    for _ in range(len(rules.NEXT_STATUS)):
        time.sleep(pause)
        r = _commit(conn, lambda: advance_order(conn, ctx, order_ids=[first_id]))
        _say(f"  {r.detail['moves']}")

    time.sleep(pause)
    second = _create_order(conn, ctx, store_id)
    second_id = second.entity_id
    _say(f"  order {second_id} created: {second.detail}")
    time.sleep(pause)
    r = _commit(conn, lambda: cancel_order(conn, ctx, order_id=second_id))
    _say(
        f"  order {second_id} cancelled (was {r.detail['was']}), "
        f"stock restored to {r.detail['warehouse']}"
    )

    _say("expected downstream (silver orders, Phase 6):")
    _say(f"  order {first_id}: status=delivered (Bronze holds 1 insert + 3 updates)")
    _say(f"  order {second_id}: status=cancelled (Bronze holds 1 insert + 1 update)")
    _say("source check:")
    status = dict(
        conn.execute(
            "SELECT order_id, status FROM orders WHERE order_id = ANY(%s)", ([first_id, second_id],)
        )
    )
    ok = _check(f"order {first_id} is {status.get(first_id)}", status.get(first_id) == "delivered")
    return (
        _check(
            f"order {second_id} is {status.get(second_id)}", status.get(second_id) == "cancelled"
        )
        and ok
    )


def delete_test(conn: psycopg.Connection, ctx: Context, cfg: dict[str, Any]) -> bool:
    store_id = cfg["store_id"]
    row = conn.execute(
        "SELECT order_id FROM orders WHERE status = 'cancelled' AND store_id = %s "
        "ORDER BY order_ts, order_id LIMIT 1",
        (store_id,),
    ).fetchone()
    if row is None:  # make one: create and cancel an order for the reserved store
        created = _create_order(conn, ctx, store_id)
        _commit(conn, lambda: cancel_order(conn, ctx, order_id=created.entity_id))
        _say(f"  (store {store_id} had none: created and cancelled order {created.entity_id})")
        row = (created.entity_id,)
    order_id = row[0]
    item_ids = [
        r[0]
        for r in conn.execute(
            "SELECT order_item_id FROM order_items WHERE order_id = %s ORDER BY 1", (order_id,)
        )
    ]
    _say(f"delete_test: hard-deleting cancelled order {order_id} with order_item_ids {item_ids}")
    r = _commit(conn, lambda: hard_delete(conn, ctx, order_id=order_id))
    _say(f"  deleted order {order_id} and {r.detail['items_deleted']} order_items")

    _say("expected downstream (silver, Phase 6):")
    _say(f"  orders row {order_id} and order_items {item_ids} remain with is_deleted = true")
    _say("source check:")
    left = conn.execute(
        "SELECT (SELECT count(*) FROM orders WHERE order_id = %(id)s)"
        " + (SELECT count(*) FROM order_items WHERE order_id = %(id)s)",
        {"id": order_id},
    ).fetchone()[0]
    return _check(f"order {order_id} and its items are gone from Postgres", left == 0)


SCENARIOS: dict[str, Callable[[psycopg.Connection, Context, dict[str, Any]], bool]] = {
    "scd2_price_test": scd2_price_test,
    "scd2_burst_test": scd2_burst_test,
    "order_lifecycle_test": order_lifecycle_test,
    "delete_test": delete_test,
}


def run_scenario(name: str, sim_cfg: dict[str, Any], scenario_cfg: dict[str, Any]) -> bool:
    with db.connect() as conn:
        conn.autocommit = True  # every step commits in its own transaction
        ctx = build_context(conn, sim_cfg, SCENARIO_SEED)
        return SCENARIOS[name](conn, ctx, scenario_cfg.get(name, {}))
