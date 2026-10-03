"""One function per business event.

Each event runs inside a transaction opened by the caller and either returns an
EventResult or raises Skip when there is no valid target. Raising inside the caller's
`with conn.transaction():` rolls everything back, so an event is all-or-nothing.
"""

import random
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import psycopg
from faker import Faker

from simulator import rules


class Skip(Exception):
    """No valid target for this event right now; nothing was written."""


@dataclass
class EventResult:
    event: str
    entity_id: int | str
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class Context:
    """Everything events need besides the connection."""

    cfg: dict[str, Any]
    rng: random.Random
    fake: Faker
    region_weight: dict[int, float] = field(default_factory=dict)
    region_warehouse: dict[int, str] = field(default_factory=dict)
    popularity: dict[int, float] = field(default_factory=dict)
    # Scenario fixtures: never picked at random (see config simulator.reserved).
    reserved_stores: list[int] = field(default_factory=list)
    reserved_products: list[int] = field(default_factory=list)


def build_context(conn: psycopg.Connection, cfg: dict[str, Any], seed: int) -> Context:
    fake = Faker("en_KE")
    fake.seed_instance(seed)
    reserved = cfg.get("reserved", {})
    ctx = Context(
        cfg=cfg,
        rng=random.Random(seed),
        fake=fake,
        reserved_stores=list(reserved.get("store_ids", [])),
        reserved_products=list(reserved.get("product_ids", [])),
    )
    by_name = {r["name"]: r for r in cfg["regions"]}
    for region_id, name in conn.execute("SELECT region_id, name FROM regions"):
        region = by_name.get(name, {"weight": 0.1, "warehouse": cfg["warehouses"][0]})
        ctx.region_weight[region_id] = region["weight"]
        ctx.region_warehouse[region_id] = region["warehouse"]
    refresh_popularity(conn, ctx)
    return ctx


def refresh_popularity(conn: psycopg.Connection, ctx: Context) -> None:
    """Learn product popularity from order history (+1 smoothing for unseen SKUs)."""
    counts = dict(conn.execute("SELECT product_id, count(*) FROM order_items GROUP BY product_id"))
    product_ids = [r[0] for r in conn.execute("SELECT product_id FROM products")]
    ctx.popularity = {pid: counts.get(pid, 0) + 1 for pid in product_ids}


# --- Orders ---------------------------------------------------------------------


def new_order(conn: psycopg.Connection, ctx: Context, store_id: int | None = None) -> EventResult:
    """INSERT order + 1-8 lines at current prices, decrementing stock in the same transaction."""
    if store_id is None:
        stores = conn.execute(
            "SELECT store_id, channel, region_id, tier FROM stores WHERE NOT (store_id = ANY(%s))",
            (ctx.reserved_stores,),
        ).fetchall()
    else:
        stores = conn.execute(
            "SELECT store_id, channel, region_id, tier FROM stores WHERE store_id = %s",
            (store_id,),
        ).fetchall()
    if not stores:
        raise Skip(f"store {store_id} not found" if store_id else "no stores")
    weights = [
        ctx.region_weight.get(s[2], 0.1) * ctx.cfg["channels"][s[1]]["order_frequency"]
        for s in stores
    ]
    store_id, channel, region_id, tier = ctx.rng.choices(stores, weights=weights)[0]

    reps = [
        r[0]
        for r in conn.execute("SELECT rep_id FROM sales_reps WHERE region_id = %s", (region_id,))
    ]
    if not reps:  # region left without a rep: any rep covers it
        reps = [r[0] for r in conn.execute("SELECT rep_id FROM sales_reps")]
    rep_id = ctx.rng.choice(reps)

    active = [r[0] for r in conn.execute("SELECT product_id FROM products WHERE is_active")]
    if not active:
        raise Skip("no active products")
    shape = ctx.cfg["channels"][channel]
    wanted = rules.pick_distinct(
        ctx.rng,
        active,
        [ctx.popularity.get(pid, 1) for pid in active],
        ctx.rng.randint(*shape["lines"]),
    )

    # Lock prices (still active?) and stock rows in a stable order to avoid deadlocks.
    products = {
        pid: (price, pack)
        for pid, price, pack in conn.execute(
            "SELECT product_id, unit_price, pack_size FROM products "
            "WHERE product_id = ANY(%s) AND is_active ORDER BY product_id FOR SHARE",
            (wanted,),
        )
    }
    warehouse = ctx.region_warehouse[region_id]
    stock = dict(
        conn.execute(
            "SELECT product_id, qty_on_hand FROM inventory "
            "WHERE warehouse = %s AND product_id = ANY(%s) ORDER BY product_id FOR UPDATE",
            (warehouse, list(products)),
        )
    )

    lines = []
    for pid in wanted:
        if pid not in products:
            continue
        price, pack = products[pid]
        requested = rules.line_quantity(
            ctx.rng, shape["qty"], " x" in pack, ctx.cfg["case_qty_divisor"]
        )
        qty = rules.allocate(requested, stock.get(pid, 0))
        if qty:
            lines.append(
                (pid, qty, price, rules.discount(ctx.rng, ctx.cfg["tier_discounts"][tier]))
            )
    if not lines:
        raise Skip(f"no stock in {warehouse} for any requested line")

    order_id = conn.execute(
        "INSERT INTO orders (store_id, rep_id, order_ts) VALUES (%s, %s, now()) RETURNING order_id",
        (store_id, rep_id),
    ).fetchone()[0]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount_pct) "
            "VALUES (%s, %s, %s, %s, %s)",
            [(order_id, pid, qty, price, disc) for pid, qty, price, disc in lines],
        )
        cur.executemany(
            "UPDATE inventory SET qty_on_hand = qty_on_hand - %s "
            "WHERE product_id = %s AND warehouse = %s",
            [(qty, pid, warehouse) for pid, qty, _, _ in lines],
        )
    value = sum(qty * price * (1 - disc / 100) for _, qty, price, disc in lines)
    return EventResult(
        "new_order",
        order_id,
        {
            "store": store_id,
            "channel": channel,
            "lines": len(lines),
            "units": sum(q for _, q, _, _ in lines),
            "value": Decimal(value).quantize(rules.CENT),
        },
    )


def advance_order(
    conn: psycopg.Connection, ctx: Context, order_ids: list[int] | None = None
) -> EventResult:
    """Move a dispatch batch of the oldest open orders one step forward."""
    if order_ids is None:
        candidates = conn.execute(
            "SELECT order_id FROM orders WHERE status IN ('placed', 'confirmed', 'shipped') "
            "AND NOT (store_id = ANY(%s)) ORDER BY order_ts LIMIT %s",
            (ctx.reserved_stores, ctx.cfg["advance_candidates"]),
        ).fetchall()
        if not candidates:
            raise Skip("no open orders")
        k = min(len(candidates), ctx.rng.randint(*ctx.cfg["advance_batch"]))
        order_ids = [r[0] for r in ctx.rng.sample(candidates, k)]

    rows = conn.execute(
        "SELECT order_id, status FROM orders WHERE order_id = ANY(%s) "
        "ORDER BY order_id FOR UPDATE SKIP LOCKED",
        (order_ids,),
    ).fetchall()
    moves = [(oid, status, rules.next_status(status)) for oid, status in rows]
    moves = [m for m in moves if m[2]]
    if not moves:
        raise Skip(f"orders {order_ids} are not open")
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE orders SET status = %s WHERE order_id = %s",
            [(new, oid) for oid, _, new in moves],
        )
    return EventResult(
        "advance_order",
        ",".join(str(m[0]) for m in moves),
        {"moves": " ".join(f"{oid}:{old}->{new}" for oid, old, new in moves)},
    )


def cancel_order(
    conn: psycopg.Connection, ctx: Context, order_id: int | None = None
) -> EventResult:
    """Cancel a placed/confirmed order and put its stock back."""
    if order_id is None:
        row = conn.execute(
            "SELECT o.order_id, o.status, s.region_id FROM orders o JOIN stores s USING (store_id) "
            "WHERE o.status IN ('placed', 'confirmed') AND NOT (o.store_id = ANY(%s)) "
            "ORDER BY random() LIMIT 1 FOR UPDATE OF o SKIP LOCKED",
            (ctx.reserved_stores,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT o.order_id, o.status, s.region_id FROM orders o JOIN stores s USING (store_id) "
            "WHERE o.order_id = %s FOR UPDATE OF o",
            (order_id,),
        ).fetchone()
    if row is None:
        raise Skip("no cancellable orders" if order_id is None else f"order {order_id} not found")
    order_id, status, region_id = row
    if not rules.can_cancel(status):
        raise Skip(f"order {order_id} is {status}")

    warehouse = ctx.region_warehouse[region_id]
    conn.execute("UPDATE orders SET status = 'cancelled' WHERE order_id = %s", (order_id,))
    restored = conn.execute(
        "UPDATE inventory i SET qty_on_hand = i.qty_on_hand + oi.quantity "
        "FROM order_items oi WHERE oi.order_id = %s "
        "AND i.product_id = oi.product_id AND i.warehouse = %s",
        (order_id, warehouse),
    ).rowcount
    return EventResult(
        "cancel_order",
        order_id,
        {"was": status, "lines_restocked": restored, "warehouse": warehouse},
    )


def hard_delete(conn: psycopg.Connection, ctx: Context, order_id: int | None = None) -> EventResult:
    """Purge an old cancelled order and its lines (exercises delete handling in CDC)."""
    if order_id is None:
        row = conn.execute(
            "SELECT order_id FROM orders WHERE status = 'cancelled' "
            "AND order_ts < now() - make_interval(hours => %s) AND NOT (store_id = ANY(%s)) "
            "ORDER BY order_ts LIMIT 1 FOR UPDATE SKIP LOCKED",
            (ctx.cfg["hard_delete_min_age_hours"], ctx.reserved_stores),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT order_id FROM orders WHERE order_id = %s AND status = 'cancelled' FOR UPDATE",
            (order_id,),
        ).fetchone()
    if row is None:
        raise Skip(
            "no old cancelled orders" if order_id is None else f"order {order_id} is not cancelled"
        )
    order_id = row[0]
    items = conn.execute("DELETE FROM order_items WHERE order_id = %s", (order_id,)).rowcount
    conn.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
    return EventResult("hard_delete", order_id, {"items_deleted": items})


# --- Inventory --------------------------------------------------------------------


def restock(conn: psycopg.Connection, ctx: Context) -> EventResult:
    """Top up one low-stock SKU in one warehouse."""
    row = conn.execute(
        "SELECT i.product_id, i.warehouse, i.qty_on_hand FROM inventory i "
        "JOIN products p USING (product_id) "
        "WHERE p.is_active AND i.qty_on_hand < %s "
        "ORDER BY random() LIMIT 1 FOR UPDATE OF i SKIP LOCKED",
        (ctx.cfg["restock"]["low_stock_threshold"],),
    ).fetchone()
    if row is None:
        raise Skip("no low-stock SKUs")
    product_id, warehouse, before = row
    added = ctx.rng.randint(*ctx.cfg["restock"]["qty"])
    conn.execute(
        "UPDATE inventory SET qty_on_hand = qty_on_hand + %s "
        "WHERE product_id = %s AND warehouse = %s",
        (added, product_id, warehouse),
    )
    return EventResult(
        "restock", product_id, {"warehouse": warehouse, "before": before, "after": before + added}
    )


# --- Slowly changing master data (SCD2 sources) -------------------------------------


def price_change(
    conn: psycopg.Connection, ctx: Context, product_id: int | None = None, pct: float | None = None
) -> EventResult:
    """Move one active product's price by +/- 3-10%."""
    if product_id is None:
        row = conn.execute(
            "SELECT product_id, unit_price FROM products "
            "WHERE is_active AND NOT (product_id = ANY(%s)) "
            "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED",
            (ctx.reserved_products,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT product_id, unit_price FROM products WHERE product_id = %s FOR UPDATE",
            (product_id,),
        ).fetchone()
    if row is None:
        raise Skip(
            "no active products" if product_id is None else f"product {product_id} not found"
        )
    product_id, old = row
    if pct is None:
        pct = rules.price_change_pct(ctx.rng, *ctx.cfg["price_change_pct"])
    new = rules.changed_price(old, pct)
    conn.execute("UPDATE products SET unit_price = %s WHERE product_id = %s", (new, product_id))
    return EventResult("price_change", product_id, {"old": old, "new": new, "pct": pct})


def store_change(
    conn: psycopg.Connection, ctx: Context, store_id: int | None = None, tier_only: bool = False
) -> EventResult:
    """Upgrade/downgrade a store's tier, or change its credit limit."""
    sql = "SELECT store_id, tier, credit_limit FROM stores"
    if store_id is None:
        row = conn.execute(
            sql
            + " WHERE NOT (store_id = ANY(%s)) ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED",
            (ctx.reserved_stores,),
        ).fetchone()
    else:
        row = conn.execute(sql + " WHERE store_id = %s FOR UPDATE", (store_id,)).fetchone()
    if row is None:
        raise Skip("no stores" if store_id is None else f"store {store_id} not found")
    store_id, tier, limit = row

    if tier_only or ctx.rng.random() < ctx.cfg["store_change_tier_share"]:
        new_tier = rules.step_tier(tier, ctx.rng)
        conn.execute("UPDATE stores SET tier = %s WHERE store_id = %s", (new_tier, store_id))
        return EventResult("store_change", store_id, {"tier": f"{tier}->{new_tier}"})

    pct = rules.price_change_pct(ctx.rng, *ctx.cfg["credit_change_pct"])
    new_limit = rules.changed_credit_limit(limit, pct)
    conn.execute("UPDATE stores SET credit_limit = %s WHERE store_id = %s", (new_limit, store_id))
    return EventResult("store_change", store_id, {"credit_limit": f"{limit}->{new_limit}"})


def new_store(conn: psycopg.Connection, ctx: Context) -> EventResult:
    """Onboard a new outlet; new customers start on the bronze tier."""
    channels = ctx.cfg["channels"]
    channel = rules.weighted_choice(ctx.rng, {c: v["share"] for c, v in channels.items()})
    regions = list(ctx.region_weight)
    region_id = ctx.rng.choices(regions, weights=[ctx.region_weight[r] for r in regions])[0]
    name = rules.store_name(ctx.fake, ctx.rng, channel)
    limit = rules.credit_limit(ctx.rng, *channels[channel]["credit_limit"])
    store_id = conn.execute(
        "INSERT INTO stores (name, channel, region_id, tier, credit_limit) "
        "VALUES (%s, %s, %s, 'bronze', %s) RETURNING store_id",
        (name, channel, region_id, limit),
    ).fetchone()[0]
    return EventResult(
        "new_store", store_id, {"name": name, "channel": channel, "region": region_id}
    )


def rep_reassigned(conn: psycopg.Connection, ctx: Context) -> EventResult:
    """Move a rep to another region, never leaving their current region without cover."""
    row = conn.execute(
        "SELECT rep_id, region_id FROM sales_reps r WHERE "
        "(SELECT count(*) FROM sales_reps x WHERE x.region_id = r.region_id) > 1 "
        "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED"
    ).fetchone()
    if row is None:
        raise Skip("every region has a single rep")
    rep_id, old_region = row
    new_region = ctx.rng.choice([r for r in ctx.region_weight if r != old_region])
    conn.execute("UPDATE sales_reps SET region_id = %s WHERE rep_id = %s", (new_region, rep_id))
    return EventResult("rep_reassigned", rep_id, {"region": f"{old_region}->{new_region}"})


def product_discontinued(conn: psycopg.Connection, ctx: Context) -> EventResult:
    """Delist an active SKU, keeping at least min_active_products on sale."""
    active = conn.execute("SELECT count(*) FROM products WHERE is_active").fetchone()[0]
    if active <= ctx.cfg["min_active_products"]:
        raise Skip(f"only {active} active products left")
    row = conn.execute(
        "SELECT product_id FROM products WHERE is_active AND NOT (product_id = ANY(%s)) "
        "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED",
        (ctx.reserved_products,),
    ).fetchone()
    if row is None:
        raise Skip("no discontinuable products")
    product_id = row[0]
    conn.execute("UPDATE products SET is_active = false WHERE product_id = %s", (product_id,))
    return EventResult("product_discontinued", product_id, {"active_left": active - 1})


EVENTS: dict[str, Callable[[psycopg.Connection, Context], EventResult]] = {
    "new_order": new_order,
    "advance_order": advance_order,
    "cancel_order": cancel_order,
    "restock": restock,
    "price_change": price_change,
    "store_change": store_change,
    "new_store": new_store,
    "rep_reassigned": rep_reassigned,
    "product_discontinued": product_discontinued,
    "hard_delete": hard_delete,
}
