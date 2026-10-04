"""Typed schemas of the source tables, as Silver stores them.

Debezium sends NUMERIC as strings (decimal.handling.mode=string) and timestamptz as ISO-8601
strings; Silver casts them to DECIMAL and TIMESTAMP here. Column order is the Silver order.
"""

from dataclasses import dataclass

# Spark SQL types, as used in DDL and casts.
INT = "INT"
BIGINT = "BIGINT"
STRING = "STRING"
BOOLEAN = "BOOLEAN"
TIMESTAMP = "TIMESTAMP"
MONEY = "DECIMAL(12,2)"
PERCENT = "DECIMAL(5,2)"


@dataclass(frozen=True)
class TableSpec:
    name: str
    keys: tuple[str, ...]
    columns: tuple[tuple[str, str], ...]  # (column, Spark SQL type), business columns only

    @property
    def column_names(self) -> list[str]:
        return [c for c, _ in self.columns]


FACTS: dict[str, TableSpec] = {
    "orders": TableSpec(
        name="orders",
        keys=("order_id",),
        columns=(
            ("order_id", BIGINT),
            ("store_id", INT),
            ("rep_id", INT),
            ("order_ts", TIMESTAMP),
            ("status", STRING),
            ("updated_at", TIMESTAMP),
        ),
    ),
    "order_items": TableSpec(
        name="order_items",
        keys=("order_item_id",),
        columns=(
            ("order_item_id", BIGINT),
            ("order_id", BIGINT),
            ("product_id", INT),
            ("quantity", INT),
            ("unit_price", MONEY),
            ("discount_pct", PERCENT),
            ("updated_at", TIMESTAMP),
        ),
    ),
    "inventory": TableSpec(
        name="inventory",
        keys=("product_id", "warehouse"),
        columns=(
            ("product_id", INT),
            ("warehouse", STRING),
            ("qty_on_hand", INT),
            ("updated_at", TIMESTAMP),
        ),
    ),
}
