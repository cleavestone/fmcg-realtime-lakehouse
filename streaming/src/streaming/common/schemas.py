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

# Regions never change in this domain, so they're kept as plain current state alongside the
# facts (Gold's dim_store needs region names) rather than as an SCD2 dimension.
FACTS["regions"] = TableSpec(
    name="regions",
    keys=("region_id",),
    columns=(
        ("region_id", INT),
        ("name", STRING),
        ("country", STRING),
        ("updated_at", TIMESTAMP),
    ),
)


@dataclass(frozen=True)
class DimensionSpec:
    """An SCD Type 2 dimension built from one source table.

    `tracked` are the attributes whose change creates a new version (hashed into row_hash);
    updated_at is deliberately excluded, since the trigger bumps it on every UPDATE, even a no-op.
    """

    source: str  # bronze table
    name: str  # silver table
    key: str  # natural key
    surrogate: str  # one value per version
    columns: tuple[tuple[str, str], ...]  # natural key first, then business columns
    tracked: tuple[str, ...]

    @property
    def column_names(self) -> list[str]:
        return [c for c, _ in self.columns]


DIMENSIONS: dict[str, DimensionSpec] = {
    "stores": DimensionSpec(
        source="stores",
        name="dim_store",
        key="store_id",
        surrogate="store_sk",
        columns=(
            ("store_id", INT),
            ("name", STRING),
            ("channel", STRING),
            ("region_id", INT),
            ("tier", STRING),
            ("credit_limit", MONEY),
            ("updated_at", TIMESTAMP),
        ),
        tracked=("name", "channel", "region_id", "tier", "credit_limit"),
    ),
    "products": DimensionSpec(
        source="products",
        name="dim_product",
        key="product_id",
        surrogate="product_sk",
        columns=(
            ("product_id", INT),
            ("sku", STRING),
            ("name", STRING),
            ("brand", STRING),
            ("category", STRING),
            ("pack_size", STRING),
            ("unit_price", MONEY),
            ("is_active", BOOLEAN),
            ("updated_at", TIMESTAMP),
        ),
        tracked=("sku", "name", "brand", "category", "pack_size", "unit_price", "is_active"),
    ),
    "sales_reps": DimensionSpec(
        source="sales_reps",
        name="dim_sales_rep",
        key="rep_id",
        surrogate="rep_sk",
        columns=(
            ("rep_id", INT),
            ("name", STRING),
            ("region_id", INT),
            ("updated_at", TIMESTAMP),
        ),
        tracked=("name", "region_id"),
    ),
}
