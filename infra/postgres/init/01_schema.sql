-- FMCG distributor OLTP schema (public).
-- Runs once, on first start of an empty data volume.

CREATE TABLE regions (
    region_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    country TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE stores (
    store_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    channel TEXT NOT NULL,
    region_id INTEGER NOT NULL REFERENCES regions (region_id),
    tier TEXT NOT NULL DEFAULT 'bronze',
    credit_limit NUMERIC(12, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT stores_channel_chk CHECK (channel IN ('supermarket', 'kiosk', 'wholesale')),
    CONSTRAINT stores_tier_chk CHECK (tier IN ('bronze', 'silver', 'gold')),
    CONSTRAINT stores_credit_limit_chk CHECK (credit_limit >= 0)
);

CREATE TABLE products (
    product_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    brand TEXT NOT NULL,
    category TEXT NOT NULL,
    pack_size TEXT NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT products_category_chk CHECK (
        category IN ('beverages', 'dairy', 'snacks', 'personal_care', 'household')
    ),
    CONSTRAINT products_unit_price_chk CHECK (unit_price > 0)
);

CREATE TABLE sales_reps (
    rep_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    region_id INTEGER NOT NULL REFERENCES regions (region_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_id INTEGER NOT NULL REFERENCES stores (store_id),
    rep_id INTEGER NOT NULL REFERENCES sales_reps (rep_id),
    order_ts TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'placed',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT orders_status_chk CHECK (
        status IN ('placed', 'confirmed', 'shipped', 'delivered', 'cancelled')
    )
);

CREATE TABLE order_items (
    order_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders (order_id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products (product_id),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    discount_pct NUMERIC(5, 2) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT order_items_quantity_chk CHECK (quantity > 0),
    CONSTRAINT order_items_unit_price_chk CHECK (unit_price > 0),
    CONSTRAINT order_items_discount_chk CHECK (discount_pct BETWEEN 0 AND 100),
    CONSTRAINT order_items_order_product_uq UNIQUE (order_id, product_id)
);

CREATE TABLE inventory (
    product_id INTEGER NOT NULL REFERENCES products (product_id),
    warehouse TEXT NOT NULL,
    qty_on_hand INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, warehouse),
    CONSTRAINT inventory_qty_chk CHECK (qty_on_hand >= 0)
);

CREATE INDEX stores_region_id_idx ON stores (region_id);
CREATE INDEX sales_reps_region_id_idx ON sales_reps (region_id);
CREATE INDEX orders_store_id_idx ON orders (store_id);
CREATE INDEX orders_rep_id_idx ON orders (rep_id);
CREATE INDEX orders_status_idx ON orders (status);
CREATE INDEX orders_order_ts_idx ON orders (order_ts);
CREATE INDEX order_items_product_id_idx ON order_items (product_id);

-- CDC: log the full old row on UPDATE/DELETE. With the default identity (primary key only)
-- a Debezium delete event carries placeholder values (0, 1970-01-01, column defaults) for
-- every non-key NOT NULL column; FULL makes delete events carry the real last state.
ALTER TABLE regions REPLICA IDENTITY FULL;
ALTER TABLE stores REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE sales_reps REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
ALTER TABLE inventory REPLICA IDENTITY FULL;
