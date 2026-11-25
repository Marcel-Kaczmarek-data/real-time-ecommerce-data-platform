-- Operational source database (system of record for master data).
-- Transactional events (orders, payments, clickstream...) flow through Kafka;
-- legacy historical orders arrive as file exports. Every table carries
-- `updated_at`, maintained by trigger, for watermark-based incremental extraction.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE OR REPLACE FUNCTION ops.touch_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS ops.countries (
  country_code      CHAR(2) PRIMARY KEY,
  country_name      TEXT NOT NULL,
  region            TEXT NOT NULL,
  currency_code     CHAR(3) NOT NULL,
  vat_rate          NUMERIC(5,4) NOT NULL CHECK (vat_rate >= 0 AND vat_rate < 1),
  timezone          TEXT NOT NULL,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.currencies (
  currency_code     CHAR(3) PRIMARY KEY,
  currency_name     TEXT NOT NULL,
  minor_units       SMALLINT NOT NULL DEFAULT 2,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.categories (
  category_id       TEXT PRIMARY KEY,
  category_name     TEXT NOT NULL,
  department        TEXT NOT NULL,
  parent_category   TEXT,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.suppliers (
  supplier_id       TEXT PRIMARY KEY,
  supplier_name     TEXT NOT NULL,
  country_code      CHAR(2) NOT NULL REFERENCES ops.countries(country_code),
  lead_time_days    SMALLINT NOT NULL CHECK (lead_time_days > 0),
  reliability_score NUMERIC(4,3) NOT NULL,
  is_active         BOOLEAN NOT NULL DEFAULT true,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.warehouses (
  warehouse_id      TEXT PRIMARY KEY,
  warehouse_name    TEXT NOT NULL,
  country_code      CHAR(2) NOT NULL REFERENCES ops.countries(country_code),
  city              TEXT NOT NULL,
  capacity_units    INTEGER NOT NULL CHECK (capacity_units > 0),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.customers (
  customer_id       TEXT PRIMARY KEY,
  first_name        TEXT NOT NULL,
  last_name         TEXT NOT NULL,
  email             TEXT NOT NULL UNIQUE,
  country_code      CHAR(2) NOT NULL REFERENCES ops.countries(country_code),
  registration_date TIMESTAMPTZ NOT NULL,
  customer_segment  TEXT NOT NULL CHECK (customer_segment IN ('consumer', 'small_business', 'corporate')),
  loyalty_status    TEXT NOT NULL CHECK (loyalty_status IN ('none', 'bronze', 'silver', 'gold', 'platinum')),
  marketing_opt_in  BOOLEAN NOT NULL DEFAULT false,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_customers_updated_at ON ops.customers (updated_at);

CREATE TABLE IF NOT EXISTS ops.products (
  product_id        TEXT PRIMARY KEY,
  product_name      TEXT NOT NULL,
  category_id       TEXT NOT NULL REFERENCES ops.categories(category_id),
  supplier_id       TEXT NOT NULL REFERENCES ops.suppliers(supplier_id),
  brand             TEXT NOT NULL,
  unit_price        NUMERIC(12,2) NOT NULL CHECK (unit_price > 0),
  unit_cost         NUMERIC(12,2) NOT NULL CHECK (unit_cost > 0),
  currency          CHAR(3) NOT NULL REFERENCES ops.currencies(currency_code),
  product_status    TEXT NOT NULL CHECK (product_status IN ('active', 'discontinued', 'out_of_stock', 'draft')),
  weight_kg         NUMERIC(8,3),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_products_updated_at ON ops.products (updated_at);

CREATE TABLE IF NOT EXISTS ops.promotions (
  promotion_id      TEXT PRIMARY KEY,
  promotion_name    TEXT NOT NULL,
  promotion_type    TEXT NOT NULL CHECK (promotion_type IN ('percentage', 'free_shipping', 'seasonal', 'clearance')),
  discount_pct      NUMERIC(5,4) NOT NULL CHECK (discount_pct >= 0 AND discount_pct < 1),
  start_date        DATE NOT NULL,
  end_date          DATE NOT NULL,
  channel           TEXT,
  department        TEXT,
  category_id       TEXT REFERENCES ops.categories(category_id),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (end_date >= start_date)
);

CREATE TABLE IF NOT EXISTS ops.inventory (
  warehouse_id       TEXT NOT NULL REFERENCES ops.warehouses(warehouse_id),
  product_id         TEXT NOT NULL REFERENCES ops.products(product_id),
  available_quantity INTEGER NOT NULL,
  reserved_quantity  INTEGER NOT NULL DEFAULT 0,
  reorder_threshold  INTEGER NOT NULL,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (warehouse_id, product_id)
);
CREATE INDEX IF NOT EXISTS ix_inventory_updated_at ON ops.inventory (updated_at);

CREATE TABLE IF NOT EXISTS ops.supplier_deliveries (
  delivery_id        TEXT PRIMARY KEY,
  supplier_id        TEXT NOT NULL REFERENCES ops.suppliers(supplier_id),
  warehouse_id       TEXT NOT NULL REFERENCES ops.warehouses(warehouse_id),
  product_id         TEXT NOT NULL REFERENCES ops.products(product_id),
  quantity           INTEGER NOT NULL CHECK (quantity > 0),
  ordered_at         TIMESTAMPTZ NOT NULL,
  expected_at        TIMESTAMPTZ NOT NULL,
  delivered_at       TIMESTAMPTZ,
  status             TEXT NOT NULL CHECK (status IN ('ordered', 'in_transit', 'delivered', 'cancelled')),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_supplier_deliveries_updated_at ON ops.supplier_deliveries (updated_at);

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['countries','currencies','categories','suppliers','warehouses','customers',
                           'products','promotions','inventory','supplier_deliveries']
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_touch_%1$s ON ops.%1$I', t);
    EXECUTE format('CREATE TRIGGER trg_touch_%1$s BEFORE UPDATE ON ops.%1$I
                    FOR EACH ROW EXECUTE FUNCTION ops.touch_updated_at()', t);
  END LOOP;
END $$;
