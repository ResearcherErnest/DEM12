/* ================================================================
-- Sales Data Platform — PostgreSQL Schema
-- ================================================================
-- This script runs automatically when the postgres container starts
-- for the first time (mounted in /docker-entrypoint-initdb.d/).
-- ================================================================

-- == Switch to sales database ==================================*/
\connect sales

-- ================================================================
-- 1. PRODUCT_CATEGORIES — dimension table for product categories
-- ================================================================
CREATE TABLE IF NOT EXISTS product_categories (
    category_id     SERIAL          PRIMARY KEY,
    name            VARCHAR(60)     NOT NULL UNIQUE,
    description     TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_product_categories_name ON product_categories (name);

-- 2. PRODUCTS — product dimension table
-- ================================================================
CREATE TABLE IF NOT EXISTS products (
    product_id      VARCHAR(36)     PRIMARY KEY,
    name            VARCHAR(120)    NOT NULL,
    category_id     INTEGER         NOT NULL REFERENCES product_categories(category_id),
    unit_price      NUMERIC(10, 2)  NOT NULL CHECK (unit_price >= 0),
    cost            NUMERIC(10, 2)  NOT NULL CHECK (cost >= 0),
    margin          NUMERIC(10, 2)  GENERATED ALWAYS AS (unit_price - cost) STORED,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_products_category_id ON products (category_id);
CREATE INDEX IF NOT EXISTS idx_products_name        ON products (name);

-- 3. CUSTOMERS — customer dimension table
-- ================================================================
CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(36)     PRIMARY KEY,
    name            VARCHAR(120)    NOT NULL,
    email           VARCHAR(255)    NOT NULL,
    region          VARCHAR(60)     NOT NULL,
    signup_date     DATE            NOT NULL,
    lifetime_value  NUMERIC(14, 2)  NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_customers_region      ON customers (region);
CREATE INDEX IF NOT EXISTS idx_customers_signup_date ON customers (signup_date);

-- 4. ORDERS — core sales fact table
-- ================================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id        VARCHAR(36)     PRIMARY KEY,
    customer_id     VARCHAR(36)     NOT NULL REFERENCES customers(customer_id),
    product_id      VARCHAR(36)     NOT NULL REFERENCES products(product_id),
    quantity        INTEGER         NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(10, 2)  NOT NULL CHECK (unit_price >= 0),
    discount        NUMERIC(5, 4)   NOT NULL DEFAULT 0 CHECK (discount BETWEEN 0 AND 1),
    total_revenue   NUMERIC(12, 2)  GENERATED ALWAYS AS
                        (quantity * unit_price * (1 - discount)) STORED,
    order_date      DATE            NOT NULL,
    status          VARCHAR(30)     NOT NULL DEFAULT 'completed',
    region          VARCHAR(60)     NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_order_date   ON orders (order_date);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id  ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_product_id   ON orders (product_id);
CREATE INDEX IF NOT EXISTS idx_orders_region       ON orders (region);
CREATE INDEX IF NOT EXISTS idx_orders_status       ON orders (status);

-- ================================================================
-- 5. RETURNED_ORDERS — tracks order returns / refunds
-- ================================================================
CREATE TABLE IF NOT EXISTS returned_orders (
    return_id       SERIAL          PRIMARY KEY,
    order_id        VARCHAR(36)     NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    return_reason   VARCHAR(255),
    return_date     DATE            NOT NULL,
    refund_amount   NUMERIC(12, 2)  NOT NULL CHECK (refund_amount >= 0),
    processed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_returns_order_id    ON returned_orders (order_id);
CREATE INDEX IF NOT EXISTS idx_returns_return_date ON returned_orders (return_date);

-- 6. PURCHASED_PRODUCTS — product-level aggregation table
--    Refreshed by the Airflow pipeline after each load.
-- ================================================================
CREATE TABLE IF NOT EXISTS purchased_products (
    product_id          VARCHAR(36)     PRIMARY KEY REFERENCES products(product_id),
    product_name        VARCHAR(120)    NOT NULL,
    category_name       VARCHAR(60)     NOT NULL,
    total_units_sold    BIGINT          NOT NULL DEFAULT 0,
    total_revenue       NUMERIC(14, 2)  NOT NULL DEFAULT 0,
    avg_discount        NUMERIC(5, 4)   NOT NULL DEFAULT 0,
    last_purchased_date DATE,
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- 7. PIPELINE_RUNS — ETL audit log
-- ================================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          SERIAL          PRIMARY KEY,
    dag_run_id      VARCHAR(255)    NOT NULL,
    file_processed  VARCHAR(500)    NOT NULL,
    rows_inserted   INTEGER         NOT NULL DEFAULT 0,
    rows_skipped    INTEGER         NOT NULL DEFAULT 0,
    status          VARCHAR(30)     NOT NULL DEFAULT 'running',
    started_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_run_id ON pipeline_runs (dag_run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status     ON pipeline_runs (status);

-- 8. DATA_QUALITY_LOG — Tracks invalid files and rows
-- ================================================================
CREATE TABLE IF NOT EXISTS data_quality_log (
    log_id          SERIAL          PRIMARY KEY,
    dag_run_id      VARCHAR(255)    NOT NULL,
    file_name       VARCHAR(500)    NOT NULL,
    issue_type      VARCHAR(100)    NOT NULL,
    error_message   TEXT,
    logged_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_log_dag_run_id ON data_quality_log (dag_run_id);

-- GRANTS — sales_user gets full access, metabase_user gets read-only
-- ================================================================
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO sales_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sales_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON TABLES    TO sales_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON SEQUENCES TO sales_user;

-- Metabase read-only access
GRANT CONNECT ON DATABASE sales TO metabase_user;
GRANT USAGE ON SCHEMA public TO metabase_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO metabase_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO metabase_user;
