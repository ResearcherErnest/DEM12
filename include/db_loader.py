"""
include/db_loader.py
====================
Idempotent PostgreSQL bulk loader for the sales platform.
Loads all entity types: categories, products, customers, orders,
returned_orders, purchased_products.
No Airflow imports — fully unit-testable in isolation.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator, Tuple

import psycopg2
import psycopg2.extras
import pandas as pd

from include.config import settings

logger = logging.getLogger(__name__)


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Context-managed psycopg2 connection with auto-rollback on error."""
    conn = psycopg2.connect(settings.postgres_dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# === Product Categories ==========================================


def upsert_categories(
    df: pd.DataFrame,
    conn: psycopg2.extensions.connection,
) -> dict[str, int]:
    """
    Insert unique categories and return a mapping of name → category_id.
    """
    if df.empty:
        return {}

    records = [(row["name"], row["description"]) for _, row in df.iterrows()]

    sql = """
        INSERT INTO product_categories (name, description)
        VALUES %s
        ON CONFLICT (name) DO UPDATE SET
            description = EXCLUDED.description
        RETURNING category_id, name
        ;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, records, page_size=100)
        results = cur.fetchall()

    # Also fetch any existing rows not returned by the upsert
    with conn.cursor() as cur:
        cur.execute("SELECT category_id, name FROM product_categories;")
        all_categories = cur.fetchall()

    category_map = {name: cid for cid, name in all_categories}
    logger.info("Upserted %d product categories.", len(df))
    return category_map


# === Products ==========================================


def upsert_products(
    df: pd.DataFrame,
    conn: psycopg2.extensions.connection,
    category_map: dict[str, int],
) -> int:
    """
    Bulk-upsert product records. Requires category_map for FK resolution.
    Returns number of rows affected.
    """
    if df.empty:
        logger.warning("upsert_products called with empty DataFrame.")
        return 0

    # Map category name to category_id
    df = df.copy()
    df["category_id"] = df["category"].map(category_map)

    # Drop any products with unmapped categories
    unmapped = df[df["category_id"].isna()]
    if not unmapped.empty:
        logger.warning("Dropping %d products with unknown categories.", len(unmapped))
        df = df.dropna(subset=["category_id"])

    df["category_id"] = df["category_id"].astype(int)

    columns = ["product_id", "name", "category_id", "unit_price", "cost"]
    records = [tuple(row[col] for col in columns) for _, row in df.iterrows()]

    sql = f"""
        INSERT INTO products ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (product_id) DO UPDATE SET
            name        = EXCLUDED.name,
            category_id = EXCLUDED.category_id,
            unit_price  = EXCLUDED.unit_price,
            cost        = EXCLUDED.cost
        ;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, records, page_size=500)
        rows_affected = cur.rowcount

    logger.info("Upserted %d product rows.", rows_affected)
    return rows_affected


# === Customers ==========================================


def upsert_customers(
    df: pd.DataFrame,
    conn: psycopg2.extensions.connection,
) -> int:
    """
    Bulk-upsert customer records.
    Returns number of rows affected.
    """
    if df.empty:
        logger.warning("upsert_customers called with empty DataFrame.")
        return 0

    columns = ["customer_id", "name", "email", "region", "signup_date", "lifetime_value"]
    records = [tuple(row[col] for col in columns) for _, row in df.iterrows()]

    sql = f"""
        INSERT INTO customers ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (customer_id) DO UPDATE SET
            name           = EXCLUDED.name,
            email          = EXCLUDED.email,
            region         = EXCLUDED.region,
            signup_date    = EXCLUDED.signup_date
        ;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, records, page_size=500)
        rows_affected = cur.rowcount

    logger.info("Upserted %d customer rows.", rows_affected)
    return rows_affected


# === Orders (transactions) ==========================================


def upsert_orders(
    df: pd.DataFrame,
    conn: psycopg2.extensions.connection,
) -> Tuple[int, int]:
    """
    Bulk-upsert rows into the `orders` table.

    Returns:
        (rows_inserted, rows_skipped)
    """
    if df.empty:
        logger.warning("upsert_orders called with empty DataFrame — nothing to do.")
        return 0, 0

    columns = [
        "order_id", "customer_id", "product_id",
        "quantity", "unit_price", "discount",
        "order_date", "status", "region",
    ]

    records = [tuple(row[col] for col in columns) for _, row in df.iterrows()]

    sql = f"""
        INSERT INTO orders ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (order_id) DO UPDATE SET
            customer_id  = EXCLUDED.customer_id,
            product_id   = EXCLUDED.product_id,
            quantity     = EXCLUDED.quantity,
            unit_price   = EXCLUDED.unit_price,
            discount     = EXCLUDED.discount,
            order_date   = EXCLUDED.order_date,
            status       = EXCLUDED.status,
            region       = EXCLUDED.region,
            ingested_at  = NOW()
        ;
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, records, page_size=500)
        rows_affected = cur.rowcount

    rows_inserted = rows_affected
    rows_skipped = len(df) - rows_inserted if rows_inserted >= 0 else 0
    logger.info("Upserted %d order rows into PostgreSQL.", rows_inserted)
    return rows_inserted, rows_skipped


# === Returned Orders ==========================================


def insert_returned_orders(
    df: pd.DataFrame,
    conn: psycopg2.extensions.connection,
) -> int:
    """
    Insert returned order records. Skips duplicates by checking existing order_ids.
    Returns number of rows inserted.
    """
    if df.empty:
        logger.info("No returned orders to insert.")
        return 0

    # Avoid duplicates: only insert for order_ids not already in returned_orders
    order_ids = tuple(df["order_id"].tolist())
    with conn.cursor() as cur:
        cur.execute(
            "SELECT order_id FROM returned_orders WHERE order_id = ANY(%s);",
            (list(order_ids),),
        )
        existing = {row[0] for row in cur.fetchall()}

    df = df[~df["order_id"].isin(existing)]
    if df.empty:
        logger.info("All returned orders already exist — skipping.")
        return 0

    columns = ["order_id", "return_reason", "return_date", "refund_amount"]
    records = [tuple(row[col] for col in columns) for _, row in df.iterrows()]

    sql = f"""
        INSERT INTO returned_orders ({', '.join(columns)})
        VALUES %s
        ;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, records, page_size=500)
        rows_affected = cur.rowcount

    logger.info("Inserted %d returned order records.", rows_affected)
    return rows_affected


# === Purchased Products (aggregation) ==========================================


def upsert_purchased_products(
    agg_df: pd.DataFrame,
    conn: psycopg2.extensions.connection,
) -> None:
    """Upsert the purchased_products aggregation table."""
    if agg_df.empty:
        return

    columns = [
        "product_id", "product_name", "category_name",
        "total_units_sold", "total_revenue", "avg_discount",
        "last_purchased_date",
    ]
    records = [tuple(row[col] for col in columns) for _, row in agg_df.iterrows()]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"""
                INSERT INTO purchased_products ({', '.join(columns)}, updated_at)
                VALUES %s
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name        = EXCLUDED.product_name,
                    category_name       = EXCLUDED.category_name,
                    total_units_sold    = purchased_products.total_units_sold + EXCLUDED.total_units_sold,
                    total_revenue       = purchased_products.total_revenue    + EXCLUDED.total_revenue,
                    avg_discount        = EXCLUDED.avg_discount,
                    last_purchased_date = GREATEST(purchased_products.last_purchased_date, EXCLUDED.last_purchased_date),
                    updated_at          = NOW()
            """,
            records,
            template="(%s, %s, %s, %s, %s, %s, %s, NOW())",
            page_size=500,
        )
    logger.info("Upserted %d product aggregation rows.", len(agg_df))


# === Customer Lifetime Value ==========================================


def update_customer_lifetime_values(conn: psycopg2.extensions.connection) -> None:
    """Update lifetime_value for all customers based on their completed orders."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE customers c
            SET lifetime_value = COALESCE(sub.total, 0)
            FROM (
                SELECT customer_id, SUM(total_revenue) AS total
                FROM orders
                WHERE status = 'completed'
                GROUP BY customer_id
            ) sub
            WHERE c.customer_id = sub.customer_id;
        """)
        rows = cur.rowcount
    logger.info("Updated lifetime_value for %d customers.", rows)


# === Pipeline Audit Log ==========================================


def log_pipeline_run(
    conn: psycopg2.extensions.connection,
    dag_run_id: str,
    file_processed: str,
    rows_inserted: int,
    rows_skipped: int,
    status: str,
) -> int:
    """Insert an audit row into pipeline_runs; return the new run_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs
                (dag_run_id, file_processed, rows_inserted, rows_skipped, status, finished_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            RETURNING run_id
            """,
            (dag_run_id, file_processed, rows_inserted, rows_skipped, status),
        )
        run_id = cur.fetchone()[0]
    logger.info("Logged pipeline_run #%d — status=%s", run_id, status)
    return run_id


# === Data Quality Log ==========================================


def log_data_quality_issue(
    conn: psycopg2.extensions.connection,
    dag_run_id: str,
    file_name: str,
    issue_type: str,
    error_message: str,
) -> int:
    """Insert an error record into data_quality_log."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_quality_log
                (dag_run_id, file_name, issue_type, error_message)
            VALUES (%s, %s, %s, %s)
            RETURNING log_id
            """,
            (dag_run_id, file_name, issue_type, error_message),
        )
        log_id = cur.fetchone()[0]
    logger.warning("Logged Data Quality Issue #%d for %s: %s", log_id, file_name, issue_type)
    return log_id

