"""
dags/sales_pipeline_dag.py
==========================
Airflow DAG: MinIO (Streaming) → Validate → Batch Transform → PostgreSQL → Archive

Processes three CSV file types per run:
  1. customers_*.csv  — customer dimension
  2. products_*.csv   — product dimension (+ auto-extracted categories)
  3. sales_*.csv      — transaction fact table (+ returns extraction)

Schedule: every 15 minutes
Retry:    3 attempts with 2-minute back-off
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from minio import Minio
from minio.commonconfig import CopySource

# Airflow runs inside the container; include/ is on PYTHONPATH via volume mount
from include.config import settings
from include.db_loader import (
    get_connection,
    insert_returned_orders,
    log_pipeline_run,
    log_data_quality_issue,
    update_customer_lifetime_values,
    upsert_categories,
    upsert_customers,
    upsert_orders,
    upsert_products,
    upsert_purchased_products,
)
from include.transformations import (
    build_product_aggregations,
    clean_and_transform,
    clean_customers,
    clean_products,
    extract_categories,
    extract_returns,
)

logger = logging.getLogger(__name__)

# === DAG default args =============================================
DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

# === Helpers =============================================================


def _get_minio_client():
    endpoint = settings.minio_endpoint.replace("http://", "").replace("https://", "")
    return Minio(
        endpoint,
        access_key=settings.minio_root_user,
        secret_key=settings.minio_root_password,
        secure=settings.minio_endpoint.startswith("https")
    )


def _on_failure_callback(context: dict) -> None:
    """Log failed pipeline runs to the audit table."""
    try:
        dag_run_id = context.get("run_id", "unknown")
        with get_connection() as conn:
            log_pipeline_run(
                conn=conn,
                dag_run_id=dag_run_id,
                file_processed="N/A",
                rows_inserted=0,
                rows_skipped=0,
                status="failed",
            )
    except Exception as exc:
        logger.error("Failed to log pipeline failure: %s", exc)


# === Task callables =============================================


def run_data_generator(**context) -> None:
    """Generate and upload data using the existing script."""
    import sys

    gen_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data-generator")
    if gen_path not in sys.path:
        sys.path.insert(0, gen_path)

    try:
        from generate_data import (
            generate_customers,
            generate_products,
            generate_transactions,
            upload_csv_to_minio,
        )
        from config import settings as gen_settings

        logger.info("DAG generating data (seed=%d)", gen_settings.generator_seed)

        customers_df = generate_customers(gen_settings.generator_num_customers)
        products_df = generate_products()
        transactions_df = generate_transactions(
            customers_df["customer_id"].tolist(),
            products_df,
            gen_settings.generator_num_transactions,
        )

        keys = []
        keys.append(upload_csv_to_minio(customers_df, "customers"))
        keys.append(upload_csv_to_minio(products_df, "products"))
        keys.append(upload_csv_to_minio(transactions_df, "sales"))
        logger.info("DAG generated %d MinIO objects: %s", len(keys), keys)
    finally:
        if gen_path in sys.path:
            sys.path.remove(gen_path)


def discover_files(**context) -> None:
    """Discover pending CSVs without downloading natively to disk."""
    client = _get_minio_client()
    objects = client.list_objects(settings.minio_raw_bucket, recursive=True)
    all_files = [obj.object_name for obj in objects]

    if not all_files:
        raise ValueError("No files found in MinIO raw-data bucket.")

    classified: dict[str, list[str]] = {"customers": [], "products": [], "sales": []}
    for key in all_files:
        basename = key.lower()
        if basename.startswith("customers"):
            classified["customers"].append(key)
        elif basename.startswith("products"):
            classified["products"].append(key)
        elif basename.startswith("sales"):
            classified["sales"].append(key)

    logger.info("Classified files: %s", classified)

    ti = context["ti"]
    ti.xcom_push(key="classified_files", value=classified)
    ti.xcom_push(key="all_object_keys", value=all_files)


def validate_csv(**context) -> None:
    """Validate each pending CSV using a HeadSensor (reading first chunk only)."""
    classified = context["ti"].xcom_pull(key="classified_files")
    client = _get_minio_client()
    valid_classified: dict[str, list[str]] = {"customers": [], "products": [], "sales": []}
    
    dag_run_id = context["run_id"]

    for file_type, keys in classified.items():
        for key in keys:
            response = None
            try:
                # HeadSensor: Read a small chunk to check columns natively from stream
                response = client.get_object(settings.minio_raw_bucket, key, offset=0, length=10000)
                data = response.read().decode('utf-8').split('\n')
                if not data or len(data) < 2:
                    raise ValueError(f"{file_type.capitalize()} CSV is empty or too small.")
                
                header = [c.strip('\r"') for c in data[0].split(',')]
                
                if file_type == "customers":
                    required = {"customer_id", "name", "email", "region", "signup_date"}
                elif file_type == "products":
                    required = {"product_id", "name", "category", "unit_price", "cost"}
                else:
                    required = {
                        "order_id", "customer_id", "product_id",
                        "quantity", "unit_price", "discount",
                        "order_date", "status", "region"
                    }

                missing = required - set(header)
                if missing:
                    raise ValueError(f"Missing columns: {missing}")
                
                valid_classified[file_type].append(key)
                logger.info("Validation passed for %s", key)
                
            except Exception as e:
                logger.error("Validation failed for %s: %s", key, e)
                # Move to invalid bucket
                try:
                    client.copy_object(
                        settings.minio_invalid_bucket, key,
                        CopySource(settings.minio_raw_bucket, key)
                    )
                    client.remove_object(settings.minio_raw_bucket, key)
                except Exception as ex:
                    logger.error("Failed to move invalid file to quarantine: %s", ex)
                
                # Log to DB
                with get_connection() as conn:
                    log_data_quality_issue(
                        conn=conn,
                        dag_run_id=dag_run_id,
                        file_name=key,
                        issue_type="validation_error",
                        error_message=str(e),
                    )
            finally:
                if response:
                    response.close()
                    response.release_conn()

    context["ti"].xcom_push(key="valid_classified_files", value=valid_classified)


def transform_data(**context) -> None:
    """Stream clean & transform CSVs in 20k row batches; write chunked parquet paths."""
    valid_classified = context["ti"].xcom_pull(key="valid_classified_files")
    client = _get_minio_client()
    
    cleaned_paths: dict[str, list[str]] = {"customers": [], "products": [], "sales": []}
    total_skipped = 0
    
    for file_type, keys in valid_classified.items():
        for key in keys:
            response = None
            try:
                logger.info("Streaming dataset for transformation: %s", key)
                response = client.get_object(settings.minio_raw_bucket, key)
                # Intelligent batching natively
                chunk_iter = pd.read_csv(response, chunksize=20000)
                
                for i, chunk in enumerate(chunk_iter):
                    if file_type == "customers":
                        df_clean, skipped = clean_customers(chunk)
                    elif file_type == "products":
                        df_clean, skipped = clean_products(chunk)
                    else:
                        df_clean, skipped = clean_and_transform(chunk)
                        
                    total_skipped += skipped
                    
                    if not df_clean.empty:
                        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False, prefix=f"{file_type}_clean_{i}_")
                        tmp.close()
                        df_clean.to_parquet(tmp.name, index=False)
                        cleaned_paths[file_type].append(tmp.name)
                        
            finally:
                if response:
                    response.close()
                    response.release_conn()
                    
    context["ti"].xcom_push(key="cleaned_paths", value=cleaned_paths)
    context["ti"].xcom_push(key="rows_skipped", value=total_skipped)
    logger.info("Transformation complete. Cleaned chunks: %s", cleaned_paths)


def load_to_postgres(**context) -> None:
    """
    Bulk-upsert all entities into PostgreSQL in FK-safe order from parquet chunks.
    """
    cleaned_paths = context["ti"].xcom_pull(key="cleaned_paths")
    rows_skipped = context["ti"].xcom_pull(key="rows_skipped") or 0
    dag_run_id = context["run_id"]

    total_rows_inserted = 0

    with get_connection() as conn:
        # 1. Load products + extract categories
        products_df = []
        for path in cleaned_paths.get("products", []):
            products_df.append(pd.read_parquet(path))
            
        full_products_df = None
        if products_df:
            full_products_df = pd.concat(products_df).drop_duplicates(subset=["product_id"], keep="last")
            cat_df = extract_categories(full_products_df)
            category_map = upsert_categories(cat_df, conn)
            upsert_products(full_products_df, conn, category_map)
            logger.info("Loaded %d products.", len(full_products_df))

        # 2. Load customers
        for path in cleaned_paths.get("customers", []):
            df = pd.read_parquet(path)
            upsert_customers(df, conn)

        # 3. Load orders (transactions)
        all_orders = []
        for path in cleaned_paths.get("sales", []):
            df = pd.read_parquet(path)
            rows_inserted, _ = upsert_orders(df, conn)
            total_rows_inserted += rows_inserted
            all_orders.append(df)
            
        if all_orders:
            orders_df = pd.concat(all_orders)

            # 4. Extract and load returned orders
            returns_df = extract_returns(orders_df)
            if not returns_df.empty:
                insert_returned_orders(returns_df, conn)

            # 5. Build and upsert purchased_products aggregation
            if full_products_df is not None:
                agg_df = build_product_aggregations(orders_df, full_products_df)
                upsert_purchased_products(agg_df, conn)

            # 6. Update customer lifetime values
            update_customer_lifetime_values(conn)

        # 7. Log pipeline run
        valid_classified = context["ti"].xcom_pull(key="valid_classified_files")
        valid_keys = []
        for v in valid_classified.values():
            valid_keys.extend(v)
            
        log_pipeline_run(
            conn=conn,
            dag_run_id=dag_run_id,
            file_processed=", ".join(valid_keys) if valid_keys else "N/A",
            rows_inserted=total_rows_inserted,
            rows_skipped=rows_skipped,
            status="success",
        )

    logger.info(
        "Loaded %d order rows into PostgreSQL (skipped=%d).",
        total_rows_inserted, rows_skipped,
    )


def archive_file(**context) -> None:
    """Move all processed files from raw-data → processed-data bucket."""
    valid_classified = context["ti"].xcom_pull(key="valid_classified_files")
    client = _get_minio_client()

    archived_count = 0
    for keys in valid_classified.values():
        for object_key in keys:
            client.copy_object(
                settings.minio_processed_bucket, object_key,
                CopySource(settings.minio_raw_bucket, object_key)
            )
            client.remove_object(settings.minio_raw_bucket, object_key)
            logger.info("Archived '%s' → processed-data bucket.", object_key)
            archived_count += 1

    # Cleanup temp parquet chunks
    cleaned_paths = context["ti"].xcom_pull(key="cleaned_paths") or {}
    for paths in cleaned_paths.values():
        for path in paths:
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                pass

    logger.info("Archived %d files to processed-data bucket.", archived_count)


# === DAG Definition =============================================
with DAG(
    dag_id="sales_pipeline_dag",
    description="E-Commerce ETL: MinIO → Validation → Batching Transform → PostgreSQL",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["sales", "etl", "minio", "postgres"],
    on_failure_callback=_on_failure_callback,
) as dag:

    t_generate = PythonOperator(
        task_id="generate_data",
        python_callable=run_data_generator,
    )

    t_discover = PythonOperator(
        task_id="discover_files",
        python_callable=discover_files,
    )

    t_validate = PythonOperator(
        task_id="validate_csv",
        python_callable=validate_csv,
    )

    t_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    t_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    t_archive = PythonOperator(
        task_id="archive_file",
        python_callable=archive_file,
    )

    # === Task graph =============================================
    t_generate >> t_discover >> t_validate >> t_transform >> t_load >> t_archive
