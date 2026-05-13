"""
spark/jobs/bronze_ingestion.py
──────────────────────────────
Incremental MongoDB → HDFS Bronze layer loader.

Usage:
  spark-submit \\
    --master yarn \\
    --py-files utils.zip \\
    bronze_ingestion.py \\
    --collection customers \\
    --run-id <uuid>
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

# ── Path setup ────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parents[1]))

from utils.common import (
    fetch_active_collections,
    get_collection_metadata,
    get_logger,
    insert_audit_record,
    load_config,
    update_watermark,
    utcnow,
    generate_run_id,
)
from utils.spark_session import get_spark_session, stop_spark

log = get_logger("bronze_ingestion")


# ──────────────────────────────────────────────────────────────
# MongoDB Extraction
# ──────────────────────────────────────────────────────────────

def extract_from_mongodb(
    spark: SparkSession,
    database: str,
    collection: str,
    watermark_col: str,
    last_loaded_ts: datetime,
    upper_bound_ts: datetime,
) -> DataFrame:
    """
    Read records from MongoDB where watermark_col is between
    last_loaded_ts (exclusive) and upper_bound_ts (inclusive).
    """
    pipeline = [
        {
            "$match": {
                watermark_col: {
                    "$gt":  {"$date": last_loaded_ts.isoformat() + "Z"},
                    "$lte": {"$date": upper_bound_ts.isoformat() + "Z"},
                }
            }
        }
    ]

    log.info(
        "Extracting [%s] | watermark_col=%s | window=[%s → %s]",
        collection, watermark_col, last_loaded_ts, upper_bound_ts,
    )

    df = (
        spark.read.format("mongodb")
        .option("database",   database)
        .option("collection", collection)
        .option("aggregation.pipeline", str(pipeline))
        .load()
    )

    log.info("Extracted %d records from [%s]", df.count(), collection)
    return df


# ──────────────────────────────────────────────────────────────
# Schema Flattening / Enrichment
# ──────────────────────────────────────────────────────────────

def enrich_bronze(df: DataFrame, collection: str, load_date: str) -> DataFrame:
    """
    Add audit columns and serialise the raw document as JSON string.
    Nested structs are preserved; only metadata columns are added.
    """
    # Convert _id (ObjectId struct) to string
    if "_id" in df.columns:
        df = df.withColumn("_id", F.col("_id").cast(StringType()))

    # Ensure timestamp columns are cast properly
    for col_name in df.columns:
        if col_name in ("created_at", "updated_at"):
            df = df.withColumn(col_name, F.col(col_name).cast(TimestampType()))

    # Serialise entire row to JSON for audit/lineage
    df = df.withColumn("raw_document", F.to_json(F.struct(*df.columns)))

    # Add pipeline metadata columns
    df = (
        df
        .withColumn("load_date",        F.lit(load_date))
        .withColumn("pipeline_load_ts", F.current_timestamp())
        .withColumn("source_collection", F.lit(collection))
    )

    return df


# ──────────────────────────────────────────────────────────────
# HDFS Write
# ──────────────────────────────────────────────────────────────

def write_bronze(df: DataFrame, bronze_path: str, load_date: str) -> str:
    """Write DataFrame to HDFS bronze path partitioned by load_date."""
    output_path = f"{bronze_path}/load_date={load_date}"

    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(output_path)
    )

    log.info("Written bronze data → %s", output_path)
    return output_path


# ──────────────────────────────────────────────────────────────
# Main Ingestion Routine
# ──────────────────────────────────────────────────────────────

def ingest_collection(
    spark: SparkSession,
    collection_name: str,
    run_id: str,
    dag_id: str = "manual",
    task_id: str = "bronze_ingestion",
    upper_bound_ts: Optional[datetime] = None,
) -> dict:
    """
    Full bronze ingestion for one collection.
    Returns a summary dict for upstream DAG tasks.
    """
    start_time = utcnow()
    load_date  = start_time.strftime("%Y-%m-%d")
    status     = "FAILED"
    records_extracted = records_loaded = records_failed = 0
    output_path = None
    error_msg   = None

    try:
        # ── Fetch metadata ────────────────────────────────────
        meta = get_collection_metadata(collection_name)
        if not meta:
            raise ValueError(f"No active metadata for collection: {collection_name}")

        watermark_col    = meta["watermark_column"]
        last_loaded_ts   = meta["last_loaded_timestamp"]
        bronze_path      = meta["bronze_path"]
        upper_bound_ts   = upper_bound_ts or utcnow()

        if isinstance(last_loaded_ts, str):
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts)

        cfg = load_config()

        # ── Extract ───────────────────────────────────────────
        raw_df = extract_from_mongodb(
            spark,
            database       = cfg["mongodb"]["database"],
            collection     = collection_name,
            watermark_col  = watermark_col,
            last_loaded_ts = last_loaded_ts,
            upper_bound_ts = upper_bound_ts,
        )

        records_extracted = raw_df.count()

        if records_extracted == 0:
            log.info("[%s] No new records. Skipping write.", collection_name)
            status = "SUCCESS"
            return {
                "collection": collection_name,
                "records_extracted": 0,
                "records_loaded": 0,
                "status": "SUCCESS",
                "output_path": None,
            }

        # ── Enrich ────────────────────────────────────────────
        enriched_df = enrich_bronze(raw_df, collection_name, load_date)

        # ── Write ─────────────────────────────────────────────
        output_path     = write_bronze(enriched_df, bronze_path, load_date)
        records_loaded  = records_extracted
        status          = "SUCCESS"

        # ── Advance watermark ─────────────────────────────────
        update_watermark(collection_name, upper_bound_ts)
        log.info("[%s] Watermark advanced to %s", collection_name, upper_bound_ts)

    except Exception as exc:
        error_msg      = str(exc)
        records_failed = records_extracted
        status         = "FAILED"
        log.exception("[%s] Bronze ingestion failed: %s", collection_name, exc)

    finally:
        end_time = utcnow()
        insert_audit_record(
            run_id            = run_id,
            dag_id            = dag_id,
            task_id           = task_id,
            collection_name   = collection_name,
            layer             = "bronze",
            start_time        = start_time,
            end_time          = end_time,
            records_extracted = records_extracted,
            records_loaded    = records_loaded,
            records_failed    = records_failed,
            watermark_start   = meta.get("last_loaded_timestamp") if "meta" in dir() else None,
            watermark_end     = upper_bound_ts,
            status            = status,
            error_message     = error_msg,
            hdfs_path         = output_path,
        )

    return {
        "collection":         collection_name,
        "records_extracted":  records_extracted,
        "records_loaded":     records_loaded,
        "records_failed":     records_failed,
        "status":             status,
        "output_path":        output_path,
        "error":              error_msg,
    }


# ──────────────────────────────────────────────────────────────
# CLI Entry Point
# ──────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bronze layer ingestion job")
    p.add_argument("--collection", required=True, help="MongoDB collection name")
    p.add_argument("--run-id",     default=generate_run_id(), help="Pipeline run UUID")
    p.add_argument("--dag-id",     default="manual")
    p.add_argument("--task-id",    default="bronze_ingestion")
    p.add_argument("--upper-bound", help="Upper watermark (ISO format)", default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    upper_bound = (
        datetime.fromisoformat(args.upper_bound)
        if args.upper_bound else None
    )

    spark = get_spark_session(app_name=f"Bronze_{args.collection}")
    try:
        result = ingest_collection(
            spark           = spark,
            collection_name = args.collection,
            run_id          = args.run_id,
            dag_id          = args.dag_id,
            task_id         = args.task_id,
            upper_bound_ts  = upper_bound,
        )
        log.info("Result: %s", result)
        if result["status"] == "FAILED":
            sys.exit(1)
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
