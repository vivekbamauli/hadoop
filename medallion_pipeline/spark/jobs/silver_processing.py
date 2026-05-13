"""
spark/jobs/silver_processing.py
────────────────────────────────
Bronze → Silver transformation with:
  • Schema validation & cleansing
  • Deduplication
  • SCD Type 1 (upsert)
  • SCD Type 2 (history tracking)
  • Null handling
  • Record hashing for change detection

Usage:
  spark-submit \\
    --master yarn \\
    --py-files utils.zip \\
    silver_processing.py \\
    --collection customers \\
    --run-id <uuid>
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    BooleanType, IntegerType, StringType, TimestampType,
)

sys.path.insert(0, str(Path(__file__).parents[1]))

from utils.common import (
    generate_run_id,
    get_collection_metadata,
    get_logger,
    insert_audit_record,
    load_config,
    utcnow,
)
from utils.spark_session import get_spark_session, stop_spark

log = get_logger("silver_processing")

# ──────────────────────────────────────────────────────────────
# Collection-specific cleansing rules
# ──────────────────────────────────────────────────────────────

COLLECTION_RULES: Dict[str, Dict] = {
    "customers": {
        "required_cols": ["customer_id", "email"],
        "string_cols":   ["first_name", "last_name", "email", "phone",
                          "address", "city", "country", "status"],
        "ts_cols":       ["created_at", "updated_at"],
        "derived": {
            "full_name": "CONCAT(COALESCE(first_name,''), ' ', COALESCE(last_name,''))",
        },
    },
    "orders": {
        "required_cols": ["order_id", "customer_id"],
        "string_cols":   ["order_id", "customer_id", "currency", "status"],
        "ts_cols":       ["created_at", "updated_at"],
        "numeric_cols":  ["total_amount"],
        "derived": {},
    },
    "products": {
        "required_cols": ["product_id", "name"],
        "string_cols":   ["product_id", "name", "category", "sub_category", "status"],
        "ts_cols":       ["created_at", "updated_at"],
        "numeric_cols":  ["price", "stock_qty"],
        "derived": {},
    },
    "payments": {
        "required_cols": ["payment_id", "order_id"],
        "string_cols":   ["payment_id", "order_id", "customer_id",
                          "currency", "method", "gateway", "status"],
        "ts_cols":       ["created_at"],
        "numeric_cols":  ["amount"],
        "derived": {},
    },
    "reviews": {
        "required_cols": ["review_id", "product_id", "customer_id"],
        "string_cols":   ["review_id", "product_id", "customer_id",
                          "title", "body", "status"],
        "ts_cols":       ["created_at"],
        "numeric_cols":  ["rating"],
        "derived": {},
    },
}

# ──────────────────────────────────────────────────────────────
# Read Bronze
# ──────────────────────────────────────────────────────────────

def read_bronze(
    spark: SparkSession,
    bronze_path: str,
    load_date: str,
) -> DataFrame:
    path = f"{bronze_path}/load_date={load_date}"
    log.info("Reading bronze: %s", path)
    return spark.read.parquet(path)


# ──────────────────────────────────────────────────────────────
# Cleansing
# ──────────────────────────────────────────────────────────────

def cleanse(df: DataFrame, collection: str) -> DataFrame:
    rules = COLLECTION_RULES.get(collection, {})

    # Drop nulls on required columns
    required = rules.get("required_cols", [])
    existing_required = [c for c in required if c in df.columns]
    if existing_required:
        df = df.dropna(subset=existing_required)

    # Trim & lower string columns
    for col in rules.get("string_cols", []):
        if col in df.columns:
            df = df.withColumn(col, F.trim(F.col(col)))
            if col in ("email", "status"):
                df = df.withColumn(col, F.lower(F.col(col)))

    # Cast timestamp columns
    for col in rules.get("ts_cols", []):
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(TimestampType()))

    # Cast numeric columns
    for col in rules.get("numeric_cols", []):
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast("double"))

    # Add derived columns
    for derived_col, expr_str in rules.get("derived", {}).items():
        df = df.withColumn(derived_col, F.expr(expr_str))

    # Drop internal bronze audit cols not needed in silver
    drop_cols = ["raw_document", "source_collection"]
    df = df.drop(*[c for c in drop_cols if c in df.columns])

    return df


# ──────────────────────────────────────────────────────────────
# Deduplication
# ──────────────────────────────────────────────────────────────

def deduplicate(df: DataFrame, pk_col: str, watermark_col: str) -> DataFrame:
    """Keep the latest record per primary key within the incoming batch."""
    w = Window.partitionBy(pk_col).orderBy(F.col(watermark_col).desc())
    return (
        df.withColumn("_rank", F.row_number().over(w))
          .filter(F.col("_rank") == 1)
          .drop("_rank")
    )


# ──────────────────────────────────────────────────────────────
# Record Hash
# ──────────────────────────────────────────────────────────────

def add_record_hash(df: DataFrame, exclude: Optional[List[str]] = None) -> DataFrame:
    exclude_set = set(exclude or [
        "load_date", "pipeline_load_ts", "record_hash",
        "effective_date", "expiry_date", "is_current", "version_number",
    ])
    hash_cols = sorted([c for c in df.columns if c not in exclude_set])
    hash_expr = F.md5(F.concat_ws("|", *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in hash_cols]))
    return df.withColumn("record_hash", hash_expr)


# ──────────────────────────────────────────────────────────────
# SCD Type 1  (simple upsert – overwrite changed records)
# ──────────────────────────────────────────────────────────────

def apply_scd1(
    spark: SparkSession,
    incoming_df: DataFrame,
    silver_path: str,
    pk_col: str,
    load_date: str,
) -> DataFrame:
    """
    Merge incoming records into the silver layer using SCD Type 1.
    Changed records overwrite existing; new records are appended.
    """
    output_path = silver_path

    try:
        existing_df = spark.read.parquet(output_path)
        log.info("SCD1: existing silver rows = %d", existing_df.count())
    except Exception:
        log.info("SCD1: No existing silver data; full load.")
        existing_df = None

    incoming_enriched = (
        incoming_df
        .withColumn("pipeline_load_ts", F.current_timestamp())
        .withColumn("load_date",         F.lit(load_date))
    )

    if existing_df is None:
        merged = incoming_enriched
    else:
        # Remove records that appear in incoming (will be replaced)
        updated_keys = incoming_df.select(pk_col)
        existing_unchanged = existing_df.join(updated_keys, on=pk_col, how="left_anti")
        merged = existing_unchanged.unionByName(incoming_enriched, allowMissingColumns=True)

    merged.write.mode("overwrite").partitionBy("load_date").option("compression","snappy").parquet(output_path)
    log.info("SCD1: Wrote %d rows → %s", merged.count(), output_path)
    return merged


# ──────────────────────────────────────────────────────────────
# SCD Type 2  (full history with effective/expiry dates)
# ──────────────────────────────────────────────────────────────

_SCD2_EXPIRY = datetime(9999, 12, 31, 23, 59, 59)
_SCD2_EXPIRY_LIT = F.to_timestamp(F.lit("9999-12-31 23:59:59"))


def apply_scd2(
    spark: SparkSession,
    incoming_df: DataFrame,
    silver_path: str,
    pk_col: str,
    load_date: str,
) -> DataFrame:
    """
    SCD Type 2 merge:
      1. Existing current rows whose hash changed → set is_current=False, expiry_date=now
      2. All incoming rows → insert as new current rows (version+1)
      3. Unchanged current rows → pass through untouched
    """
    now_ts = utcnow()

    try:
        existing_df = (
            spark.read.parquet(silver_path)
            .filter(F.col("is_current") == True)
        )
        log.info("SCD2: existing current rows = %d", existing_df.count())
        has_existing = True
    except Exception:
        log.info("SCD2: No existing silver data; full load.")
        existing_df = None
        has_existing = False

    # Prepare incoming
    incoming_with_hash = add_record_hash(incoming_df)

    if not has_existing:
        # First load – all records are new current rows (version=1)
        result = (
            incoming_with_hash
            .withColumn("effective_date",  F.lit(now_ts).cast(TimestampType()))
            .withColumn("expiry_date",     _SCD2_EXPIRY_LIT)
            .withColumn("is_current",      F.lit(True).cast(BooleanType()))
            .withColumn("version_number",  F.lit(1).cast(IntegerType()))
            .withColumn("pipeline_load_ts", F.current_timestamp())
            .withColumn("load_date",        F.lit(load_date))
        )
        result.write.mode("overwrite").partitionBy("load_date").option("compression","snappy").parquet(silver_path)
        log.info("SCD2: Initial load – wrote %d rows", result.count())
        return result

    # Join incoming vs existing on PK
    incoming_alias  = incoming_with_hash.alias("inc")
    existing_alias  = existing_df.alias("ex")

    joined = incoming_alias.join(
        existing_alias.select(pk_col, "record_hash", "version_number"),
        on=pk_col,
        how="left",
    )

    # ── Changed records (hash differs) ────────────────────────
    changed_incoming = (
        joined
        .filter(
            F.col("ex.record_hash").isNull() |          # new record
            (F.col("inc.record_hash") != F.col("ex.record_hash"))  # updated
        )
        .select("inc.*")
        .withColumn("version_number",  F.coalesce(F.col("ex.version_number"), F.lit(0)) + 1)
        .withColumn("effective_date",  F.lit(now_ts).cast(TimestampType()))
        .withColumn("expiry_date",     _SCD2_EXPIRY_LIT)
        .withColumn("is_current",      F.lit(True).cast(BooleanType()))
        .withColumn("pipeline_load_ts", F.current_timestamp())
        .withColumn("load_date",        F.lit(load_date))
    )

    # ── Expire existing rows for changed records ───────────────
    changed_pks     = changed_incoming.select(pk_col)
    existing_expired = (
        existing_df
        .join(changed_pks, on=pk_col, how="inner")
        .withColumn("expiry_date",  F.lit(now_ts).cast(TimestampType()))
        .withColumn("is_current",   F.lit(False).cast(BooleanType()))
    )

    # ── Unchanged current rows ─────────────────────────────────
    existing_unchanged = existing_df.join(changed_pks, on=pk_col, how="left_anti")

    # ── Also read all historical (non-current) rows ────────────
    try:
        historical_df = spark.read.parquet(silver_path).filter(F.col("is_current") == False)
    except Exception:
        historical_df = spark.createDataFrame([], existing_df.schema)

    # ── Union all ─────────────────────────────────────────────
    final_df = (
        existing_unchanged
        .unionByName(existing_expired,  allowMissingColumns=True)
        .unionByName(historical_df,     allowMissingColumns=True)
        .unionByName(changed_incoming,  allowMissingColumns=True)
    )

    final_df.write.mode("overwrite").partitionBy("load_date").option("compression","snappy").parquet(silver_path)
    log.info(
        "SCD2: wrote %d rows (changed=%d, expired=%d, unchanged=%d)",
        final_df.count(), changed_incoming.count(),
        existing_expired.count(), existing_unchanged.count(),
    )
    return final_df


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def process_collection(
    spark: SparkSession,
    collection_name: str,
    run_id: str,
    dag_id: str = "manual",
    task_id: str = "silver_processing",
    load_date: Optional[str] = None,
) -> dict:
    start_time = utcnow()
    load_date  = load_date or start_time.strftime("%Y-%m-%d")
    status     = "FAILED"
    records_loaded = 0
    error_msg  = None

    try:
        meta = get_collection_metadata(collection_name)
        if not meta:
            raise ValueError(f"No metadata for: {collection_name}")

        pk_col      = meta["primary_key_column"]
        scd_type    = meta["scd_type"]
        watermark_col = meta["watermark_column"]
        bronze_path = meta["bronze_path"]
        silver_path = meta["silver_path"]

        # ── Read Bronze ───────────────────────────────────────
        raw_df = read_bronze(spark, bronze_path, load_date)

        # ── Cleanse ───────────────────────────────────────────
        clean_df = cleanse(raw_df, collection_name)

        # ── Deduplicate ───────────────────────────────────────
        wm_col_exists = watermark_col in clean_df.columns
        if wm_col_exists:
            deduped_df = deduplicate(clean_df, pk_col, watermark_col)
        else:
            deduped_df = clean_df.dropDuplicates([pk_col])

        # ── Hash ──────────────────────────────────────────────
        hashed_df = add_record_hash(deduped_df)

        # ── SCD Merge ─────────────────────────────────────────
        if scd_type == 2:
            result_df = apply_scd2(spark, hashed_df, silver_path, pk_col, load_date)
        else:
            result_df = apply_scd1(spark, hashed_df, silver_path, pk_col, load_date)

        records_loaded = result_df.count()
        status = "SUCCESS"

    except Exception as exc:
        error_msg = str(exc)
        log.exception("[%s] Silver processing failed: %s", collection_name, exc)

    finally:
        end_time = utcnow()
        insert_audit_record(
            run_id=run_id, dag_id=dag_id, task_id=task_id,
            collection_name=collection_name, layer="silver",
            start_time=start_time, end_time=end_time,
            records_extracted=0, records_loaded=records_loaded, records_failed=0,
            watermark_start=None, watermark_end=None,
            status=status, error_message=error_msg,
            hdfs_path=meta.get("silver_path") if "meta" in dir() else None,
        )

    return {"collection": collection_name, "records_loaded": records_loaded, "status": status}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--collection", required=True)
    p.add_argument("--run-id",     default=generate_run_id())
    p.add_argument("--dag-id",     default="manual")
    p.add_argument("--task-id",    default="silver_processing")
    p.add_argument("--load-date",  default=None)
    return p.parse_args()


def main():
    args   = parse_args()
    spark  = get_spark_session(app_name=f"Silver_{args.collection}")
    try:
        result = process_collection(
            spark, args.collection, args.run_id,
            args.dag_id, args.task_id, args.load_date,
        )
        log.info("Result: %s", result)
        if result["status"] == "FAILED":
            sys.exit(1)
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
