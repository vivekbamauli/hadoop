"""
spark/utils/schema_evolution.py
────────────────────────────────
Automatic schema evolution handler for Parquet files on HDFS.

When an incoming DataFrame has new or removed columns compared to
the existing Silver table, this module reconciles the schemas and
logs the change to PostgreSQL for observability.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

from .common import get_logger, get_pg_connection, release_pg_connection, utcnow

log = get_logger("schema_evolution")


def get_existing_schema(spark: SparkSession, path: str) -> Optional[StructType]:
    """Read the schema of an existing Parquet dataset; return None if absent."""
    try:
        return spark.read.parquet(path).schema
    except Exception:
        return None


def compare_schemas(
    existing: StructType, incoming: StructType
) -> Tuple[List[str], List[str], List[str]]:
    """
    Compare two schemas.
    Returns (added_columns, dropped_columns, type_changed_columns).
    """
    ex_fields = {f.name: f.dataType for f in existing.fields}
    in_fields = {f.name: f.dataType for f in incoming.fields}

    added   = [n for n in in_fields if n not in ex_fields]
    dropped = [n for n in ex_fields if n not in in_fields]
    changed = [
        n for n in in_fields
        if n in ex_fields and str(in_fields[n]) != str(ex_fields[n])
    ]
    return added, dropped, changed


def reconcile_schemas(
    existing_df: DataFrame,
    incoming_df: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """
    Add missing columns (as NULL) to each DataFrame so they can be unioned.
    Existing columns take precedence for type conflicts (cast incoming).
    """
    ex_fields = {f.name: f.dataType for f in existing_df.schema.fields}
    in_fields = {f.name: f.dataType for f in incoming_df.schema.fields}

    # Add missing columns to incoming
    for col_name, dtype in ex_fields.items():
        if col_name not in in_fields:
            incoming_df = incoming_df.withColumn(col_name, F.lit(None).cast(dtype))
            log.debug("Added NULL column '%s' to incoming DataFrame", col_name)

    # Add missing columns to existing
    for col_name, dtype in in_fields.items():
        if col_name not in ex_fields:
            existing_df = existing_df.withColumn(col_name, F.lit(None).cast(dtype))
            log.debug("Added NULL column '%s' to existing DataFrame", col_name)

    # Align column order
    all_cols = list(dict.fromkeys(list(ex_fields.keys()) + list(in_fields.keys())))
    existing_df  = existing_df.select(*[c for c in all_cols if c in [f.name for f in existing_df.schema]])
    incoming_df  = incoming_df.select(*[c for c in all_cols if c in [f.name for f in incoming_df.schema]])

    return existing_df, incoming_df


def log_schema_change(
    collection_name: str,
    change_type: str,
    column_name: str,
    old_schema: Optional[dict],
    new_schema: Optional[dict],
) -> None:
    """Persist schema change event to PostgreSQL for observability."""
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO schema_evolution_log
                    (collection_name, change_type, column_name, old_schema, new_schema)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    collection_name,
                    change_type,
                    column_name,
                    json.dumps(old_schema) if old_schema else None,
                    json.dumps(new_schema) if new_schema else None,
                ),
            )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.warning("Failed to log schema change: %s", exc)
    finally:
        release_pg_connection(conn)


def handle_schema_evolution(
    spark: SparkSession,
    incoming_df: DataFrame,
    target_path: str,
    collection_name: str,
) -> Tuple[DataFrame, Optional[DataFrame]]:
    """
    Main entry point: detect schema drift, reconcile schemas,
    log changes, and return aligned (existing_df, incoming_df).

    Returns (None, incoming_df) if no existing data.
    """
    existing_schema = get_existing_schema(spark, target_path)

    if existing_schema is None:
        log.info("[%s] No existing schema – first load, skipping evolution check", collection_name)
        return None, incoming_df

    added, dropped, changed = compare_schemas(existing_schema, incoming_df.schema)

    if not any([added, dropped, changed]):
        log.info("[%s] Schema unchanged", collection_name)
        existing_df = spark.read.parquet(target_path)
        return existing_df, incoming_df

    log.warning(
        "[%s] Schema drift detected | added=%s | dropped=%s | changed=%s",
        collection_name, added, dropped, changed,
    )

    # Log each change
    old_schema_dict = {f.name: str(f.dataType) for f in existing_schema.fields}
    new_schema_dict = {f.name: str(f.dataType) for f in incoming_df.schema.fields}

    for col in added:
        log_schema_change(collection_name, "ADD_COLUMN", col, old_schema_dict, new_schema_dict)
    for col in dropped:
        log_schema_change(collection_name, "DROP_COLUMN", col, old_schema_dict, new_schema_dict)
    for col in changed:
        log_schema_change(collection_name, "TYPE_CHANGE", col, old_schema_dict, new_schema_dict)

    # Reconcile so both DataFrames can be unioned
    existing_df = spark.read.parquet(target_path)
    existing_aligned, incoming_aligned = reconcile_schemas(existing_df, incoming_df)

    return existing_aligned, incoming_aligned
