#!/usr/bin/env python3
"""
scripts/onboard_collection.py
──────────────────────────────
CLI tool to onboard a new MongoDB collection into the pipeline.

Steps performed automatically:
  1. Register collection in PostgreSQL metadata table
  2. Create HDFS Bronze / Silver / Gold directories
  3. Print Hive DDL to add (manual copy-paste step)

Usage:
  python onboard_collection.py \\
    --collection invoices \\
    --pk-column  invoice_id \\
    --watermark  updated_at \\
    --scd-type   1
"""

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from spark.utils.common import get_pg_connection, release_pg_connection, load_config, get_logger

log = get_logger("onboard_collection")

def register_metadata(collection: str, pk: str, watermark: str, scd: int) -> None:
    cfg  = load_config()
    base = cfg["hdfs"]

    bronze = f"{base['bronze_path']}/{collection}"
    silver = f"{base['silver_path']}/{collection}"
    gold   = f"{base['gold_path']}/{collection}"

    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO collections_metadata
                    (collection_name, watermark_column, scd_type, primary_key_column,
                     bronze_path, silver_path, gold_path, active_flag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT (collection_name) DO UPDATE
                  SET watermark_column   = EXCLUDED.watermark_column,
                      scd_type           = EXCLUDED.scd_type,
                      primary_key_column = EXCLUDED.primary_key_column,
                      active_flag        = TRUE,
                      updated_at         = NOW()
                """,
                (collection, watermark, scd, pk, bronze, silver, gold),
            )
        conn.commit()
        log.info("✅ Metadata registered for [%s]", collection)
    finally:
        release_pg_connection(conn)


def create_hdfs_dirs(collection: str) -> None:
    cfg  = load_config()
    base = cfg["hdfs"]
    nn   = f"hdfs://{base['namenode_host']}:{base['namenode_port']}"

    for layer in ("bronze", "silver", "gold"):
        path = f"{nn}/{layer}/{collection}"
        cmd  = ["hdfs", "dfs", "-mkdir", "-p", path]
        log.info("Creating HDFS dir: %s", path)
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            log.warning("hdfs mkdir returned non-zero: %s", result.stderr)
        else:
            log.info("  ✓ %s", path)


def print_hive_ddl(collection: str, pk: str) -> None:
    cfg  = load_config()
    base = cfg["hdfs"]

    ddl = f"""
-- ── Add to sql/hive/hive_tables.sql ──────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_{collection} (
    _id          STRING,
    {pk}         STRING,
    -- TODO: add your columns here
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP,
    raw_document STRING
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '{base["bronze_path"]}/{collection}'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS silver_{collection} (
    {pk}             STRING,
    -- TODO: add your columns here
    created_at       TIMESTAMP,
    updated_at       TIMESTAMP,
    record_hash      STRING,
    pipeline_load_ts TIMESTAMP
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION '{base["silver_path"]}/{collection}'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

MSCK REPAIR TABLE bronze_{collection};
MSCK REPAIR TABLE silver_{collection};
"""
    print("\n" + "═" * 60)
    print("📋 Hive DDL (copy and run manually):")
    print("═" * 60)
    print(ddl)


def main() -> None:
    p = argparse.ArgumentParser(description="Onboard a new MongoDB collection")
    p.add_argument("--collection", required=True)
    p.add_argument("--pk-column",  required=True, dest="pk")
    p.add_argument("--watermark",  default="updated_at")
    p.add_argument("--scd-type",   type=int, default=1, choices=[1, 2])
    p.add_argument("--skip-hdfs",  action="store_true", help="Skip HDFS dir creation")
    args = p.parse_args()

    log.info("Onboarding collection: %s", args.collection)

    register_metadata(args.collection, args.pk, args.watermark, args.scd_type)

    if not args.skip_hdfs:
        create_hdfs_dirs(args.collection)

    print_hive_ddl(args.collection, args.pk)

    print(f"\n✅ Collection [{args.collection}] is ready!")
    print("   The next Airflow run will automatically pick it up.")


if __name__ == "__main__":
    main()
