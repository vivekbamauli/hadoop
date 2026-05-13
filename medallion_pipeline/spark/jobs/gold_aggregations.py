"""
spark/jobs/gold_aggregations.py
────────────────────────────────
Silver → Gold layer: business-ready aggregations.

Tables produced:
  • customer_sales_summary
  • top_products
  • order_analytics
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, str(Path(__file__).parents[1]))

from utils.common import (
    generate_run_id,
    get_logger,
    insert_audit_record,
    load_config,
    utcnow,
)
from utils.spark_session import get_spark_session, stop_spark

log = get_logger("gold_aggregations")

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def read_silver(spark: SparkSession, path: str, current_only: bool = False) -> DataFrame:
    df = spark.read.parquet(path)
    if current_only and "is_current" in df.columns:
        df = df.filter(F.col("is_current") == True)
    return df


def write_gold(df: DataFrame, path: str, report_date: str) -> str:
    out = f"{path}/report_date={report_date}"
    df.write.mode("overwrite").option("compression","snappy").parquet(out)
    log.info("Written gold → %s  (%d rows)", out, df.count())
    return out


# ──────────────────────────────────────────────────────────────
# 1. Customer Sales Summary
# ──────────────────────────────────────────────────────────────

def build_customer_sales_summary(
    spark: SparkSession, cfg: dict, report_date: str
) -> str:
    hdfs = cfg["hdfs"]
    customers_df = read_silver(spark, f"{hdfs['silver_path']}/customers", current_only=True)
    orders_df    = read_silver(spark, f"{hdfs['silver_path']}/orders")

    order_agg = (
        orders_df
        .filter(F.col("status").isin("completed", "delivered"))
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("created_at").alias("first_order_date"),
            F.max("created_at").alias("last_order_date"),
        )
    )

    customer_sales = (
        customers_df
        .join(order_agg, on="customer_id", how="left")
        .select(
            "customer_id",
            F.coalesce(F.col("full_name"), F.concat_ws(" ", F.col("first_name"), F.col("last_name"))).alias("full_name"),
            "email",
            "country",
            F.coalesce("total_orders",    F.lit(0)).alias("total_orders"),
            F.coalesce("total_revenue",   F.lit(0.0)).alias("total_revenue"),
            F.coalesce("avg_order_value", F.lit(0.0)).alias("avg_order_value"),
            "first_order_date",
            "last_order_date",
            # Segment customers by revenue
            F.when(F.col("total_revenue") > 10000,  "Platinum")
             .when(F.col("total_revenue") > 5000,   "Gold")
             .when(F.col("total_revenue") > 1000,   "Silver")
             .otherwise("Bronze")
             .alias("customer_segment"),
            # LTV tier
            F.when(F.col("total_orders") >= 10, "High")
             .when(F.col("total_orders") >= 3,  "Medium")
             .otherwise("Low")
             .alias("lifetime_value_tier"),
            F.current_timestamp().alias("pipeline_load_ts"),
        )
    )

    gold_path = f"{hdfs['gold_path']}/customer_sales_summary"
    return write_gold(customer_sales, gold_path, report_date)


# ──────────────────────────────────────────────────────────────
# 2. Top Products
# ──────────────────────────────────────────────────────────────

def build_top_products(
    spark: SparkSession, cfg: dict, report_date: str
) -> str:
    hdfs = cfg["hdfs"]
    products_df = read_silver(spark, f"{hdfs['silver_path']}/products", current_only=True)
    orders_df   = read_silver(spark, f"{hdfs['silver_path']}/orders")
    reviews_df  = read_silver(spark, f"{hdfs['silver_path']}/reviews")

    # Explode line items from orders (stored as JSON in items column)
    # Fallback: join on product_id if items is already flattened
    order_items = (
        orders_df
        .withColumn("items_parsed", F.from_json(
            F.col("items"),
            "array<struct<product_id:string,qty:int,unit_price:double>>"
        ))
        .withColumn("item", F.explode_outer(F.col("items_parsed")))
        .select(
            F.col("item.product_id").alias("product_id"),
            F.col("item.qty").alias("qty"),
            F.col("item.unit_price").alias("unit_price"),
        )
        .groupBy("product_id")
        .agg(
            F.sum("qty").alias("total_units_sold"),
            F.sum(F.col("qty") * F.col("unit_price")).alias("total_revenue"),
        )
    )

    review_agg = (
        reviews_df
        .groupBy("product_id")
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.count("review_id").alias("review_count"),
        )
    )

    from pyspark.sql import Window
    w_cat = Window.partitionBy("category").orderBy(F.col("total_revenue").desc())

    top_products = (
        products_df
        .join(order_items, on="product_id", how="left")
        .join(review_agg,  on="product_id", how="left")
        .select(
            "product_id",
            F.col("name").alias("product_name"),
            "category",
            "sub_category",
            F.coalesce("total_units_sold", F.lit(0)).cast("long").alias("total_units_sold"),
            F.coalesce("total_revenue",    F.lit(0.0)).alias("total_revenue"),
            F.round(F.coalesce("avg_rating", F.lit(0.0)), 2).alias("avg_rating"),
            F.coalesce("review_count",     F.lit(0)).cast("long").alias("review_count"),
            F.lit(0.0).alias("return_rate"),          # placeholder
            F.current_timestamp().alias("pipeline_load_ts"),
        )
        .withColumn("rank_in_category", F.rank().over(w_cat))
    )

    gold_path = f"{hdfs['gold_path']}/top_products"
    return write_gold(top_products, gold_path, report_date)


# ──────────────────────────────────────────────────────────────
# 3. Order Analytics
# ──────────────────────────────────────────────────────────────

def build_order_analytics(
    spark: SparkSession, cfg: dict, report_date: str
) -> str:
    hdfs = cfg["hdfs"]
    orders_df   = read_silver(spark, f"{hdfs['silver_path']}/orders")
    customers_df = read_silver(spark, f"{hdfs['silver_path']}/customers", current_only=True)
    products_df  = read_silver(spark, f"{hdfs['silver_path']}/products",  current_only=True)

    enriched = (
        orders_df
        .join(customers_df.select("customer_id", "country"), on="customer_id", how="left")
        .withColumn("order_date", F.date_format("created_at", "yyyy-MM-dd"))
    )

    analytics = (
        enriched
        .groupBy("order_date", "country")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum(F.when(F.col("status") == "completed",  1).otherwise(0)).alias("completed_orders"),
            F.sum(F.when(F.col("status") == "cancelled",  1).otherwise(0)).alias("cancelled_orders"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
        )
        .withColumn(
            "completion_rate",
            F.round(F.col("completed_orders") / F.col("total_orders"), 4)
        )
        .withColumn("category",         F.lit("ALL"))
        .withColumn("pipeline_load_ts", F.current_timestamp())
    )

    gold_path = f"{hdfs['gold_path']}/order_analytics"
    return write_gold(analytics, gold_path, report_date)


# ──────────────────────────────────────────────────────────────
# Orchestrate All Gold Tables
# ──────────────────────────────────────────────────────────────

def build_all_gold(
    spark: SparkSession,
    run_id: str,
    dag_id: str = "manual",
    task_id: str = "gold_aggregations",
    report_date: Optional[str] = None,
) -> dict:
    cfg         = load_config()
    start_time  = utcnow()
    report_date = report_date or start_time.strftime("%Y-%m-%d")
    results     = {}
    status      = "SUCCESS"

    builders = {
        "customer_sales_summary": build_customer_sales_summary,
        "top_products":           build_top_products,
        "order_analytics":        build_order_analytics,
    }

    for table_name, builder_fn in builders.items():
        try:
            log.info("Building gold table: %s", table_name)
            path = builder_fn(spark, cfg, report_date)
            results[table_name] = {"status": "SUCCESS", "path": path}
        except Exception as exc:
            log.exception("Failed to build gold table [%s]: %s", table_name, exc)
            results[table_name] = {"status": "FAILED", "error": str(exc)}
            status = "PARTIAL"

    end_time = utcnow()
    insert_audit_record(
        run_id=run_id, dag_id=dag_id, task_id=task_id,
        collection_name="gold_layer", layer="gold",
        start_time=start_time, end_time=end_time,
        records_extracted=0, records_loaded=0, records_failed=0,
        watermark_start=None, watermark_end=None,
        status=status, error_message=None,
        hdfs_path=cfg["hdfs"]["gold_path"],
    )

    return results


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--run-id",      default=generate_run_id())
    p.add_argument("--dag-id",      default="manual")
    p.add_argument("--task-id",     default="gold_aggregations")
    p.add_argument("--report-date", default=None)
    return p.parse_args()


def main():
    args  = parse_args()
    spark = get_spark_session(app_name="Gold_Aggregations")
    try:
        results = build_all_gold(
            spark, args.run_id, args.dag_id, args.task_id, args.report_date
        )
        log.info("Gold results: %s", results)
        if any(v["status"] == "FAILED" for v in results.values()):
            sys.exit(1)
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
