"""
utils/spark_session.py
──────────────────────
Centralised SparkSession factory with MongoDB connector + HDFS config.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

from .common import load_config, get_logger

log = get_logger(__name__)


def get_spark_session(
    app_name: Optional[str] = None,
    extra_configs: Optional[dict] = None,
) -> SparkSession:
    """
    Create (or retrieve) a configured SparkSession.

    Extra JAR packages and MongoDB Atlas connection details are
    injected from pipeline_config.yaml so no secrets live in code.
    """
    cfg = load_config()
    spark_cfg = cfg["spark"]
    mongo_cfg = cfg["mongodb"]

    app = app_name or spark_cfg.get("app_name", "MedallionPipeline")
    packages = ",".join(spark_cfg.get("packages", []))

    log.info("Initialising SparkSession: %s | master=%s", app, spark_cfg["master"])

    builder = (
        SparkSession.builder
        .appName(app)
        .master(spark_cfg["master"])
        # ── Resource settings ─────────────────────────────────
        .config("spark.executor.memory",            spark_cfg.get("executor_memory", "4g"))
        .config("spark.driver.memory",              spark_cfg.get("driver_memory", "2g"))
        .config("spark.executor.cores",             str(spark_cfg.get("executor_cores", 2)))
        .config("spark.sql.shuffle.partitions",     str(spark_cfg.get("shuffle_partitions", 200)))
        # ── MongoDB Connector ─────────────────────────────────
        .config("spark.mongodb.read.connection.uri",  mongo_cfg["uri"])
        .config("spark.mongodb.write.connection.uri", mongo_cfg["uri"])
        # ── Parquet / HDFS ────────────────────────────────────
        .config("spark.sql.parquet.compression.codec",   "snappy")
        .config("spark.sql.parquet.mergeSchema",          "true")
        .config("spark.sql.parquet.filterPushdown",       "true")
        .config("spark.hadoop.parquet.enable.summary-metadata", "false")
        # ── Performance ───────────────────────────────────────
        .config("spark.sql.adaptive.enabled",               "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer",    "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.broadcastTimeout", "600")
        # ── Jars ─────────────────────────────────────────────
        .config("spark.jars.packages", packages)
    )

    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    log.info("SparkSession created | version=%s", spark.version)
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Gracefully stop the SparkSession."""
    try:
        spark.stop()
        log.info("SparkSession stopped.")
    except Exception as exc:
        log.warning("Error stopping SparkSession: %s", exc)
