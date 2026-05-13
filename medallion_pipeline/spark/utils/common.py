"""
utils/common.py
───────────────
Shared utility functions: config, logging, DB, HDFS helpers.
"""

from __future__ import annotations

import hashlib
import json
import logging
import logging.handlers
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import yaml
from psycopg2.pool import ThreadedConnectionPool

# ──────────────────────────────────────────────────────────────
# Config Loader
# ──────────────────────────────────────────────────────────────

_CONFIG_PATH = Path(__file__).parents[2] / "config" / "pipeline_config.yaml"
_config_cache: Optional[Dict] = None


def load_config(path: str | Path = _CONFIG_PATH) -> Dict:
    """Load and cache pipeline YAML config."""
    global _config_cache
    if _config_cache is None:
        with open(path, "r") as fh:
            _config_cache = yaml.safe_load(fh)
    return _config_cache


# ──────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────

def get_logger(name: str) -> logging.Logger:
    """Return a configured rotating-file + console logger."""
    cfg = load_config().get("logging", {})
    log_dir = Path(cfg.get("log_dir", "/var/log/medallion_pipeline"))
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(getattr(logging, cfg.get("level", "INFO")))

    fmt = logging.Formatter(
        cfg.get("format", "%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )

    # Rotating file handler
    fh = logging.handlers.RotatingFileHandler(
        log_dir / f"{name.replace('.', '_')}.log",
        maxBytes=cfg.get("max_bytes", 10_485_760),
        backupCount=cfg.get("backup_count", 5),
    )
    fh.setFormatter(fmt)

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ──────────────────────────────────────────────────────────────
# PostgreSQL Connection Pool
# ──────────────────────────────────────────────────────────────

_pg_pool: Optional[ThreadedConnectionPool] = None


def get_pg_pool() -> ThreadedConnectionPool:
    global _pg_pool
    if _pg_pool is None:
        cfg = load_config()["postgresql"]
        _pg_pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=cfg.get("pool_size", 5),
            host=cfg["host"],
            port=cfg.get("port", 5432),
            dbname=cfg["database"],
            user=cfg["username"],
            password=cfg["password"],
        )
    return _pg_pool


def get_pg_connection():
    """Return a connection from the pool (caller must release)."""
    return get_pg_pool().getconn()


def release_pg_connection(conn) -> None:
    get_pg_pool().putconn(conn)


# ──────────────────────────────────────────────────────────────
# Metadata Helpers
# ──────────────────────────────────────────────────────────────

def fetch_active_collections() -> List[Dict]:
    """Return all active collections from the metadata table."""
    conn = get_pg_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT collection_name, watermark_column, last_loaded_timestamp,
                       scd_type, primary_key_column, bronze_path, silver_path, gold_path
                FROM   collections_metadata
                WHERE  active_flag = TRUE
                ORDER  BY collection_name
                """
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_pg_connection(conn)


def get_collection_metadata(collection_name: str) -> Optional[Dict]:
    """Return metadata row for a single collection."""
    conn = get_pg_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM collections_metadata
                WHERE  collection_name = %s AND active_flag = TRUE
                """,
                (collection_name,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        release_pg_connection(conn)


def update_watermark(collection_name: str, new_watermark: datetime) -> None:
    """Advance the watermark for a collection after a successful load."""
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE collections_metadata
                SET    last_loaded_timestamp = %s, updated_at = NOW()
                WHERE  collection_name = %s
                """,
                (new_watermark, collection_name),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_pg_connection(conn)


# ──────────────────────────────────────────────────────────────
# Audit Helpers
# ──────────────────────────────────────────────────────────────

def insert_audit_record(
    run_id: str,
    dag_id: str,
    task_id: str,
    collection_name: str,
    layer: str,
    start_time: datetime,
    end_time: datetime,
    records_extracted: int,
    records_loaded: int,
    records_failed: int,
    watermark_start: Optional[datetime],
    watermark_end: Optional[datetime],
    status: str,
    error_message: Optional[str] = None,
    hdfs_path: Optional[str] = None,
) -> int:
    """Insert a pipeline audit record and return its id."""
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_audit (
                    run_id, dag_id, task_id, collection_name, layer,
                    pipeline_start_time, pipeline_end_time, duration_seconds,
                    records_extracted, records_loaded, records_failed,
                    watermark_start, watermark_end,
                    status, error_message, hdfs_output_path
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, EXTRACT(EPOCH FROM (%s - %s)),
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s
                ) RETURNING id
                """,
                (
                    run_id, dag_id, task_id, collection_name, layer,
                    start_time, end_time, end_time, start_time,
                    records_extracted, records_loaded, records_failed,
                    watermark_start, watermark_end,
                    status, error_message, hdfs_path,
                ),
            )
            audit_id = cur.fetchone()[0]
        conn.commit()
        return audit_id
    except Exception:
        conn.rollback()
        raise
    finally:
        release_pg_connection(conn)


# ──────────────────────────────────────────────────────────────
# Hashing / Record Comparison
# ──────────────────────────────────────────────────────────────

def compute_record_hash(row_dict: Dict, exclude_cols: Optional[List[str]] = None) -> str:
    """Compute MD5 hash of a row dict (excluding audit/SCD columns)."""
    exclude = set(exclude_cols or [
        "effective_date", "expiry_date", "is_current",
        "version_number", "record_hash", "pipeline_load_ts",
        "load_date",
    ])
    filtered = {k: v for k, v in sorted(row_dict.items()) if k not in exclude}
    serialized = json.dumps(filtered, default=str, sort_keys=True)
    return hashlib.md5(serialized.encode()).hexdigest()


# ──────────────────────────────────────────────────────────────
# General Utilities
# ──────────────────────────────────────────────────────────────

def generate_run_id() -> str:
    return str(uuid.uuid4())


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def date_str(dt: Optional[datetime] = None) -> str:
    return (dt or utcnow()).strftime("%Y-%m-%d")


def validate_env_vars(required: List[str]) -> None:
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {missing}")
