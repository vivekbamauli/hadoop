"""
airflow/dags/medallion_pipeline_dag.py
───────────────────────────────────────
Production Airflow DAG for the Medallion Data Pipeline.

Features:
  • Dynamic task generation from PostgreSQL metadata
  • Parallel collection processing
  • Bronze → Silver → Gold task dependencies per collection
  • Retry / backoff configuration
  • Email alerting on failure
  • Full audit trail via PostgreSQL
"""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# ──────────────────────────────────────────────────────────────
# Airflow logger
# ──────────────────────────────────────────────────────────────
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# Default arguments
# ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "email":            ["data-engineering@company.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":  timedelta(minutes=30),
}

# ──────────────────────────────────────────────────────────────
# Helpers to call Spark submit
# ──────────────────────────────────────────────────────────────
SPARK_HOME    = Variable.get("SPARK_HOME",    default_var="/opt/spark")
PIPELINE_HOME = Variable.get("PIPELINE_HOME", default_var="/opt/medallion_pipeline")
PYTHON_ENV    = Variable.get("PYTHON_ENV",    default_var="python3")

SPARK_SUBMIT = f"{SPARK_HOME}/bin/spark-submit"
SPARK_PACKAGES = (
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,"
    "org.apache.spark:spark-avro_2.12:3.5.0"
)

SPARK_CONF_ARGS = [
    "--master",            "yarn",
    "--deploy-mode",       "client",
    "--executor-memory",   "4g",
    "--driver-memory",     "2g",
    "--executor-cores",    "2",
    "--num-executors",     "4",
    "--packages",          SPARK_PACKAGES,
    "--py-files",          f"{PIPELINE_HOME}/spark/utils.zip",
]


def _spark_submit(script: str, extra_args: List[str]) -> None:
    """Execute a spark-submit command and raise on non-zero return code."""
    cmd = [SPARK_SUBMIT] + SPARK_CONF_ARGS + [script] + extra_args
    log.info("Executing: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    log.info("STDOUT:\n%s", result.stdout[-5000:])
    if result.returncode != 0:
        log.error("STDERR:\n%s", result.stderr[-5000:])
        raise RuntimeError(
            f"spark-submit failed (rc={result.returncode}): {result.stderr[-2000:]}"
        )


# ──────────────────────────────────────────────────────────────
# Task callables
# ──────────────────────────────────────────────────────────────

def fetch_active_collections_task(**context) -> List[Dict]:
    """Read active collections from PostgreSQL and push to XCom."""
    import sys
    sys.path.insert(0, PIPELINE_HOME)
    from spark.utils.common import fetch_active_collections, generate_run_id

    run_id      = generate_run_id()
    collections = fetch_active_collections()
    log.info("Active collections: %s", [c["collection_name"] for c in collections])

    context["task_instance"].xcom_push(key="run_id",      value=run_id)
    context["task_instance"].xcom_push(key="collections", value=json.dumps(collections, default=str))
    return collections


def bronze_ingestion_task(collection_name: str, **context) -> None:
    """Trigger bronze Spark job for one collection."""
    ti          = context["task_instance"]
    run_id      = ti.xcom_pull(task_ids="fetch_metadata", key="run_id")
    dag_id      = context["dag"].dag_id
    task_id     = context["task"].task_id
    exec_date   = context["ds"]           # YYYY-MM-DD

    script = f"{PIPELINE_HOME}/spark/jobs/bronze_ingestion.py"
    _spark_submit(script, [
        "--collection",  collection_name,
        "--run-id",      run_id,
        "--dag-id",      dag_id,
        "--task-id",     task_id,
        "--upper-bound", f"{exec_date}T23:59:59",
    ])


def silver_processing_task(collection_name: str, **context) -> None:
    """Trigger silver Spark job for one collection."""
    ti      = context["task_instance"]
    run_id  = ti.xcom_pull(task_ids="fetch_metadata", key="run_id")
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    exec_date = context["ds"]

    script = f"{PIPELINE_HOME}/spark/jobs/silver_processing.py"
    _spark_submit(script, [
        "--collection", collection_name,
        "--run-id",     run_id,
        "--dag-id",     dag_id,
        "--task-id",    task_id,
        "--load-date",  exec_date,
    ])


def gold_aggregations_task(**context) -> None:
    """Trigger gold aggregation Spark job (runs once after all silver tasks)."""
    ti          = context["task_instance"]
    run_id      = ti.xcom_pull(task_ids="fetch_metadata", key="run_id")
    dag_id      = context["dag"].dag_id
    exec_date   = context["ds"]

    script = f"{PIPELINE_HOME}/spark/jobs/gold_aggregations.py"
    _spark_submit(script, [
        "--run-id",      run_id,
        "--dag-id",      dag_id,
        "--task-id",     "gold_aggregations",
        "--report-date", exec_date,
    ])


def repair_hive_partitions_task(**context) -> None:
    """Run MSCK REPAIR TABLE for all Hive external tables."""
    tables = [
        "medallion.bronze_customers",  "medallion.bronze_orders",
        "medallion.bronze_products",   "medallion.bronze_payments",
        "medallion.bronze_reviews",
        "medallion.silver_customers",  "medallion.silver_orders",
        "medallion.silver_products",
        "medallion.gold_customer_sales_summary",
        "medallion.gold_top_products", "medallion.gold_order_analytics",
    ]
    hive_server = Variable.get("HIVE_SERVER2", default_var="localhost:10000")
    for tbl in tables:
        cmd = [
            "beeline", "-u", f"jdbc:hive2://{hive_server}",
            "-e", f"MSCK REPAIR TABLE {tbl};",
        ]
        log.info("Repairing Hive partitions for %s", tbl)
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            log.warning("Hive repair warning for %s: %s", tbl, result.stderr[:500])


def pipeline_success_callback(context) -> None:
    log.info(
        "✅ Pipeline completed successfully | dag=%s | run=%s | date=%s",
        context["dag"].dag_id,
        context["run_id"],
        context["ds"],
    )


def pipeline_failure_callback(context) -> None:
    log.error(
        "❌ Pipeline FAILED | dag=%s | task=%s | date=%s | exception=%s",
        context["dag"].dag_id,
        context["task_instance"].task_id,
        context["ds"],
        context.get("exception"),
    )


# ──────────────────────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────────────────────

with DAG(
    dag_id              = "medallion_data_pipeline",
    description         = "Metadata-driven MongoDB→Bronze→Silver→Gold pipeline",
    default_args        = DEFAULT_ARGS,
    schedule_interval   = "0 2 * * *",     # Daily at 02:00 UTC
    catchup             = False,
    max_active_runs     = 1,
    concurrency         = 8,
    tags                = ["medallion", "mongodb", "spark", "hdfs"],
    on_success_callback = pipeline_success_callback,
    on_failure_callback = pipeline_failure_callback,
    doc_md="""
## Medallion Data Pipeline

End-to-end incremental ingestion from **MongoDB Atlas** to **HDFS** 
using the Medallion Architecture (Bronze / Silver / Gold).

### Workflow
1. Read active collections from PostgreSQL metadata
2. For each collection (parallel):
   - Bronze ingestion (MongoDB → HDFS raw parquet)
   - Silver processing (cleansing + SCD merge)
3. Gold aggregation (business-ready tables)
4. Hive partition repair

### Monitoring
Audit records are written to `pipeline_audit` after every task.
    """,
) as dag:

    # ── Start ──────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Fetch metadata ─────────────────────────────────────────
    fetch_metadata = PythonOperator(
        task_id         = "fetch_metadata",
        python_callable = fetch_active_collections_task,
        provide_context = True,
    )

    # ── Dynamic per-collection task groups ────────────────────
    # Collections are determined at parse time via metadata query.
    # For fully dynamic DAGs at runtime, see the TaskFlow API variant.

    import sys; sys.path.insert(0, PIPELINE_HOME)
    try:
        from spark.utils.common import fetch_active_collections
        active_collections = fetch_active_collections()
    except Exception:
        # Fallback when metadata DB is not yet available at parse time
        active_collections = [
            {"collection_name": c}
            for c in ["customers", "orders", "products", "payments", "reviews"]
        ]

    all_silver_tasks = []

    for meta in active_collections:
        cname = meta["collection_name"]

        with TaskGroup(group_id=f"collection_{cname}") as tg:

            bronze_task = PythonOperator(
                task_id         = f"bronze_{cname}",
                python_callable = bronze_ingestion_task,
                op_kwargs       = {"collection_name": cname},
                provide_context = True,
            )

            silver_task = PythonOperator(
                task_id         = f"silver_{cname}",
                python_callable = silver_processing_task,
                op_kwargs       = {"collection_name": cname},
                provide_context = True,
            )

            bronze_task >> silver_task

        all_silver_tasks.append(tg)

    # ── Gold (after all silver tasks) ─────────────────────────
    gold_task = PythonOperator(
        task_id         = "gold_aggregations",
        python_callable = gold_aggregations_task,
        provide_context = True,
    )

    # ── Hive repair ───────────────────────────────────────────
    hive_repair = PythonOperator(
        task_id         = "hive_partition_repair",
        python_callable = repair_hive_partitions_task,
        provide_context = True,
    )

    # ── End ────────────────────────────────────────────────────
    end = EmptyOperator(task_id="end")

    # ── Wire up ────────────────────────────────────────────────
    start >> fetch_metadata >> all_silver_tasks >> gold_task >> hive_repair >> end
