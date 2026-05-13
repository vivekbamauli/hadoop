# 🏗️ Medallion Data Pipeline
### Enterprise-Grade MongoDB → HDFS Data Engineering System

> **Stack:** MongoDB Atlas · Apache Airflow 2.9 · Apache Spark 3.5 · Hadoop HDFS 3.2 · PostgreSQL 15 · Apache Hive 2.3

---

## 📐 Architecture

```
MongoDB Atlas (Source)
        │
        │  Incremental Extract (Spark + watermark)
        ▼
┌───────────────────────────────────────┐
│         HDFS  /data-lake/             │
│                                       │
│  🟤 Bronze  ← raw Parquet (daily)     │
│       │                               │
│  ⚪  Silver ← cleansed + SCD merge   │
│       │                               │
│  🟡 Gold   ← aggregated analytics    │
└───────────────────────────────────────┘
        │
        │  External Tables
        ▼
    Apache Hive
        │
    BI / Dashboards
```

**Orchestration:** Airflow reads `collections_metadata` from PostgreSQL, dynamically generates task groups per collection, and logs every run to `pipeline_audit`.

---

## 📁 Project Structure

```
medallion_pipeline/
├── airflow/
│   └── dags/
│       └── medallion_pipeline_dag.py   # Main DAG (dynamic tasks)
├── spark/
│   ├── jobs/
│   │   ├── bronze_ingestion.py         # MongoDB → Bronze
│   │   ├── silver_processing.py        # Bronze → Silver (SCD1/2)
│   │   └── gold_aggregations.py        # Silver → Gold tables
│   └── utils/
│       ├── common.py                   # Config, logging, DB helpers
│       └── spark_session.py            # SparkSession factory
├── sql/
│   ├── metadata/
│   │   └── 01_create_metadata_tables.sql
│   └── hive/
│       └── hive_tables.sql
├── docker/
│   ├── docker-compose.yml
│   └── hadoop.env
├── config/
│   └── pipeline_config.yaml
├── data/
│   └── generate_sample_data.py
├── scripts/
│   └── setup_hdfs.sh
├── tests/
│   └── test_silver_scd.py
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start (Docker)

### Prerequisites
- Docker ≥ 24 + Docker Compose v2
- MongoDB Atlas cluster (free tier works)

### 1 – Clone & configure

```bash
git clone <repo>
cd medallion_pipeline

# Edit MongoDB URI in config/pipeline_config.yaml
nano config/pipeline_config.yaml
```

### 2 – Start the stack

```bash
cd docker
docker compose up -d

# Wait ~90 s for all services to become healthy
docker compose ps
```

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 (admin / admin) |
| Spark UI | http://localhost:8090 |
| HDFS NameNode UI | http://localhost:9870 |
| Hive Server2 | localhost:10000 |

### 3 – Initialise PostgreSQL metadata

```bash
docker exec -it medallion-postgres \
  psql -U pipeline_user -d pipeline_metadata \
  -f /docker-entrypoint-initdb.d/01_metadata.sql
```

### 4 – Initialise HDFS

```bash
docker exec -it medallion-namenode bash /opt/medallion_pipeline/scripts/setup_hdfs.sh
```

### 5 – Generate sample data

```bash
pip install pymongo faker
python data/generate_sample_data.py \
  --uri "mongodb+srv://user:pass@cluster.mongodb.net" \
  --records 500
```

### 6 – Create Hive tables

```bash
docker exec -it medallion-hive-server \
  beeline -u jdbc:hive2://localhost:10000 \
  -f /opt/medallion_pipeline/sql/hive/hive_tables.sql
```

### 7 – Trigger the pipeline

Log into Airflow at http://localhost:8080, enable the `medallion_data_pipeline` DAG and trigger a manual run — or wait for the 02:00 UTC schedule.

---

## ⚙️ Manual Execution (without Docker)

### Prerequisites (Ubuntu / WSL)

```bash
# Java 11
sudo apt-get install -y openjdk-11-jdk

# Hadoop 3.2.1
wget https://downloads.apache.org/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz
tar -xzf hadoop-3.2.1.tar.gz -C /opt && ln -s /opt/hadoop-3.2.1 /opt/hadoop

# Spark 3.5.1
wget https://downloads.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz -C /opt && ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark

# Airflow
pip install apache-airflow==2.9.2
airflow db migrate
airflow users create --username admin --role Admin --email a@b.com --password admin

# Pipeline deps
pip install -r requirements.txt
```

### Start services

```bash
# Hadoop
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# Spark History Server (optional)
$SPARK_HOME/sbin/start-history-server.sh

# Airflow
export AIRFLOW_HOME=/opt/airflow
airflow webserver -p 8080 &
airflow scheduler &
```

### Set Airflow Variables

```bash
airflow variables set SPARK_HOME    /opt/spark
airflow variables set PIPELINE_HOME /opt/medallion_pipeline
airflow variables set HIVE_SERVER2  localhost:10000
```

---

## 🔄 How Incremental Load Works

1. `collections_metadata.last_loaded_timestamp` stores the last successful watermark.
2. Spark extracts only documents where `updated_at > last_loaded_timestamp AND updated_at <= NOW()`.
3. After a successful Bronze write, the watermark is advanced to `NOW()`.
4. On the next run, only the delta is fetched — zero re-processing of already-loaded data.

**Example timeline:**

```
Day 1 run:  watermark=1970-01-01 → extract all → write bronze → watermark=2024-06-01 02:05
Day 2 run:  watermark=2024-06-01 02:05 → extract new/changed → write bronze → watermark=2024-06-02 02:05
```

---

## 🗂️ SCD Type 2 Walkthrough

**Scenario:** Customer `CUST-001` changes their email on 2024-06-10.

| Version | email | effective_date | expiry_date | is_current |
|---|---|---|---|---|
| 1 | old@mail.com | 2024-01-01 | 2024-06-09 | false |
| 2 | new@mail.com | 2024-06-10 | 9999-12-31 | **true** |

**What Spark does:**
1. Computes MD5 hash of the incoming record → hash differs from stored hash.
2. Sets `expiry_date = NOW()` and `is_current = False` on version 1.
3. Inserts version 2 with `effective_date = NOW()`, `expiry_date = 9999-12-31`, `is_current = True`.

---

## ➕ Onboarding a New Collection

1. Insert a row into `collections_metadata`:

```sql
INSERT INTO collections_metadata
  (collection_name, watermark_column, scd_type, primary_key_column,
   bronze_path, silver_path, gold_path)
VALUES
  ('invoices', 'updated_at', 1, 'invoice_id',
   '/data-lake/bronze/invoices', '/data-lake/silver/invoices', NULL);
```

2. Create HDFS directories:

```bash
hdfs dfs -mkdir -p /data-lake/bronze/invoices /data-lake/silver/invoices
```

3. Add Hive table definitions to `sql/hive/hive_tables.sql` and run `MSCK REPAIR`.

4. On the next Airflow run, the DAG automatically detects `invoices` and adds a Bronze + Silver task group. **No code changes required.**

---

## 📊 Monitoring

**Pipeline health:**

```sql
SELECT collection_name, layer, status, records_loaded,
       pipeline_start_time, duration_seconds
FROM   pipeline_audit
ORDER  BY pipeline_start_time DESC
LIMIT  50;
```

**Current watermarks:**

```sql
SELECT collection_name, last_loaded_timestamp, active_flag
FROM   collections_metadata
ORDER  BY collection_name;
```

---

## 🛠️ Troubleshooting

| Symptom | Fix |
|---|---|
| `MongoServerSelectionError` | Check MongoDB Atlas IP whitelist; confirm URI in `pipeline_config.yaml` |
| `hdfs: call from namenode failed` | Run `start-dfs.sh`; verify `HADOOP_HOME` env var |
| Spark job OOM | Increase `executor_memory` in config; tune `shuffle_partitions` |
| Airflow task stuck | Check scheduler heartbeat; confirm Postgres connection |
| Hive table empty | Run `MSCK REPAIR TABLE`; verify HDFS path has Parquet files |

---

## 🏷️ License

MIT – free for personal and commercial use.
