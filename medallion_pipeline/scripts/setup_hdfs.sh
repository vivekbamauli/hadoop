#!/usr/bin/env bash
# =============================================================
# scripts/setup_hdfs.sh
# Create Medallion Architecture folder structure on HDFS
# =============================================================
set -euo pipefail

HDFS_CMD="${HADOOP_HOME:-/opt/hadoop}/bin/hdfs"
NAMENODE="${NAMENODE_HOST:-namenode:9000}"
BASE="/data-lake"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Medallion Pipeline – HDFS Initialisation"
echo " NameNode: hdfs://${NAMENODE}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── Helper ────────────────────────────────────────────────────
hdfs_mkdir() {
    local path="$1"
    echo "  Creating: ${path}"
    $HDFS_CMD dfs -mkdir -p "hdfs://${NAMENODE}${path}"
    $HDFS_CMD dfs -chmod 755  "hdfs://${NAMENODE}${path}"
}

# ── Base ──────────────────────────────────────────────────────
hdfs_mkdir "${BASE}"
hdfs_mkdir "${BASE}/bronze"
hdfs_mkdir "${BASE}/silver"
hdfs_mkdir "${BASE}/gold"

# ── Bronze ────────────────────────────────────────────────────
for coll in customers orders products payments reviews; do
    hdfs_mkdir "${BASE}/bronze/${coll}"
done

# ── Silver ────────────────────────────────────────────────────
for coll in customers orders products payments reviews; do
    hdfs_mkdir "${BASE}/silver/${coll}"
done

# ── Gold ──────────────────────────────────────────────────────
for table in customer_sales_summary top_products order_analytics; do
    hdfs_mkdir "${BASE}/gold/${table}"
done

# ── Logs / Checkpoints ────────────────────────────────────────
hdfs_mkdir "${BASE}/_logs"
hdfs_mkdir "${BASE}/_checkpoints"
hdfs_mkdir "${BASE}/_staging"

# ── Hive warehouse ────────────────────────────────────────────
$HDFS_CMD dfs -mkdir -p hdfs://${NAMENODE}/user/hive/warehouse
$HDFS_CMD dfs -chmod -R 777 hdfs://${NAMENODE}/user/hive/warehouse
$HDFS_CMD dfs -mkdir -p hdfs://${NAMENODE}/user/hive/warehouse/medallion.db

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " HDFS directory listing:"
$HDFS_CMD dfs -ls -R "hdfs://${NAMENODE}${BASE}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " ✅ HDFS initialisation complete"
