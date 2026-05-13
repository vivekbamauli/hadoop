-- =============================================================
-- Medallion Pipeline – PostgreSQL Metadata & Audit Schema
-- =============================================================

-- ─── Database & User Setup ────────────────────────────────────
CREATE DATABASE pipeline_metadata;
CREATE USER pipeline_user WITH ENCRYPTED PASSWORD 'pipeline_pass';
GRANT ALL PRIVILEGES ON DATABASE pipeline_metadata TO pipeline_user;

\c pipeline_metadata

-- ─── Extensions ───────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- =============================================================
-- 1. COLLECTIONS METADATA
-- =============================================================
CREATE TABLE IF NOT EXISTS collections_metadata (
    id                      SERIAL PRIMARY KEY,
    collection_name         VARCHAR(100)  NOT NULL UNIQUE,
    source_database         VARCHAR(100)  NOT NULL DEFAULT 'enterprise_db',
    watermark_column        VARCHAR(100)  NOT NULL DEFAULT 'updated_at',
    last_loaded_timestamp   TIMESTAMP     NOT NULL DEFAULT '1970-01-01 00:00:00',
    scd_type                SMALLINT      NOT NULL DEFAULT 1 CHECK (scd_type IN (1, 2)),
    primary_key_column      VARCHAR(100)  NOT NULL,
    active_flag             BOOLEAN       NOT NULL DEFAULT TRUE,
    bronze_path             VARCHAR(500),
    silver_path             VARCHAR(500),
    gold_path               VARCHAR(500),
    partition_column        VARCHAR(100)  DEFAULT 'load_date',
    schema_version          VARCHAR(20)   DEFAULT '1.0',
    created_at              TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE collections_metadata IS
  'Metadata registry driving automated incremental ingestion per collection.';

-- ─── Seed Metadata ────────────────────────────────────────────
INSERT INTO collections_metadata
    (collection_name, watermark_column, scd_type, primary_key_column,
     bronze_path, silver_path, gold_path)
VALUES
    ('customers', 'updated_at', 2, 'customer_id',
     '/data-lake/bronze/customers', '/data-lake/silver/customers', '/data-lake/gold/customer_sales_summary'),
    ('orders',    'updated_at', 1, 'order_id',
     '/data-lake/bronze/orders',   '/data-lake/silver/orders',   '/data-lake/gold/order_analytics'),
    ('products',  'updated_at', 2, 'product_id',
     '/data-lake/bronze/products', '/data-lake/silver/products', '/data-lake/gold/top_products'),
    ('payments',  'created_at', 1, 'payment_id',
     '/data-lake/bronze/payments', '/data-lake/silver/payments', NULL),
    ('reviews',   'created_at', 1, 'review_id',
     '/data-lake/bronze/reviews',  '/data-lake/silver/reviews',  NULL)
ON CONFLICT (collection_name) DO NOTHING;

-- =============================================================
-- 2. PIPELINE AUDIT
-- =============================================================
CREATE TABLE IF NOT EXISTS pipeline_audit (
    id                   BIGSERIAL PRIMARY KEY,
    run_id               UUID          NOT NULL DEFAULT uuid_generate_v4(),
    dag_id               VARCHAR(200),
    task_id              VARCHAR(200),
    collection_name      VARCHAR(100)  NOT NULL,
    layer                VARCHAR(20)   CHECK (layer IN ('bronze', 'silver', 'gold')),
    pipeline_start_time  TIMESTAMP     NOT NULL,
    pipeline_end_time    TIMESTAMP,
    duration_seconds     NUMERIC(10,2),
    records_extracted    BIGINT        DEFAULT 0,
    records_loaded       BIGINT        DEFAULT 0,
    records_failed       BIGINT        DEFAULT 0,
    records_skipped      BIGINT        DEFAULT 0,
    watermark_start      TIMESTAMP,
    watermark_end        TIMESTAMP,
    status               VARCHAR(20)   NOT NULL DEFAULT 'RUNNING'
                             CHECK (status IN ('RUNNING','SUCCESS','FAILED','PARTIAL')),
    error_message        TEXT,
    spark_job_id         VARCHAR(200),
    hdfs_output_path     VARCHAR(500),
    created_at           TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_collection   ON pipeline_audit (collection_name);
CREATE INDEX idx_audit_run_id       ON pipeline_audit (run_id);
CREATE INDEX idx_audit_status       ON pipeline_audit (status);
CREATE INDEX idx_audit_start_time   ON pipeline_audit (pipeline_start_time);

COMMENT ON TABLE pipeline_audit IS
  'End-to-end audit log for every pipeline execution per collection per layer.';

-- =============================================================
-- 3. SCHEMA EVOLUTION LOG
-- =============================================================
CREATE TABLE IF NOT EXISTS schema_evolution_log (
    id                SERIAL PRIMARY KEY,
    collection_name   VARCHAR(100) NOT NULL,
    detected_at       TIMESTAMP    NOT NULL DEFAULT NOW(),
    old_schema        JSONB,
    new_schema        JSONB,
    change_type       VARCHAR(50),   -- ADD_COLUMN | DROP_COLUMN | TYPE_CHANGE
    column_name       VARCHAR(100),
    resolved          BOOLEAN      DEFAULT FALSE,
    resolved_at       TIMESTAMP,
    notes             TEXT
);

-- =============================================================
-- 4. HELPFUL VIEWS
-- =============================================================
CREATE OR REPLACE VIEW v_active_collections AS
SELECT
    collection_name,
    watermark_column,
    last_loaded_timestamp,
    scd_type,
    primary_key_column,
    bronze_path,
    silver_path,
    gold_path
FROM collections_metadata
WHERE active_flag = TRUE
ORDER BY collection_name;

CREATE OR REPLACE VIEW v_pipeline_summary AS
SELECT
    collection_name,
    layer,
    COUNT(*)                                              AS total_runs,
    SUM(records_extracted)                                AS total_extracted,
    SUM(records_loaded)                                   AS total_loaded,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)  AS successful_runs,
    SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END)  AS failed_runs,
    MAX(pipeline_start_time)                              AS last_run
FROM pipeline_audit
GROUP BY collection_name, layer
ORDER BY collection_name, layer;

-- =============================================================
-- 5. STORED PROCEDURES
-- =============================================================

-- Update watermark after successful load
CREATE OR REPLACE FUNCTION update_watermark(
    p_collection  VARCHAR,
    p_watermark   TIMESTAMP
) RETURNS VOID AS $$
BEGIN
    UPDATE collections_metadata
    SET    last_loaded_timestamp = p_watermark,
           updated_at            = NOW()
    WHERE  collection_name = p_collection
      AND  active_flag     = TRUE;
END;
$$ LANGUAGE plpgsql;

-- Insert audit record
CREATE OR REPLACE FUNCTION insert_audit(
    p_run_id        UUID,
    p_dag_id        VARCHAR,
    p_task_id       VARCHAR,
    p_collection    VARCHAR,
    p_layer         VARCHAR,
    p_start_time    TIMESTAMP,
    p_end_time      TIMESTAMP,
    p_extracted     BIGINT,
    p_loaded        BIGINT,
    p_failed        BIGINT,
    p_wm_start      TIMESTAMP,
    p_wm_end        TIMESTAMP,
    p_status        VARCHAR,
    p_error_msg     TEXT,
    p_hdfs_path     VARCHAR
) RETURNS BIGINT AS $$
DECLARE
    v_id BIGINT;
BEGIN
    INSERT INTO pipeline_audit (
        run_id, dag_id, task_id, collection_name, layer,
        pipeline_start_time, pipeline_end_time,
        duration_seconds, records_extracted, records_loaded, records_failed,
        watermark_start, watermark_end,
        status, error_message, hdfs_output_path
    ) VALUES (
        p_run_id, p_dag_id, p_task_id, p_collection, p_layer,
        p_start_time, p_end_time,
        EXTRACT(EPOCH FROM (p_end_time - p_start_time)),
        p_extracted, p_loaded, p_failed,
        p_wm_start, p_wm_end,
        p_status, p_error_msg, p_hdfs_path
    ) RETURNING id INTO v_id;

    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

GRANT ALL ON ALL TABLES    IN SCHEMA public TO pipeline_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO pipeline_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO pipeline_user;
