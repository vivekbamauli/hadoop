"""
tests/test_silver_scd.py
─────────────────────────
Unit tests for SCD Type 1 and Type 2 merge logic.
Run: pytest tests/ -v
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, IntegerType


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("MedallionTests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("email",       StringType(), True),
    StructField("status",      StringType(), True),
    StructField("updated_at",  TimestampType(), True),
])


def make_df(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


class TestDeduplication:
    def test_keeps_latest_per_pk(self, spark):
        from spark.jobs.silver_processing import deduplicate

        rows = [
            ("C001", "a@b.com", "active",   datetime(2024, 1, 1, 10, 0)),
            ("C001", "a@b.com", "inactive", datetime(2024, 1, 2, 10, 0)),
            ("C002", "x@y.com", "active",   datetime(2024, 1, 1, 10, 0)),
        ]
        df     = make_df(spark, rows)
        result = deduplicate(df, "customer_id", "updated_at")

        assert result.count() == 2
        c001 = result.filter(F.col("customer_id") == "C001").first()
        assert c001["status"] == "inactive"


class TestRecordHash:
    def test_same_data_same_hash(self, spark):
        from spark.jobs.silver_processing import add_record_hash

        rows = [("C001", "a@b.com", "active", datetime(2024, 1, 1))]
        df1  = add_record_hash(make_df(spark, rows))
        df2  = add_record_hash(make_df(spark, rows))

        h1 = df1.first()["record_hash"]
        h2 = df2.first()["record_hash"]
        assert h1 == h2

    def test_different_data_different_hash(self, spark):
        from spark.jobs.silver_processing import add_record_hash

        r1 = make_df(spark, [("C001", "a@b.com", "active",   datetime(2024, 1, 1))])
        r2 = make_df(spark, [("C001", "a@b.com", "inactive", datetime(2024, 1, 1))])

        h1 = add_record_hash(r1).first()["record_hash"]
        h2 = add_record_hash(r2).first()["record_hash"]
        assert h1 != h2


class TestCleansing:
    def test_trim_and_lowercase_email(self, spark):
        from spark.jobs.silver_processing import cleanse

        rows = [("C001", "  A@B.COM  ", "ACTIVE", datetime(2024, 1, 1))]
        df   = make_df(spark, rows)
        out  = cleanse(df, "customers")

        row = out.first()
        assert row["email"] == "a@b.com"

    def test_null_required_field_dropped(self, spark):
        from spark.jobs.silver_processing import cleanse

        rows = [
            ("C001", None,      "active", datetime(2024, 1, 1)),
            ("C002", "x@y.com", "active", datetime(2024, 1, 1)),
        ]
        df  = make_df(spark, rows)
        out = cleanse(df, "customers")
        assert out.count() == 1


class TestCommonUtils:
    def test_compute_record_hash_excludes_scd_cols(self):
        from spark.utils.common import compute_record_hash

        row = {
            "customer_id":  "C001",
            "email":        "a@b.com",
            "effective_date": "2024-01-01",
            "is_current":   True,
        }
        h1 = compute_record_hash(row)

        row_modified_scd = dict(row)
        row_modified_scd["is_current"] = False
        h2 = compute_record_hash(row_modified_scd)

        # SCD columns excluded → hashes must match
        assert h1 == h2

    def test_compute_record_hash_sensitive_to_data_change(self):
        from spark.utils.common import compute_record_hash

        r1 = {"customer_id": "C001", "email": "a@b.com"}
        r2 = {"customer_id": "C001", "email": "x@y.com"}
        assert compute_record_hash(r1) != compute_record_hash(r2)
