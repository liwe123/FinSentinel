import pytest
from pyspark.sql import SparkSession
from src.schemas import transaction_schema, raw_schema, clean_schema, alert_schema


@pytest.fixture(scope="session")
def spark():
    """创建测试用 Spark Session"""
    return (
        SparkSession.builder.appName("TestSchemas")
        .master("local[*]")
        .getOrCreate()
    )


def test_transaction_schema_fields(spark):
    """验证交易 Schema 字段完整性"""
    expected_fields = {
        "transaction_id", "user_id", "merchant_id", "amount",
        "currency", "timestamp", "location", "ip_address",
        "device_id", "transaction_type"
    }
    actual_fields = {f.name for f in transaction_schema.fields}
    assert expected_fields == actual_fields


def test_raw_schema_fields(spark):
    """验证 Raw 表 Schema 字段完整性"""
    expected_fields = {
        "transaction_id", "user_id", "merchant_id", "amount",
        "timestamp", "latitude", "longitude", "ip_address",
        "device_id", "transaction_type", "kafka_partition",
        "kafka_offset", "ingest_time"
    }
    actual_fields = {f.name for f in raw_schema.fields}
    assert expected_fields == actual_fields


def test_clean_schema_fields(spark):
    """验证 Clean 表 Schema 字段完整性（脱敏后）"""
    expected_fields = {
        "transaction_id", "user_hash", "merchant_id", "amount",
        "timestamp", "latitude", "longitude", "ip_mask",
        "transaction_type", "kafka_partition", "kafka_offset",
        "ingest_time"
    }
    actual_fields = {f.name for f in clean_schema.fields}
    assert expected_fields == actual_fields


def test_transaction_schema_nullable(spark):
    """验证必填字段不可为空"""
    non_nullable = {"transaction_id", "user_id", "merchant_id", "amount", "currency", "timestamp"}
    for field in transaction_schema.fields:
        if field.name in non_nullable:
            assert not field.nullable, f"{field.name} should not be nullable"


def test_schema_can_create_df(spark):
    """验证 Schema 可以创建空 DataFrame"""
    raw_df = spark.createDataFrame([], raw_schema)
    assert raw_df.count() == 0
    assert len(raw_df.columns) == len(raw_schema.fields)

    clean_df = spark.createDataFrame([], clean_schema)
    assert clean_df.count() == 0
    assert len(clean_df.columns) == len(clean_schema.fields)


def test_alert_schema_fields(spark):
    """验证告警 Schema 字段完整性"""
    expected_fields = {
        "alert_id", "rule_type", "transaction_id", "user_hash",
        "amount", "timestamp", "trigger_detail", "alert_time"
    }
    actual_fields = {f.name for f in alert_schema.fields}
    assert expected_fields == actual_fields


def test_alert_schema_can_create_df(spark):
    """验证告警 Schema 可以创建空 DataFrame"""
    alert_df = spark.createDataFrame([], alert_schema)
    assert alert_df.count() == 0
    assert len(alert_df.columns) == len(alert_schema.fields)
