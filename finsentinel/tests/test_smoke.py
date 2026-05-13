from datetime import datetime, timedelta
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.appName("SmokeTest")
        .master("local[2]")
        .getOrCreate()
    )


# ===================== 1. Configuration Loading =====================

def test_config_loaded():
    from config.settings import get_config
    cfg = get_config()
    assert "kafka" in cfg
    assert "mongo" in cfg
    assert "delta" in cfg
    assert "generator" in cfg
    assert cfg["kafka"]["topic"] == "txns"
    assert cfg["mongo"]["database"] == "fraud_detection"


def test_fraud_rules_config_loaded():
    from src.fraud_rules import load_rules_config
    rules = load_rules_config()
    for r in ["R1", "R2", "R3", "R4"]:
        assert r in rules
        assert rules[r]["enabled"] is True
        assert "name" in rules[r]


# ===================== 2. Schema Validation =====================

def test_all_schemas_defined():
    from src.schemas import transaction_schema, raw_schema, clean_schema, alert_schema
    assert transaction_schema is not None
    assert raw_schema is not None
    assert clean_schema is not None
    assert alert_schema is not None


def test_raw_schema_has_ingest_metadata(spark):
    from src.schemas import raw_schema
    fields = {f.name for f in raw_schema.fields}
    assert "kafka_partition" in fields
    assert "kafka_offset" in fields
    assert "ingest_time" in fields


def test_clean_schema_has_pii_fields(spark):
    from src.schemas import clean_schema
    fields = {f.name for f in clean_schema.fields}
    assert "user_hash" in fields
    assert "ip_mask" in fields
    assert "device_id" not in fields


# ===================== 3. Transaction Generator =====================

def test_generate_normal_tx_structure():
    from src.transaction_generator import generate_normal_tx
    tx = generate_normal_tx()
    required = ["transaction_id", "user_id", "merchant_id", "amount",
                "currency", "timestamp", "location", "ip_address",
                "device_id", "transaction_type"]
    for field in required:
        assert field in tx
    assert tx["currency"] == "USD"
    assert isinstance(tx["amount"], float)
    assert "TXN-" in tx["transaction_id"]


def test_inject_fraud_r1():
    from src.transaction_generator import inject_fraud_tx
    tx = inject_fraud_tx("R1")
    assert tx["user_id"] == "FRAUD_USER_R1"
    assert tx["amount"] == 100.0


def test_inject_fraud_r2():
    from src.transaction_generator import inject_fraud_tx
    tx = inject_fraud_tx("R2")
    assert tx["user_id"] == "FRAUD_USER_R2"
    assert tx["location"]["city"] == "Tokyo"


def test_inject_fraud_r3():
    from src.transaction_generator import inject_fraud_tx
    tx = inject_fraud_tx("R3")
    assert tx["amount"] >= 6000
    ts = datetime.fromisoformat(tx["timestamp"].replace("Z", "+00:00"))
    assert 0 <= ts.hour <= 4


def test_inject_fraud_r4():
    from src.transaction_generator import inject_fraud_tx
    tx = inject_fraud_tx("R4")
    assert tx["merchant_id"] == "FRAUD_MERCHANT_R4"
    assert tx["amount"] == 50.0


# ===================== 4. Fraud Rules (Full Integration) =====================

def test_apply_all_rules_integration(spark):
    from src.fraud_rules import apply_all_rules

    base_time = datetime(2024, 1, 15, 3, 0, 0)
    data = [
        {"transaction_id": f"TXN-{i}", "user_id": "U1001", "merchant_id": "M3001",
         "amount": 6000.0 + i, "timestamp": base_time + timedelta(minutes=i),
         "latitude": 40.7128, "longitude": -74.0060,
         "ip_address": "192.168.1.1", "device_id": "DEV-001",
         "transaction_type": "purchase"}
        for i in range(6)
    ]
    df = spark.createDataFrame(data)
    result = apply_all_rules(df)
    assert result.count() > 0


def test_apply_all_rules_returns_rule_types(spark):
    from src.fraud_rules import apply_all_rules

    base_time = datetime(2024, 1, 15, 3, 0, 0)
    data = [
        {"transaction_id": f"TXN-{i}", "user_id": "U1001", "merchant_id": "M3001",
         "amount": 6000.0 + i, "timestamp": base_time + timedelta(minutes=i),
         "latitude": 40.7128, "longitude": -74.0060,
         "ip_address": "192.168.1.1", "device_id": "DEV-001",
         "transaction_type": "purchase"}
        for i in range(6)
    ]
    df = spark.createDataFrame(data)
    result = apply_all_rules(df)
    rule_types = [r.rule_type for r in result.select("rule_type").distinct().collect()]
    assert "NIGHT_LARGE" in rule_types
    assert "VELOCITY_FRAUD" in rule_types


# ===================== 5. PII Masking (Delta Writer) =====================

def test_pii_user_hash(spark):
    from src.delta_writer import process_clean_table

    data = [{"transaction_id": "TXN-001", "user_id": "U1001", "merchant_id": "M3001",
             "amount": 100.0, "timestamp": datetime(2024, 1, 15, 12, 0, 0),
             "latitude": 40.7128, "longitude": -74.0060,
             "ip_address": "192.168.1.100", "device_id": "DEV-001",
             "transaction_type": "purchase", "kafka_partition": 0,
             "kafka_offset": 1, "ingest_time": datetime(2024, 1, 15, 12, 0, 0)}]
    df = spark.createDataFrame(data)
    result = process_clean_table(df).collect()

    row = result[0]
    assert row.user_hash is not None
    assert row.user_hash != "U1001"
    assert len(row.user_hash) == 64


def test_pii_ip_mask(spark):
    from src.delta_writer import process_clean_table

    data = [{"transaction_id": "TXN-001", "user_id": "U1001", "merchant_id": "M3001",
             "amount": 100.0, "timestamp": datetime(2024, 1, 15, 12, 0, 0),
             "latitude": 40.7128, "longitude": -74.0060,
             "ip_address": "10.20.30.40", "device_id": "DEV-001",
             "transaction_type": "purchase", "kafka_partition": 0,
             "kafka_offset": 1, "ingest_time": datetime(2024, 1, 15, 12, 0, 0)}]
    df = spark.createDataFrame(data)
    result = process_clean_table(df).collect()

    row = result[0]
    assert row.ip_mask == "10.20.*.*"


def test_pii_device_id_removed(spark):
    from src.delta_writer import process_clean_table
    from src.schemas import clean_schema

    fields = {f.name for f in clean_schema.fields}
    assert "device_id" not in fields

    data = [{"transaction_id": "TXN-001", "user_id": "U1001", "merchant_id": "M3001",
             "amount": 100.0, "timestamp": datetime(2024, 1, 15, 12, 0, 0),
             "latitude": 40.7128, "longitude": -74.0060,
             "ip_address": "192.168.1.1", "device_id": "DEV-SECRET",
             "transaction_type": "purchase", "kafka_partition": 0,
             "kafka_offset": 1, "ingest_time": datetime(2024, 1, 15, 12, 0, 0)}]
    df = spark.createDataFrame(data)
    result = process_clean_table(df)
    assert "device_id" not in result.columns


# ===================== 6. Data Integrity =====================

def test_raw_table_retains_original_data(spark):
    from src.delta_writer import process_raw_table

    data = [{"transaction_id": "TXN-001", "user_id": "U1001", "merchant_id": "M3001",
             "amount": 100.0, "timestamp": datetime(2024, 1, 15, 12, 0, 0),
             "latitude": 40.7128, "longitude": -74.0060,
             "ip_address": "192.168.1.100", "device_id": "DEV-001",
             "transaction_type": "purchase", "kafka_partition": 0,
             "kafka_offset": 1}]
    df = spark.createDataFrame(data)
    result = process_raw_table(df).collect()

    row = result[0]
    assert row.user_id == "U1001"
    assert row.ip_address == "192.168.1.100"
    assert row.device_id == "DEV-001"
    assert row.ingest_time is not None


# ===================== 7. Alert Schema Integration =====================

def test_alert_schema_fits_rule_output(spark):
    from src.fraud_rules import apply_all_rules

    base_time = datetime(2024, 1, 15, 3, 0, 0)
    data = [
        {"transaction_id": "TXN-001", "user_id": "U1001", "merchant_id": "M3001",
         "amount": 7000.0, "timestamp": base_time,
         "latitude": 40.7128, "longitude": -74.0060,
         "ip_address": "192.168.1.1", "device_id": "DEV-001",
         "transaction_type": "purchase"}
    ]
    df = spark.createDataFrame(data)
    alerts = apply_all_rules(df)
    assert alerts.count() >= 1
    result = alerts.collect()[0]
    assert result.rule_type == "NIGHT_LARGE"
    assert result.user_hash is not None
    assert result.amount == 7000.0
