import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """创建测试用 Spark Session"""
    return (
        SparkSession.builder.appName("TestFraudRules")
        .master("local[*]")
        .getOrCreate()
    )


def test_check_night_large_hit(spark):
    """R3: 深夜大额 - 应命中"""
    from src.fraud_rules import check_night_large

    data = [
        {
            "transaction_id": "TXN-001",
            "user_id": "U1001",
            "merchant_id": "M3001",
            "amount": 6000.0,
            "timestamp": datetime(2024, 1, 15, 3, 0, 0),
            "latitude": 40.7128,
            "longitude": -74.0060,
            "ip_address": "192.168.1.1",
            "device_id": "DEV-001",
            "transaction_type": "purchase",
        }
    ]

    df = spark.createDataFrame(data)
    result = check_night_large(df)
    assert result.count() == 1


def test_check_night_large_miss(spark):
    """R3: 下午大额 - 不应命中"""
    from src.fraud_rules import check_night_large

    data = [
        {
            "transaction_id": "TXN-002",
            "user_id": "U1001",
            "merchant_id": "M3001",
            "amount": 6000.0,
            "timestamp": datetime(2024, 1, 15, 14, 0, 0),
            "latitude": 40.7128,
            "longitude": -74.0060,
            "ip_address": "192.168.1.1",
            "device_id": "DEV-001",
            "transaction_type": "purchase",
        }
    ]

    df = spark.createDataFrame(data)
    result = check_night_large(df)
    assert result.count() == 0


def test_check_night_large_small_amount(spark):
    """R3: 深夜小额 - 不应命中"""
    from src.fraud_rules import check_night_large

    data = [
        {
            "transaction_id": "TXN-003",
            "user_id": "U1001",
            "merchant_id": "M3001",
            "amount": 100.0,
            "timestamp": datetime(2024, 1, 15, 3, 0, 0),
            "latitude": 40.7128,
            "longitude": -74.0060,
            "ip_address": "192.168.1.1",
            "device_id": "DEV-001",
            "transaction_type": "purchase",
        }
    ]

    df = spark.createDataFrame(data)
    result = check_night_large(df)
    assert result.count() == 0


def test_check_velocity_hit(spark):
    """R1: 速度欺诈 - 5分钟内5笔交易应命中"""
    from src.fraud_rules import check_velocity

    base_time = datetime(2024, 1, 15, 12, 0, 0)
    data = [
        {
            "transaction_id": f"TXN-V{i}",
            "user_id": "U1001",
            "merchant_id": "M3001",
            "amount": 100.0,
            "timestamp": base_time + timedelta(minutes=i),
            "latitude": 40.7128,
            "longitude": -74.0060,
            "ip_address": "192.168.1.1",
            "device_id": "DEV-001",
            "transaction_type": "purchase",
        }
        for i in range(6)
    ]

    df = spark.createDataFrame(data)
    result = check_velocity(df, window_minutes=5, threshold=5)
    assert result.count() > 0


def test_check_velocity_miss(spark):
    """R1: 正常用户 - 5分钟内2笔交易不应命中"""
    from src.fraud_rules import check_velocity

    base_time = datetime(2024, 1, 15, 12, 0, 0)
    data = [
        {
            "transaction_id": f"TXN-V{i}",
            "user_id": "U1001",
            "merchant_id": "M3001",
            "amount": 100.0,
            "timestamp": base_time + timedelta(minutes=i * 3),
            "latitude": 40.7128,
            "longitude": -74.0060,
            "ip_address": "192.168.1.1",
            "device_id": "DEV-001",
            "transaction_type": "purchase",
        }
        for i in range(2)
    ]

    df = spark.createDataFrame(data)
    result = check_velocity(df, window_minutes=5, threshold=5)
    assert result.count() == 0


def test_check_geo_jump_hit(spark):
    """R2: 地理跳跃 - 10分钟内 >1000km 应命中"""
    from src.fraud_rules import check_geo_jump

    base_time = datetime(2024, 1, 15, 12, 0, 0)
    data = [
        {
            "transaction_id": "TXN-G1",
            "user_id": "U1001",
            "merchant_id": "M3001",
            "amount": 200.0,
            "timestamp": base_time,
            "latitude": 40.7128,
            "longitude": -74.0060,
            "ip_address": "192.168.1.1",
            "device_id": "DEV-001",
            "transaction_type": "purchase",
        },
        {
            "transaction_id": "TXN-G2",
            "user_id": "U1001",
            "merchant_id": "M3002",
            "amount": 300.0,
            "timestamp": base_time + timedelta(minutes=5),
            "latitude": 35.6762,
            "longitude": 139.6503,
            "ip_address": "10.0.0.1",
            "device_id": "DEV-001",
            "transaction_type": "purchase",
        },
    ]

    df = spark.createDataFrame(data)
    result = check_geo_jump(df, max_distance_km=1000, max_interval_minutes=10)
    assert result.count() == 1


def test_check_geo_jump_miss(spark):
    """R2: 同城交易 - 不应命中"""
    from src.fraud_rules import check_geo_jump

    base_time = datetime(2024, 1, 15, 12, 0, 0)
    data = [
        {
            "transaction_id": "TXN-G3",
            "user_id": "U1001",
            "merchant_id": "M3001",
            "amount": 200.0,
            "timestamp": base_time,
            "latitude": 40.7128,
            "longitude": -74.0060,
            "ip_address": "192.168.1.1",
            "device_id": "DEV-001",
            "transaction_type": "purchase",
        },
        {
            "transaction_id": "TXN-G4",
            "user_id": "U1001",
            "merchant_id": "M3002",
            "amount": 300.0,
            "timestamp": base_time + timedelta(minutes=5),
            "latitude": 40.7500,
            "longitude": -73.9800,
            "ip_address": "10.0.0.1",
            "device_id": "DEV-001",
            "transaction_type": "purchase",
        },
    ]

    df = spark.createDataFrame(data)
    result = check_geo_jump(df, max_distance_km=1000, max_interval_minutes=10)
    assert result.count() == 0


def test_check_merchant_velocity_hit(spark):
    """R4: 商户高频 - 1分钟内 >3笔交易应命中"""
    from src.fraud_rules import check_merchant_velocity

    base_time = datetime(2024, 1, 15, 12, 0, 0)
    data = [
        {
            "transaction_id": f"TXN-M{i}",
            "user_id": f"U{1000 + i}",
            "merchant_id": "M9999",
            "amount": 50.0,
            "timestamp": base_time + timedelta(seconds=i * 10),
            "latitude": 40.7128,
            "longitude": -74.0060,
            "ip_address": "192.168.1.1",
            "device_id": f"DEV-00{i}",
            "transaction_type": "purchase",
        }
        for i in range(4)
    ]

    df = spark.createDataFrame(data)
    result = check_merchant_velocity(df, window_minutes=1, threshold=3)
    assert result.count() > 0


def test_check_merchant_velocity_miss(spark):
    """R4: 正常商户 - 1分钟内2笔交易不应命中"""
    from src.fraud_rules import check_merchant_velocity

    base_time = datetime(2024, 1, 15, 12, 0, 0)
    data = [
        {
            "transaction_id": f"TXN-M{i}",
            "user_id": f"U{1000 + i}",
            "merchant_id": "M9998",
            "amount": 50.0,
            "timestamp": base_time + timedelta(seconds=i * 30),
            "latitude": 40.7128,
            "longitude": -74.0060,
            "ip_address": "192.168.1.1",
            "device_id": f"DEV-00{i}",
            "transaction_type": "purchase",
        }
        for i in range(2)
    ]

    df = spark.createDataFrame(data)
    result = check_merchant_velocity(df, window_minutes=1, threshold=3)
    assert result.count() == 0
