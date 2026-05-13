from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType, TimestampType
)

transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("city", StringType(), True),
    ]), True),
    StructField("ip_address", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
])

raw_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("ip_address", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("ingest_time", TimestampType(), True),
])

clean_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_hash", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("ip_mask", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("ingest_time", TimestampType(), True),
])

alert_schema = StructType([
    StructField("alert_id", StringType(), False),
    StructField("rule_type", StringType(), False),
    StructField("transaction_id", StringType(), False),
    StructField("user_hash", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("trigger_detail", StringType(), True),
    StructField("alert_time", TimestampType(), False),
])
