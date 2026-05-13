import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def process_raw_table(df: DataFrame) -> DataFrame:
    result = df.select(
        F.col("transaction_id"),
        F.col("user_id"),
        F.col("merchant_id"),
        F.col("amount"),
        F.col("timestamp"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("ip_address"),
        F.col("device_id"),
        F.col("transaction_type"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.current_timestamp().alias("ingest_time"),
    )
    return result


def process_clean_table(df: DataFrame, salt: str = "FinSentinel2024") -> DataFrame:
    result = df.select(
        F.col("transaction_id"),
        F.sha2(F.concat(F.col("user_id"), F.lit(salt)), 256).alias("user_hash"),
        F.col("merchant_id"),
        F.col("amount"),
        F.col("timestamp"),
        F.col("latitude"),
        F.col("longitude"),
        F.concat(
            F.split(F.col("ip_address"), "\\.").getItem(0),
            F.lit("."),
            F.split(F.col("ip_address"), "\\.").getItem(1),
            F.lit(".*.*"),
        ).alias("ip_mask"),
        F.col("transaction_type"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("ingest_time"),
    )
    return result


def write_delta(raw_df: DataFrame, clean_df: DataFrame, delta_config: dict):
    raw_query = (
        raw_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(delta_config["raw_path"], "_checkpoint"))
        .partitionBy("kafka_partition")
        .start(delta_config["raw_path"])
    )

    clean_query = (
        clean_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(delta_config["clean_path"], "_checkpoint"))
        .partitionBy("kafka_partition")
        .start(delta_config["clean_path"])
    )

    return raw_query, clean_query
