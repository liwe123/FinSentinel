import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import get_config
from src.delta_writer import process_clean_table, process_raw_table, write_delta
from src.fraud_rules import apply_all_rules
from src.mongo_writer import MongoAlertWriter
from src.schemas import transaction_schema

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("spark_streaming")


def create_spark_session():
    return (
        SparkSession.builder.appName("FinSentinel-FraudDetection")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main_streaming_pipeline():
    config = get_config()
    kafka_config = config["kafka"]
    delta_config = config["delta"]

    spark = create_spark_session()

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
        .option("subscribe", kafka_config["topic"])
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        raw_stream.selectExpr("CAST(value AS STRING) as json_str", "partition", "offset")
        .select(
            F.from_json(F.col("json_str"), transaction_schema).alias("data"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .select("data.*", "kafka_partition", "kafka_offset")
        .withColumn("latitude", F.col("location.latitude"))
        .withColumn("longitude", F.col("location.longitude"))
        .withColumn("city", F.col("location.city"))
        .drop("location")
    )

    parsed_stream = (
        parsed_stream
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withWatermark("timestamp", "10 minutes")
    )

    raw_df = process_raw_table(parsed_stream)

    clean_df = process_clean_table(raw_df)

    raw_query, clean_query = write_delta(raw_df, clean_df, delta_config)

    alert_df = apply_all_rules(raw_df)

    mongo_writer = MongoAlertWriter(
        config["mongo"]["uri"],
        config["mongo"]["database"],
        config["mongo"]["collection"],
    )

    checkpoint_dir = os.path.join(
        os.path.dirname(os.path.normpath(delta_config["raw_path"])),
        "_checkpoints",
        "alerts"
    )
    alert_query = (
        alert_df.writeStream.foreachBatch(mongo_writer.write_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="5 seconds")
        .start()
    )

    logger.info("Streaming pipeline started. Waiting for data...")
    logger.info(f"  Raw Delta: {delta_config['raw_path']}")
    logger.info(f"  Clean Delta: {delta_config['clean_path']}")
    logger.info(f"  MongoDB: {config['mongo']['uri']}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main_streaming_pipeline()
