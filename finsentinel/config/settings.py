import os
from dotenv import load_dotenv

load_dotenv()


def get_config():
    """从环境变量读取配置，无硬编码路径"""
    return {
        "kafka": {
            "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "topic": os.getenv("KAFKA_TOPIC", "txns"),
            "num_partitions": int(os.getenv("KAFKA_NUM_PARTITIONS", "3")),
        },
        "mongo": {
            "uri": os.getenv("MONGO_URI", "mongodb://localhost:27017"),
            "database": os.getenv("MONGO_DATABASE", "fraud_detection"),
            "collection": os.getenv("MONGO_COLLECTION", "fraud_alerts"),
        },
        "delta": {
            "raw_path": os.getenv("DELTA_RAW_PATH", "./data/raw"),
            "clean_path": os.getenv("DELTA_CLEAN_PATH", "./data/clean"),
        },
        "generator": {
            "batch_size": int(os.getenv("GENERATOR_BATCH_SIZE", "1")),
            "interval_seconds": float(os.getenv("GENERATOR_INTERVAL", "1")),
        },
    }
