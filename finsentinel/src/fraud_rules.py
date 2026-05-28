import yaml
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import logging
import sys
from config.settings import get_config

logger = logging.getLogger("fraud_rules")


def load_rules_config():
    """加载欺诈规则配置"""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config",
        "fraud_rules.yaml",
    )
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["rules"]


def check_velocity(df: DataFrame, window_minutes=5, threshold=5) -> DataFrame:
    """R1: 速度欺诈 - 同一用户窗口内交易次数超阈值"""
    window_spec = (
        Window.partitionBy("user_id")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-window_minutes * 60, 0)
    )

    return df.withColumn("txn_count", F.count("*").over(window_spec)).filter(
        F.col("txn_count") > threshold
    )


def check_geo_jump(df: DataFrame, max_distance_km=1000, max_interval_minutes=10) -> DataFrame:
    """R2: 地理跳跃 - 同一用户短时间内地理位置跳跃"""
    w = Window.partitionBy("user_id").orderBy("timestamp")

    df_with_prev = df.withColumn(
        "prev_lat", F.lag("latitude").over(w)
    ).withColumn(
        "prev_lon", F.lag("longitude").over(w)
    ).withColumn(
        "prev_ts", F.lag("timestamp").over(w)
    )

    return df_with_prev.filter(
        F.col("prev_lat").isNotNull() & F.col("prev_ts").isNotNull()
    ).withColumn(
        "distance_km",
        F.acos(
            F.sin(F.radians(F.col("latitude"))) * F.sin(F.radians(F.col("prev_lat")))
            + F.cos(F.radians(F.col("latitude"))) * F.cos(F.radians(F.col("prev_lat")))
            * F.cos(F.radians(F.col("longitude")) - F.radians(F.col("prev_lon")))
        ) * 6371
    ).withColumn(
        "time_diff_minutes",
        (F.col("timestamp").cast("long") - F.col("prev_ts").cast("long")) / 60
    ).filter(
        (F.col("distance_km") > max_distance_km)
        & (F.col("time_diff_minutes") < max_interval_minutes)
    )


def check_night_large(df: DataFrame, min_amount=5000, start_hour=0, end_hour=5) -> DataFrame:
    """R3: 深夜大额 - 交易金额超阈值且在深夜时段"""
    return df.filter(
        (F.col("amount") > min_amount)
        & (F.hour("timestamp") >= start_hour)
        & (F.hour("timestamp") < end_hour)
    )


def check_merchant_velocity(df: DataFrame, window_minutes=1, threshold=3) -> DataFrame:
    """R4: 商户高频 - 同一商户窗口内交易次数超阈值"""
    window_spec = (
        Window.partitionBy("merchant_id")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-window_minutes * 60, 0)
    )

    return df.withColumn("merchant_txn_count", F.count("*").over(window_spec)).filter(
        F.col("merchant_txn_count") > threshold
    )


def apply_all_rules(df: DataFrame) -> DataFrame:
    """应用所有启用的欺诈规则"""
    rules = load_rules_config()
    alerts = []

    salt = get_config()["pii_salt"]
    df = df.withColumn(
        "user_hash",
        F.sha2(F.concat(F.col("user_id"), F.lit(salt)), 256),
    )

    if rules["R1"]["enabled"]:
        r1_alerts = check_velocity(
            df,
            window_minutes=rules["R1"]["window_minutes"],
            threshold=rules["R1"]["threshold"],
        )
        alerts.append(r1_alerts.withColumn("rule_type", F.lit("VELOCITY_FRAUD")))

    if rules["R2"]["enabled"]:
        r2_alerts = check_geo_jump(
            df,
            max_distance_km=rules["R2"]["max_distance_km"],
            max_interval_minutes=rules["R2"]["max_interval_minutes"],
        )
        alerts.append(r2_alerts.withColumn("rule_type", F.lit("GEO_JUMP")))

    if rules["R3"]["enabled"]:
        r3_alerts = check_night_large(
            df,
            min_amount=rules["R3"]["min_amount"],
            start_hour=rules["R3"]["start_hour"],
            end_hour=rules["R3"]["end_hour"],
        )
        alerts.append(r3_alerts.withColumn("rule_type", F.lit("NIGHT_LARGE")))

    if rules["R4"]["enabled"]:
        r4_alerts = check_merchant_velocity(
            df,
            window_minutes=rules["R4"]["window_minutes"],
            threshold=rules["R4"]["threshold"],
        )
        alerts.append(r4_alerts.withColumn("rule_type", F.lit("MERCHANT_VELOCITY")))

    if alerts:
        from functools import reduce
        return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), alerts)
    return df.filter(F.lit(False))
