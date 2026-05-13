import time
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ConnectionFailure


_TRIGGER_RULES = {
    "VELOCITY_FRAUD": {
        "rule": "R1: 速度欺诈 — 5分钟内 >= 5笔交易",
        "threshold": 5,
    },
    "GEO_JUMP": {
        "rule": "R2: 地理跳跃 — 距离 > 1000km 且间隔 < 10分钟",
        "max_distance_km": 1000,
        "max_interval_minutes": 10,
    },
    "NIGHT_LARGE": {
        "rule": "R3: 深夜大额 — 金额 > $5000 且在 00:00-05:00",
        "min_amount": 5000,
        "hours": "00:00-05:00",
    },
    "MERCHANT_VELOCITY": {
        "rule": "R4: 商户高频 — 1分钟内 > 3笔交易",
        "threshold": 3,
    },
}


class MongoAlertWriter:
    def __init__(self, mongo_uri: str, database: str, collection: str):
        self.mongo_uri = mongo_uri
        self.database = database
        self.collection_name = collection
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = MongoClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
            )
        return self._client

    def _insert_with_retry(self, collection, doc, max_retries: int = 3):
        for attempt in range(max_retries):
            try:
                return collection.insert_one(doc)
            except (AutoReconnect, ConnectionFailure) as e:
                if attempt == max_retries - 1:
                    print(f"[ERROR] MongoDB write failed after {max_retries} retries: {e}")
                    raise
                wait = 2 ** attempt
                print(f"[RETRY] MongoDB attempt {attempt + 1} failed, retrying in {wait}s...")
                time.sleep(wait)

    def _build_trigger_detail(self, row) -> dict:
        rule_type = row["rule_type"]
        template = _TRIGGER_RULES.get(rule_type, {"rule": rule_type})

        detail = dict(template)

        if rule_type == "VELOCITY_FRAUD":
            detail["txn_count"] = row.get("txn_count")
            detail["user_id"] = row.get("user_id")
        elif rule_type == "GEO_JUMP":
            detail["distance_km"] = round(row.get("distance_km", 0), 2) if row.get("distance_km") is not None else None
            detail["time_diff_minutes"] = round(row.get("time_diff_minutes", 0), 2) if row.get("time_diff_minutes") is not None else None
        elif rule_type == "NIGHT_LARGE":
            detail["amount"] = row.get("amount")
        elif rule_type == "MERCHANT_VELOCITY":
            detail["merchant_txn_count"] = row.get("merchant_txn_count")
            detail["merchant_id"] = row.get("merchant_id")

        return detail

    def write_batch(self, batch_df, batch_id):
        if batch_df.count() == 0:
            return

        collection = self.client[self.database][self.collection_name]
        alerts = batch_df.collect()

        for row in alerts:
            alert_doc = {
                "alert_id": f"ALERT-{row['rule_type']}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "rule_type": row["rule_type"],
                "transaction_id": row["transaction_id"],
                "user_hash": row["user_hash"],
                "amount": row["amount"],
                "timestamp": str(row["timestamp"]),
                "trigger_detail": self._build_trigger_detail(row),
                "alert_time": datetime.now().isoformat(),
            }
            self._insert_with_retry(collection, alert_doc)
            print(f"[ALERT] {row['rule_type']}: {row['transaction_id']} - ${row['amount']}")

    def close(self):
        if self._client:
            self._client.close()
            self._client = None


def write_to_mongo(batch_df, batch_id, config: dict):
    writer = MongoAlertWriter(
        config["mongo"]["uri"],
        config["mongo"]["database"],
        config["mongo"]["collection"],
    )
    try:
        writer.write_batch(batch_df, batch_id)
    finally:
        writer.close()
