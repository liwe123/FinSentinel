import logging
import sys
import time
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ConnectionFailure

logger = logging.getLogger("mongo_writer")

_RULE_MAPPING = {
    "VELOCITY_FRAUD": "R1",
    "GEO_JUMP": "R2",
    "NIGHT_LARGE": "R3",
    "MERCHANT_VELOCITY": "R4",
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
                    logger.error(f"MongoDB write failed after {max_retries} retries: {e}")
                    raise
                wait = 2 ** attempt
                logger.warning(f"MongoDB attempt {attempt + 1} failed, retrying in {wait}s...")
                time.sleep(wait)

    def _build_trigger_detail(self, row) -> dict:
        rule_type = row["rule_type"]
        yaml_key = _RULE_MAPPING.get(rule_type)

        detail = {
            "rule": rule_type,
            "description": "",
        }

        if yaml_key:
            try:
                from src.fraud_rules import load_rules_config
                rules_cfg = load_rules_config()
                rule_cfg = rules_cfg.get(yaml_key, {})
                detail["rule"] = f"{yaml_key}: {rule_cfg.get('name', rule_type)} — {rule_cfg.get('description', '')}"
                detail["description"] = rule_cfg.get("description", "")

                for k, v in rule_cfg.items():
                    if k not in ("enabled", "type", "name", "description"):
                        detail[k] = v
            except Exception as e:
                logger.warning(f"Failed to load dynamic rules config: {e}")

        # Populate transaction-specific runtime values
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
                "alert_id": f"ALERT-{row['rule_type']}-{row['transaction_id']}",
                "rule_type": row["rule_type"],
                "transaction_id": row["transaction_id"],
                "user_hash": row["user_hash"],
                "amount": row["amount"],
                "timestamp": str(row["timestamp"]),
                "trigger_detail": self._build_trigger_detail(row),
                "alert_time": datetime.now().isoformat(),
            }
            self._insert_with_retry(collection, alert_doc)
            logger.warning(f"[ALERT] {row['rule_type']}: {row['transaction_id']} - ${row['amount']}")

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
