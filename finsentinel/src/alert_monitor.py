import logging
import os
import sys
import time
from datetime import datetime

from config.settings import get_config
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("alert_monitor")


class AlertMonitor:
    def __init__(self):
        config = get_config()
        mongo_config = config["mongo"]
        self.client = MongoClient(mongo_config["uri"])
        self.db = self.client[mongo_config["database"]]
        self.collection = self.db[mongo_config["collection"]]
        self.last_check = datetime.utcnow()

    def watch_alerts(self):
        """监控 MongoDB 中的新告警"""
        logger.info("=" * 60)
        logger.info("  FinSentinel Alert Monitor")
        logger.info("  Watching for fraud alerts in MongoDB...")
        logger.info("=" * 60)

        try:
            # Attempt to use Change Streams first
            logger.info("Attempting to use MongoDB Change Streams...")
            # Try to query a dummy thing first or just open watch
            change_stream = self.collection.watch(max_await_time_ms=1000)
            logger.info("MongoDB Change Streams successfully started.")
            for change in change_stream:
                if change["operationType"] == "insert":
                    self._print_alert(change["fullDocument"])
        except Exception as e:
            logger.warning(f"MongoDB Change Streams not available ({e}), falling back to polling...")
            # Fallback to polling using UTC
            self.last_check = datetime.utcnow()
            try:
                while True:
                    # Query newer alerts in ascending order of alert_time
                    new_alerts = self.collection.find(
                        {"alert_time": {"$gt": self.last_check.isoformat()}}
                    ).sort("alert_time", 1)

                    latest_time = self.last_check
                    for alert in new_alerts:
                        self._print_alert(alert)
                        try:
                            # Parse alert_time back to datetime to update cursor
                            alert_dt = datetime.fromisoformat(alert["alert_time"])
                            if alert_dt > latest_time:
                                latest_time = alert_dt
                        except Exception:
                            pass

                    self.last_check = latest_time
                    time.sleep(2)
            except KeyboardInterrupt:
                logger.info("Alert monitor stopped.")
        finally:
            self.client.close()

    def _print_alert(self, alert):
        """打印告警信息（红色高亮）"""
        rule_type = alert.get("rule_type", "UNKNOWN")
        transaction_id = alert.get("transaction_id", "N/A")
        amount = alert.get("amount", 0)
        alert_time = alert.get("alert_time", "")

        # We print directly to stdout to preserve the beautiful terminal color formatting
        print("\n" + "=" * 60)
        print("\033[91m[ALERT] FRAUD DETECTED!\033[0m")
        print(f"  Rule:    {rule_type}")
        print(f"  TXN ID:  {transaction_id}")
        print(f"  Amount:  ${amount:.2f}")
        print(f"  Time:    {alert_time} (UTC)")
        print("=" * 60, flush=True)


def watch_alerts():
    """启动告警监控"""
    monitor = AlertMonitor()
    monitor.watch_alerts()


if __name__ == "__main__":
    watch_alerts()
