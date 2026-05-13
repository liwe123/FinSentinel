import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import get_config
from pymongo import MongoClient


class AlertMonitor:
    def __init__(self):
        config = get_config()
        mongo_config = config["mongo"]
        self.client = MongoClient(mongo_config["uri"])
        self.db = self.client[mongo_config["database"]]
        self.collection = self.db[mongo_config["collection"]]
        self.last_check = datetime.now()

    def watch_alerts(self):
        """监控 MongoDB 中的新告警"""
        print("=" * 60)
        print("  FinSentinel Alert Monitor")
        print("  Watching for fraud alerts in MongoDB...")
        print("=" * 60)

        try:
            while True:
                new_alerts = self.collection.find(
                    {"alert_time": {"$gt": self.last_check.isoformat()}}
                ).sort("alert_time", -1)

                for alert in new_alerts:
                    self._print_alert(alert)

                self.last_check = datetime.now()
                time.sleep(2)

        except KeyboardInterrupt:
            print("\nAlert monitor stopped.")
        finally:
            self.client.close()

    def _print_alert(self, alert):
        """打印告警信息（红色高亮）"""
        rule_type = alert.get("rule_type", "UNKNOWN")
        transaction_id = alert.get("transaction_id", "N/A")
        amount = alert.get("amount", 0)
        alert_time = alert.get("alert_time", "")

        print("\n" + "=" * 60)
        print("\033[91m[ALERT] FRAUD DETECTED!\033[0m")
        print(f"  Rule:    {rule_type}")
        print(f"  TXN ID:  {transaction_id}")
        print(f"  Amount:  ${amount:.2f}")
        print(f"  Time:    {alert_time}")
        print("=" * 60)


def watch_alerts():
    """启动告警监控"""
    monitor = AlertMonitor()
    monitor.watch_alerts()


if __name__ == "__main__":
    watch_alerts()
