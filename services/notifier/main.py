import os
import json
import logging
from collections import OrderedDict

import requests
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def format_alert_message(alert):
    """
    Ham thuan format noi dung canh bao de unit test de dang.
    """
    level = alert.get("level") or "UNKNOWN"
    aqi = alert.get("aqi")
    borough = alert.get("borough") or "Khong ro"
    pollutant = alert.get("dominant_pollutant")
    alert_type = alert.get("type") or "UNKNOWN"

    if alert_type == "ESCALATE":
        icon = "🚨 [KHAN CAP]"
    elif alert_type == "RECOVERED":
        icon = "✅ [HOI PHUC]"
    else:
        icon = "⚠ [MAT DU LIEU]"

    msg = f"{icon} CANH BAO CHAT LUONG KHONG KHI\n"
    msg += f"📍 Khu vuc: {borough}\n"
    msg += f"📉 Trang thai: {alert_type} ({alert.get('prev_level') or '?'} -> {level})\n"
    msg += f"☣ Muc do: {level}" + (f" (AQI: {aqi})" if aqi is not None else "") + "\n"
    if pollutant:
        msg += f"🌫 Tac nhan chinh: {pollutant}\n"
    msg += f"🕒 Thoi gian (UTC): {alert.get('event_time')}"
    return msg

def send_telegram(message):
    """ Gui tin nhan qua Telegram Bot API (Tuy chon) """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        res = requests.post(url, json=payload, timeout=5)
        if res.status_code != 200:
            logger.warning(f"Loi gui Telegram: {res.text}")
    except Exception as e:
        logger.error(f"Loi cau hinh mang gui Telegram: {e}")

class SeenAlerts:
    """Nho cac alert_id gan day. Topic la at-least-once (outbox co the gui lai sau su co),
    nen notifier bo qua alert_id da gui de nguoi dan khong nhan trung."""

    def __init__(self, capacity=10000):
        self.capacity = capacity
        self._ids = OrderedDict()

    def check_and_add(self, alert_id) -> bool:
        """True neu alert_id moi (can gui), False neu da gap."""
        if alert_id in self._ids:
            return False
        self._ids[alert_id] = None
        if len(self._ids) > self.capacity:
            self._ids.popitem(last=False)
        return True


def main():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'aq_notifier_service',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe(['aq.alerts.level-changed.v1'])
    seen = SeenAlerts()
    
    logger.info("🚀 Notifier da khoi dong. Dang lang nghe canh bao tu Kafka...")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(msg.error())
                    break
                    
            try:
                alert = json.loads(msg.value().decode('utf-8'))
                if not seen.check_and_add(alert.get("alert_id")):
                    logger.info(f"Bo qua alert trung {alert.get('alert_id')}")
                    continue

                # Format
                text_msg = format_alert_message(alert)
                
                # Log ra console de User de dang thay bang lenh `docker logs`
                logger.info(f"\n{'-'*40}\n{text_msg}\n{'-'*40}")
                
                # Gui len nhom chat
                send_telegram(text_msg)
            except Exception as e:
                logger.error(f"Loi giai ma JSON canh bao: {e}")
                
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        logger.info("Notifier da tat.")

if __name__ == "__main__":
    main()
