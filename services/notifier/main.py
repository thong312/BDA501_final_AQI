import os
import json
import logging
import requests
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def format_alert_message(alert):
    """
    Hàm thuần format nội dung cảnh báo để unit test dễ dàng.
    """
    level = alert.get("level", "UNKNOWN")
    aqi = alert.get("aqi", "?")
    borough = alert.get("borough", "Unknown City")
    pollutant = alert.get("dominant_pollutant", "?")
    alert_type = alert.get("type", "UNKNOWN")
    
    if alert_type == "ESCALATE":
        icon = "🚨 [KHẨN CẤP]"
    elif alert_type == "RECOVERED":
        icon = "✅ [HỒI PHỤC]"
    else:
        icon = "⚠️ [CHÚ Ý]"
        
    msg = f"{icon} CẢNH BÁO CHẤT LƯỢNG KHÔNG KHÍ\n"
    msg += f"📍 Khu vực: {borough}\n"
    msg += f"📉 Trạng thái: {alert_type}\n"
    msg += f"☣️ Mức độ: {level} (AQI: {aqi})\n"
    msg += f"🌫️ Tác nhân chính: {pollutant}\n"
    msg += f"🕒 Thời gian (UTC): {alert.get('event_time')}"
    return msg

def send_telegram(message):
    """ Gửi tin nhắn qua Telegram Bot API (Tuỳ chọn) """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        res = requests.post(url, json=payload, timeout=5)
        if res.status_code != 200:
            logger.warning(f"Lỗi gửi Telegram: {res.text}")
    except Exception as e:
        logger.error(f"Lỗi cấu hình mạng gửi Telegram: {e}")

def main():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'aq_notifier_service',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe(['aq.alerts.level-changed.v1'])
    
    logger.info("🚀 Notifier đã khởi động. Đang lắng nghe cảnh báo từ Kafka...")
    
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
                
                # Format
                text_msg = format_alert_message(alert)
                
                # Log ra console để User dễ dàng thấy bằng lệnh `docker logs`
                logger.info(f"\n{'-'*40}\n{text_msg}\n{'-'*40}")
                
                # Gửi lên nhóm chat
                send_telegram(text_msg)
            except Exception as e:
                logger.error(f"Lỗi giải mã JSON cảnh báo: {e}")
                
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        logger.info("Notifier đã tắt.")

if __name__ == "__main__":
    main()
