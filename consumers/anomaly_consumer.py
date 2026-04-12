# consumers/anomaly_consumer.py

from collections import deque
from confluent_kafka import Consumer, Producer, KafkaError
import json
import logging
import math

from config.settings import settings
from config.topics import Topics

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def compute_z_score(values: deque) -> float:
    if len(values) < 3:
        return 0.0
    n = len(values)
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / n
    std = math.sqrt(variance)
    if std == 0:
        return 0.0
    latest  = values[-1]
    return round((latest - mean) / std, 4)

def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")


def run():
    consumer = Consumer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "anomaly-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([Topics.ENRICHED_PRICES])
    logger.info("Anomaly consumer started — monitoring for statistically unusual price moves...")

    # Rolling price change history per commodity
    change_history: dict[str, deque] = {
        "WTI": deque(maxlen=settings.ANOMALY_LOOKBACK_PERIODS),
        "BRENT": deque(maxlen=settings.ANOMALY_LOOKBACK_PERIODS),
        "NATURAL_GAS": deque(maxlen=settings.ANOMALY_LOOKBACK_PERIODS),
    }

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))
            commodity = event["commodity"]
            change_pct = event.get("price_change_pct", 0.0)

            history = change_history[commodity]
            history.append(change_pct)

            z_score = compute_z_score(history)

            if abs(z_score) >= settings.ANOMALY_Z_SCORE_THRESHOLD:
                alert = {
                    "commodity": commodity,
                    "price": event["price"],
                    "z_score": z_score,
                    "price_change_pct": change_pct,
                    "context_snapshot": {
                        "eia_inventory_surprise_pct": event.get("eia_inventory_surprise_pct"),
                        "weather_hdd": event.get("weather_hdd"),
                        "weather_cdd": event.get("weather_cdd"),
                        "active_disruptions": event.get("active_disruptions"),
                        "sentiment_score_rolling": event.get("sentiment_score_rolling"),
                        "latest_headline": event.get("latest_headline"),
                    },
                    "event_ts": event["event_ts"],
                }

                producer.produce(
                    topic=Topics.ALERT_ANOMALIES,
                    key=commodity,
                    value=json.dumps(alert),
                    on_delivery=delivery_report,
                )
                producer.poll(0)

                logger.warning(
                    f"ANOMALY DETECTED — {commodity}: "
                    f"z={z_score:.2f}, change={change_pct:+.4f}%"
                )

    except KeyboardInterrupt:
        logger.info("Anomaly consumer shutting down...")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    run()