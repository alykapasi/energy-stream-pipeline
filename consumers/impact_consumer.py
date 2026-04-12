# consumers/impact_consumer.py

from confluent_kafka import Consumer, Producer, KafkaError
import json
import logging

from config.settings import settings
from config.topics import Topics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

def classify_driver(context: dict) -> tuple[str, float, list[str]]:
    """
    Simple decision tree to assign a likely price driver.
    Returns (driver_label, confidence_score, supporting_factors).
    """
    factors = []
    scores = {}

    eia_surprise = abs(float(context.get("eia_inventory_surprise_pct") or 0))
    hdd = float(context.get("weather_hdd") or 0)
    cdd = float(context.get("weather_cdd") or 0)
    disruptions = int(context.get("active_disruptions") or 0)
    sentiment = abs(float(context.get("sentiment_score_rolling") or 0))

    if eia_surprise > 3.0:
        scores["EIA_INVENTORY_REPORT"] = min(0.9, eia_surprise / 10)
        factors.append(f"EIA inventory surprise: {eia_surprise:.1f}%")

    if hdd > 20:
        scores["COLD_WEATHER_DEMAND"] = min(0.8, hdd / 50)
        factors.append(f"High heating demand: {hdd:.0f} HDD")

    if cdd > 20:
        scores["HOT_WEATHER_DEMAND"] = min(0.8, cdd / 50)
        factors.append(f"High cooling demand: {cdd:.0f} CDD")

    if disruptions > 0:
        scores["SUPPLY_DISRUPTION"] = min(0.85, disruptions * 0.4)
        factors.append(f"Active supply disruptions: {disruptions}")

    if sentiment > 0.3:
        scores["NEWS_SENTIMENT"] = min(0.7, sentiment)
        factors.append(f"Strong news sentiment: {sentiment:.2f}")

    if not scores:
        return "TECHNICAL_UNKNOWN", 0.3, ["No clear fundamental driver identified"]

    top_driver = max(scores, key=scores.get)
    confidence = round(scores[top_driver], 3)
    return top_driver, confidence, factors


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")


def run():
    consumer = Consumer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "impact-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([Topics.ALERT_ANOMALIES])
    logger.info("Impact consumer started — classifying anomaly drivers...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            anomaly = json.loads(msg.value().decode("utf-8"))
            context = anomaly.get("context_snapshot", {})

            driver, confidence, factors = classify_driver(context)

            impact_event = {
                "commodity": anomaly["commodity"],
                "price": anomaly["price"],
                "z_score": anomaly["z_score"],
                "likely_driver": driver,
                "driver_confidence": confidence,
                "supporting_factors": factors,
                "event_ts": anomaly["event_ts"],
            }

            producer.produce(
                topic=Topics.ALERT_IMPACTS,
                key=anomaly["commodity"],
                value=json.dumps(impact_event),
                on_delivery=delivery_report,
            )
            producer.poll(0)

            logger.warning(
                f"IMPACT EVENT — {anomaly['commodity']}: "
                f"driver={driver} confidence={confidence:.0%} "
                f"factors={factors}"
            )

    except KeyboardInterrupt:
        logger.info("Impact consumer shutting down...")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    run()