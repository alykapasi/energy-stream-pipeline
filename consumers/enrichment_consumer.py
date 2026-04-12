# consumers/enrichment_consumer.py

from collections import deque
from confluent_kafka import Consumer, Producer, KafkaError
from datetime import datetime, timezone
import json
import logging
import redis

from config.settings import settings
from config.topics import Topics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

def get_redis():
    return redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
    )

def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")

def run():
    r = get_redis()
    consumer = Consumer({
        "bootstrap.servers":    settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id":             "enrichment-consumer-group",
        "auto.offset.reset":    "earliest",
        "enable.auto.commit":   True,
    })
    producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([Topics.VALIDATED_PRICES])
    logger.info("Enrichment consumer started — fusing price ticks with market context...")

    # Rolling price history per commodity for change calculation
    price_history: dict[str, deque] = {
        "WTI":          deque(maxlen=10),
        "BRENT":        deque(maxlen=10),
        "NATURAL_GAS":  deque(maxlen=10),
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
            price = float(event["price"])
            session_open = float(event["session_open"])

            # Price change from session open
            price_change_pct = round((price - session_open) / session_open * 100, 4) if session_open else 0.0

            # 5-period rolling price change
            history = price_history[commodity]
            price_change_5p_pct = None
            if len(history) >= 5:
                price_5p_ago = history[-5]
                price_change_5p_pct = round((price - price_5p_ago) / price_5p_ago * 100, 4)
            history.append(price)

            # Pull current context from Redis
            context_key = f"context:{commodity}"
            context = r.hgetall(context_key)

            enriched = {
                "commodity": commodity,
                "price": price,
                "session_open": session_open,
                "price_change_pct": price_change_pct,
                "price_change_5p_pct": price_change_5p_pct,
                "eia_inventory_surprise_pct": float(context.get(f"fred_NATURALGAS_surprise_pct", 0) or 0),
                "weather_hdd": float(context.get("weather_hdd", 0) or 0),
                "weather_cdd": float(context.get("weather_cdd", 0) or 0),
                "weather_temp_f": float(context.get("weather_temp_f", 0) or 0),
                "active_disruptions": int(context.get("active_disruptions", 0) or 0),
                "sentiment_score_rolling": float(context.get("sentiment_score_rolling", 0) or 0),
                "latest_headline": context.get("latest_headline", ""),
                "event_ts": event["event_ts"],
            }

            # Write latest price to Redis for fast API reads
            r.hset(f"price:{commodity}", mapping={
                "price": price,
                "price_change_pct": price_change_pct,
                "session_open": session_open,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            })

            producer.produce(
                topic=Topics.ENRICHED_PRICES,
                key=commodity,
                value=json.dumps(enriched),
                on_delivery=delivery_report,
            )
            producer.poll(0)

            logger.info(
                f"Enriched {commodity}: ${price:.4f} "
                f"({price_change_pct:+.2f}%) "
                f"disruptions={enriched['active_disruptions']} "
                f"sentiment={enriched['sentiment_score_rolling']:.2f}"
            )

    except KeyboardInterrupt:
        logger.info("Enrichment consumer shutting down...")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    run()