# consumers/context_consumer.py

from confluent_kafka import Consumer, KafkaError
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

COMMODITY_MAP = {
    "WTI": "WTI",
    "BRENT": "BRENT",
    "NATURAL_GAS": "NATURAL_GAS",
}

def get_redis():
    return redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
    )

def update_fred_context(r: redis.Redis, event: dict):
    commodity = event.get("commodity")
    if not commodity:
        return
    key = f"context:{commodity}"
    surprise_pct = 0.0
    if event.get("prior_value") and event["prior_value"] != 0:
        surprise_pct = round(
            (event["observed_value"] - event["prior_value"]) / abs(event["prior_value"]) * 100, 2
        )
    r.hset(key, mapping={
        f"fred_{event['fred_series_id']}_value": event["observed_value"],
        f"fred_{event['fred_series_id']}_surprise_pct": surprise_pct,
        f"fred_{event['fred_series_id']}_date": event["observation_date"],
    })
    logger.info(f"Updated FRED context for {commodity}: {event['fred_series_id']}={event['observed_value']} (surprise={surprise_pct}%)")

def update_weather_context(r: redis.Redis, event: dict):
    commodity = event.get("commodity")
    if not commodity:
        return
    key = f"context:{commodity}"
    r.hset(key, mapping={
        "weather_temp_f": event["temp_f"],
        "weather_hdd": event["hdd"],
        "weather_cdd": event["cdd"],
        "weather_conditions": event["conditions"],
        "weather_station": event["station_name"],
    })
    logger.info(f"Updated weather context for {commodity} @ {event['station_name']}: {event['temp_f']}°F HDD={event['hdd']}")

def update_shipment_context(r: redis.Redis, event: dict):
    commodity = event.get("commodity")
    if not commodity:
        return
    key = f"context:{commodity}"

    # Track active disruption count — increment on new disruption, not tanker departures
    if event.get("event_type") != "TANKER_DEPARTURE":
        current = int(r.hget(key, "active_disruptions") or 0)
        r.hset(key, "active_disruptions", current + 1)
        r.hset(key, "latest_disruption_type", event["event_type"])
        r.hset(key, "latest_disruption_severity", event["severity"])
        logger.warning(f"Disruption event for {commodity}: {event['event_type']} severity={event['severity']}")
    else:
        r.hset(key, "latest_shipment_origin", event["origin_port"])
        r.hset(key, "latest_shipment_destination", event["destination_region"])
        logger.info(f"Shipment departure: {event['origin_port']} → {event['destination_region']} ({commodity})")

def update_news_context(r: redis.Redis, event: dict):
    commodities = event.get("commodities_mentioned", ["WTI"])
    for commodity in commodities:
        key = f"context:{commodity}"

        # Rolling sentiment: simple average of last score and new score
        current_score = float(r.hget(key, "sentiment_score_rolling") or 0.0)
        new_score = round((current_score + event["sentiment_score"]) / 2, 3)
        r.hset(key, mapping={
            "sentiment_score_rolling": new_score,
            "latest_headline": event["headline"][:200],
            "latest_sentiment": event["sentiment"],
        })
    logger.info(f"Updated news sentiment: {event['sentiment']} ({event['sentiment_score']}) — {event['headline'][:60]}...")

def run():
    r = get_redis()
    consumer = Consumer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "context-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([Topics.VALIDATED_CONTEXT])
    logger.info("Context consumer started — building live market context in Redis...")

    # Initialize empty context for all commodities
    for commodity in COMMODITY_MAP:
        r.hset(f"context:{commodity}", mapping={
            "active_disruptions": 0,
            "sentiment_score_rolling": 0.0,
        })

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
            event_type = event.get("_event_type", "")

            if event_type == "FREDReport":
                update_fred_context(r, event)
            elif event_type == "WeatherObservation":
                update_weather_context(r, event)
            elif event_type == "ShipmentEvent":
                update_shipment_context(r, event)
            elif event_type == "NewsEvent":
                update_news_context(r, event)
            else:
                logger.warning(f"Unknown event type: {event_type}")

    except KeyboardInterrupt:
        logger.info("Context consumer shutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    run()