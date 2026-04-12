# producers/shipment_producer.py

from confluent_kafka import Producer
from datetime import datetime, timezone
import json
import logging
import random
import time
import uuid

from config.settings import settings
from config.topics import Topics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# synthetic - but based on actual energy infrastructure
TANKER_ROUTES = [
    {"origin": "Ras Tanura, Saudi Arabia", "destination": "Asia Pacific", "commodity": "BRENT", "avg_volume": 2_000_000},
    {"origin": "Basra, Iraq", "destination": "Europe", "commodity": "BRENT", "avg_volume": 1_500_000},
    {"origin": "Novorossiysk, Russia", "destination": "Mediterranean", "commodity": "BRENT", "avg_volume": 1_200_000},
    {"origin": "Cushing, Oklahoma", "destination": "US Gulf Coast", "commodity": "WTI", "avg_volume": 800_000},
    {"origin": "Houston Ship Channel", "destination": "Europe", "commodity": "WTI", "avg_volume": 1_000_000},
    {"origin": "Sabine Pass LNG", "destination": "Europe", "commodity": "NATURAL_GAS", "avg_volume": 500_000},
    {"origin": "Freeport LNG", "destination": "Asia Pacific", "commodity": "NATURAL_GAS", "avg_volume": 450_000},
]

DISRUPTION_EVENTS = [
    {"type": "PIPELINE_DISRUPTION", "description": "Pipeline maintenance outage", "severity": "moderate", "duration_range": (4, 48)},
    {"type": "REFINERY_OUTAGE", "description": "Unplanned refinery shutdown", "severity": "major", "duration_range": (24, 168)},
    {"type": "TERMINAL_DELAY", "description": "Port congestion delay", "severity": "minor", "duration_range": (2, 12)},
    {"type": "WEATHER_DISRUPTION", "description": "Severe weather halts operations", "severity": "moderate", "duration_range": (6, 72)},
]

def make_producer() -> Producer:
    return Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

def generate_tanker_departure(route: dict) -> dict:
    volume_variance = random.uniform(0.85, 1.15)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "TANKER DEPARTURE",
        "origin_port": route["origin"],
        "destination_region": route["destination"],
        "commodity": route["commodity"],
        "volume_barrels": round(route["avg_volume"] * volume_variance, 0),
        "severity": "minor",
        "expected_duration_hrs": round(random.uniform(168, 720), 1), # 1-4 weeks transit usually
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }

def generate_disruption_event() -> dict:
    route = random.choice(TANKER_ROUTES)
    disruption = random.choice(DISRUPTION_EVENTS)
    min_h, max_h = disruption["duration_range"]
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": disruption["type"],
        "origin_port": route["origin"],
        "destination_region": route["destination"],
        "commodity": route["commodity"],
        "volume_barrels": round(route["avg_volume"] * random.uniform(0.3, 0.8), 0),
        "severity": disruption["severity"],
        "expected_duration_hrs": round(random.uniform(min_h, max_h), 1),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }

def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed for {msg.key()}: {err}")

def run():
    producer = make_producer()
    logger.info("Shipment producer starting (synthetic data — disclosed in README)...")

    cycle = 0
    while True:
        cycle += 1

        # Every cycle: emit 1–2 tanker departures
        num_departures = random.randint(1, 2)
        for _ in range(num_departures):
            route = random.choice(TANKER_ROUTES)
            event = generate_tanker_departure(route)
            producer.produce(
                topic=Topics.RAW_SHIPMENTS,
                key=event["commodity"],
                value=json.dumps(event),
                on_delivery=delivery_report,
            )
            logger.info(f"Published tanker departure: {event['origin_port']} → {event['destination_region']} ({event['commodity']})")

        # Every 5 cycles (~2.5 hours): emit a disruption event
        if cycle % 5 == 0:
            event = generate_disruption_event()
            producer.produce(
                topic=Topics.RAW_SHIPMENTS,
                key=event["commodity"],
                value=json.dumps(event),
                on_delivery=delivery_report,
            )
            logger.warning(f"Published disruption event: {event['event_type']} at {event['origin_port']} ({event['severity']})")

        producer.flush()
        logger.info(f"Shipment cycle {cycle} complete. Sleeping 30 minutes...")
        time.sleep(1800)  # 30 minutes between shipment events

if __name__ == "__main__":
    run()