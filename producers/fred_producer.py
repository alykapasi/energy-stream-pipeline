# producers/fred_producer.py

from confluent_kafka import Producer 
from datetime import datetime, timezone
import json
import logging
import requests
import time

from config.settings import settings
from config.topics import Topics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

def make_producer() -> Producer:
    return Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

def fetch_series(series_id: str) -> list[dict]:
    params = {
        "series_id": series_id,
        "api_key": settings.FRED_API_KEYS,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 2
    }
    res = requests.get(FRED_BASE, params=params, timeout=15)
    res.raise_for_status()
    data = res.json()
    return data.get("observations", [])

def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed for {msg.key()}: {err}")

def run():
    producer = make_producer()
    logger.info("FRED producer starting...")

    while True:
        for series in settings.FRED_SERIES:
            try:
                observations = fetch_series(series["id"])
                if len(observations) < 1:
                    logger.warning(f"No observations for {series["id"]}")
                    continue

                latest = observations[0]
                prior = observations[1] if len(observations) > 1 else None

                # skip if value is missing (FRED uses "." for missing)
                if latest["value"] == ".":
                    logger.warning(f"Missing value for {series["id"]} on {latest["data"]}")
                    continue

                event = {
                    "fred_series_id": series["id"],
                    "fred_series_name": series["name"],
                    "commodity": series["commodity"],
                    "observed_value": float(latest["value"]),
                    "prior_value": float(prior["value"]) if prior and prior["value"] != "." else None,
                    "units": series["units"],
                    "observation_date": latest["date"],
                    "event_ts": datetime.now(timezone.utc).isoformat(),
                }

                producer.produce(
                    topic=Topics.RAW_EIA_REPORTS,
                    key=series["id"],
                    value=json.dumps(event),
                    on_delivery=delivery_report
                )
                logger.info(f"Published FRED series {series["id"]}: {event["value"]} {series["units"]}")

            except Exception as e:
                logger.error(f"Failed to fetch/publish {series["id"]}: {e}")

        producer.flush()
        logger.info(f"FRED cycle complete. Sleeping {settings.FRED_POLL_INTERVAL_SECS}s...")
        time.sleep(settings.FRED_POLL_INTERVAL_SECS)

if __name__ == "__main__":
    run()