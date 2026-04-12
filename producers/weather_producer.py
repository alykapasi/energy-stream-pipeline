# producers/weather_producer.py

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

OWM_BASE = "https://api.openweathermap.org/data/2.5/weather"

def make_producer() -> Producer:
    return Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

def fetch_weather(lat: float, lon: float) -> dict:
    params = {
        "lat": lat,
        "lon": lon,
        "appid": settings.OPENWEATHER_API_KEYS,
        "units": "imperial" # farenheit - standard for us energy markets
    }
    res = requests.get(OWM_BASE, params=params, timeout=15)
    res.raise_for_status()
    return res.json()

def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed for {msg.key()}: {err}")

def run():
    producer = make_producer()
    logger.info("Weather producer starting...")

    while True:
        for station in settings.WEATHER_STATIONS:
            try:
                data = fetch_weather(station["lat"], station["lon"])

                temp_f = data["main"]["temp"]
                wind_mph = data["wind"]["speed"]
                humidity = data["main"]["humidity"]
                conditions = data["weather"][0]["description"]
                hdd = max(0, 65 - temp_f)
                cdd = max(0, temp_f - 65)

                event = {
                    "station_name": station["name"],
                    "station_city": station["city"],
                    "commodity": station["commodity"],
                    "temp_f": round(temp_f, 2),
                    "wind_speed_mph": round(wind_mph, 2),
                    "conditions": conditions,
                    "humidity_pct": float(humidity),
                    "hdd": round(hdd, 2),
                    "cdd": round(cdd, 2),
                    "event_ts": datetime.now(timezone.utc).isoformat(),
                }

                producer.produce(
                    topic=Topics.RAW_WEATHER,
                    key=station["name"],
                    value=json.dumps(event),
                    on_delivery=delivery_report,
                )
                logger.info(
                    f"Published weather @ {station['name']}: "
                    f"{temp_f:.1f}°F, HDD={hdd:.1f}, CDD={cdd:.1f}"
                )

            except Exception as e:
                logger.error(f"Failed to fetch/publish weather for {station["name"]}: {e}")

        producer.flush()
        logger.info(f"Weather cycle complete. Sleeping {settings.WEATHER_POLL_INTERVAL_SECS}s...")
        time.sleep(settings.WEATHER_POLL_INTERVAL_SECS)

if __name__ == "__main__":
    run()