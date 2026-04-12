# consumers/sink_consumer.py

from confluent_kafka import Consumer, KafkaError
from datetime import datetime, timezone
import json
import logging
import psycopg2
from psycopg2.extras import execute_values

from config.settings import settings
from config.topics import Topics

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def get_db_connection():
    return psycopg2.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        dbname=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASS,
    )

def sink_price_tick(conn, event: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events.price_ticks
                (commodity, price, session_open, volume, event_ts)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                event["commodity"],
                event["price"],
                event["session_open"],
                event.get("volume", 0),
                event["event_ts"],
            ),
        )
    conn.commit()

def sink_fred_report(conn, event: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events.fred_reports
                (fred_series_id, fred_series_name, commodity, observed_value,
                 prior_value, units, observation_date, event_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event["fred_series_id"],
                event["fred_series_name"],
                event["commodity"],
                event["observed_value"],
                event.get("prior_value"),
                event["units"],
                event["observation_date"],
                event["event_ts"],
            ),
        )
    conn.commit()

def sink_weather(conn, event: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events.weather_observations
                (station_name, station_city, commodity, temp_f,
                 wind_speed_mph, conditions, humidity_pct, hdd, cdd, event_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event["station_name"],
                event["station_city"],
                event["commodity"],
                event["temp_f"],
                event["wind_speed_mph"],
                event["conditions"],
                event["humidity_pct"],
                event["hdd"],
                event["cdd"],
                event["event_ts"],
            ),
        )
    conn.commit()

def sink_shipment(conn, event: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events.shipment_events
                (event_id, event_type, origin_port, destination_region,
                 commodity, volume_barrels, severity, expected_duration_hrs, event_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event["event_id"],
                event["event_type"],
                event["origin_port"],
                event["destination_region"],
                event["commodity"],
                event["volume_barrels"],
                event["severity"],
                event["expected_duration_hrs"],
                event["event_ts"],
            ),
        )
    conn.commit()

def sink_news(conn, event: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events.news_events
                (headline, source, article_url, commodities_mentioned,
                 sentiment, sentiment_score, published_at, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event["headline"],
                event["source"],
                event.get("url"),
                event.get("commodities_mentioned", []),
                event["sentiment"],
                event["sentiment_score"],
                event.get("published_at"),
                event.get("fetched_at"),
            ),
        )
    conn.commit()

def sink_anomaly(conn, event: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO analytics.anomalies
                (commodity, price, z_score, price_change_pct, context_snapshot, event_ts)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                event["commodity"],
                event["price"],
                event["z_score"],
                event["price_change_pct"],
                json.dumps(event.get("context_snapshot", {})),
                event["event_ts"],
            ),
        )
    conn.commit()

def sink_impact(conn, event: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO analytics.impact_events
                (commodity, price, z_score, likely_driver,
                 driver_confidence, supporting_factors, event_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event["commodity"],
                event["price"],
                event["z_score"],
                event["likely_driver"],
                event["driver_confidence"],
                event.get("supporting_factors", []),
                event["event_ts"],
            ),
        )
    conn.commit()

SINK_HANDLERS = {
    Topics.VALIDATED_PRICES: sink_price_tick,
    Topics.VALIDATED_CONTEXT: None,  # routed by event_type below
    Topics.ALERT_ANOMALIES: sink_anomaly,
    Topics.ALERT_IMPACTS: sink_impact,
}

CONTEXT_HANDLERS = {
    "FREDReport": sink_fred_report,
    "WeatherObservation": sink_weather,
    "ShipmentEvent": sink_shipment,
    "NewsEvent": sink_news,
}

def run():
    conn = get_db_connection()
    consumer = Consumer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "sink-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    topics_to_consume = [
        Topics.VALIDATED_PRICES,
        Topics.VALIDATED_CONTEXT,
        Topics.ALERT_ANOMALIES,
        Topics.ALERT_IMPACTS,
    ]
    consumer.subscribe(topics_to_consume)
    logger.info("Sink consumer started — writing all validated events to Postgres...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            logger.info(f"Received message from {msg.topic()} | event_type={json.loads(msg.value()).get('_event_type', 'N/A')}")

            topic = msg.topic()
            event = json.loads(msg.value().decode("utf-8"))

            try:
                if topic == Topics.VALIDATED_CONTEXT:
                    event_type = event.get("_event_type", "")
                    handler = CONTEXT_HANDLERS.get(event_type)
                    if handler:
                        handler(conn, event)
                        logger.debug(f"Sinked {event_type} to Postgres")
                    else:
                        logger.warning(f"No sink handler for event type: {event_type}")
                else:
                    handler = SINK_HANDLERS.get(topic)
                    if handler:
                        handler(conn, event)
                        logger.debug(f"Sinked {topic} event to Postgres")

            except Exception as e:
                logger.error(f"Sink error [{topic}]: {e}")
                conn.rollback()

    except KeyboardInterrupt:
        logger.info("Sink consumer shutting down...")
    except Exception as exc:
        import traceback
        logger.error(f"Sink error [{topic}] event_type={event.get('_event_type')}: {e}")
        logger.error(traceback.format_exc())
        conn.rollback()
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    run()