# consumers/validation_consumers.py

from confluent_kafka import Consumer, Producer, KafkaError
from datetime import datetime, timezone
import json
import logging
import psycopg2
from pydantic import ValidationError

from config.settings import settings
from config.topics import Topics
from config.schemas import FREDReport, NewsEvent, PriceTick, ShipmentEvent, WeatherObservation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# maps each raw topic to its Pydantic schema and validated output topic
TOPIC_CONFIG = {
    Topics.RAW_PRICE_TICKS: {
        "schema":       PriceTick,
        "output_topic": Topics.VALIDATED_PRICES,
    },
    Topics.RAW_EIA_REPORTS: {
        "schema":       FREDReport,
        "output_topic": Topics.VALIDATED_CONTEXT,
    },
    Topics.RAW_WEATHER: {
        "schema":       WeatherObservation,
        "output_topic": Topics.VALIDATED_CONTEXT,
    },
    Topics.RAW_SHIPMENTS: {
        "schema":       ShipmentEvent,
        "output_topic": Topics.VALIDATED_CONTEXT,
    },
    Topics.RAW_NEWS: {
        "schema":       NewsEvent,
        "output_topic": Topics.VALIDATED_CONTEXT,
    },
}

def get_db_connection():
    return psycopg2.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        dbname=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASS,
    )

def write_dead_letter(conn, topic: str, raw_payload: dict, error: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.dead_letter (topic, raw_payload, error_message, received_at)
            VALUES (%s, %s, %s, %s)
            """,
            (topic, json.dumps(raw_payload), error, datetime.now(timezone.utc)),
        )
    conn.commit()

def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")

def run():
    consumer = Consumer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "validation-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
    conn = get_db_connection()

    consumer.subscribe(list(TOPIC_CONFIG.keys()))
    logger.info(f"Validation consumer subscribed to: {list(TOPIC_CONFIG.keys())}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            err = msg.error()
            if err is not None:
                if err.code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Consumer error: {err}")
                continue

            topic = msg.topic()
            if topic is None:
                logger.error("Received message with no topic")
                continue
            
            value = msg.value()
            if value is None:
                logger.error(f"Received message with no value on topic {topic}")
                continue

            raw_value = json.loads(value.decode("utf-8"))
            config = TOPIC_CONFIG[topic]

            try:
                # validate with Pydantic
                validated = config["schema"](**raw_value)

                # add event_type tag for context consumer to route on
                output_payload = raw_value.copy()
                output_payload["_event_type"] = config["schema"].__name__

                producer.produce(
                    topic=config["output_topic"],
                    key=msg.key(),
                    value=json.dumps(output_payload),
                    on_delivery=delivery_report,
                )
                producer.poll(0)

            except (ValidationError, Exception) as e:
                logger.warning(f"Validation failed [{topic}]: {e}")
                write_dead_letter(conn, topic, raw_value, str(e))

    except KeyboardInterrupt:
        logger.info("Validation consumer shutting down...")
    finally:
        consumer.close()
        producer.flush()
        conn.close()

if __name__ == "__main__":
    run()