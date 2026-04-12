# consumers/ohlcv_consumer.py

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import psycopg2
from confluent_kafka import Consumer, KafkaError

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

def floor_to_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)

def upsert_ohlcv(conn, commodity: str, window: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO analytics.ohlcv_1min
                (commodity, price_open, price_high, price_low, price_close, volume, vwap, window_start, window_end)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (commodity, window_start) DO UPDATE SET
                high = GREATEST(analytics.ohlcv_1min.high, EXCLUDED.high),
                low = LEAST(analytics.ohlcv_1min.low, EXCLUDED.low),
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                vwap = EXCLUDED.vwap,
                window_end = EXCLUDED.window_end
            """,
            (
                commodity,
                window["price_open"],
                window["price_high"],
                window["price_low"],
                window["price_close"],
                window["volume"],
                window["vwap"],
                window["window_start"],
                window["window_end"],
            ),
        )
    conn.commit()

def run():
    conn = get_db_connection()
    consumer = Consumer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "ohlcv-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([Topics.ENRICHED_PRICES])
    logger.info("OHLCV consumer started — computing 1-minute candles...")

    # In-memory window state per commodity
    # Structure: {commodity: {window_start: {open, high, low, close, volume, pv_sum}}}
    windows: dict[str, dict] = defaultdict(dict)
    last_flush: dict[str, datetime] = {}

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
            volume = float(event.get("volume", 1.0))  # default 1 if missing
            event_ts = datetime.fromisoformat(event["event_ts"])
            window_start = floor_to_minute(event_ts)
            window_end = window_start + timedelta(minutes=1)
            window_key = window_start.isoformat()

            if window_key not in windows[commodity]:
                # New window — initialize OHLCV
                windows[commodity][window_key] = {
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": volume,
                    "pv_sum": price * volume,  # for VWAP: sum(price * volume)
                    "window_start": window_start,
                    "window_end": window_end,
                }
            else:
                w = windows[commodity][window_key]
                w["high"] = max(w["high"], price)
                w["low"] = min(w["low"], price)
                w["close"] = price
                w["volume"] += volume
                w["pv_sum"] += price * volume

            # Flush closed windows (any window that's not the current one)
            now = datetime.now(timezone.utc)
            for wkey, w in list(windows[commodity].items()):
                if w["window_end"] <= now:
                    # Compute VWAP = sum(price * volume) / total_volume
                    vwap = round(w["pv_sum"] / w["volume"], 4) if w["volume"] > 0 else w["close"]
                    w["vwap"] = vwap
                    upsert_ohlcv(conn, commodity, w)
                    logger.info(
                        f"OHLCV [{commodity}] {wkey}: "
                        f"O={w['open']:.4f} H={w['high']:.4f} "
                        f"L={w['low']:.4f} C={w['close']:.4f} "
                        f"VWAP={vwap:.4f}"
                    )
                    del windows[commodity][wkey]

    except KeyboardInterrupt:
        logger.info("OHLCV consumer shutting down...")
        # Flush any remaining open windows on shutdown
        now = datetime.now(timezone.utc)
        for commodity, wmap in windows.items():
            for wkey, w in wmap.items():
                vwap = round(w["pv_sum"] / w["volume"], 4) if w["volume"] > 0 else w["close"]
                w["vwap"] = vwap
                upsert_ohlcv(conn, commodity, w)
                logger.info(f"Flushed open window [{commodity}] {wkey} on shutdown")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run()