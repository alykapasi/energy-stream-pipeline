# producers/price_producer.py

from confluent_kafka import Producer
from datetime import datetime, timezone
import json
import logging
import math
import random
import time
import yfinance as yf

from config.settings import settings
from config.topics import Topics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

TICKER_MAP = {
    "WTI": "CL=F",
    "BRENT": "BZ=F",
    "NATURAL_GAS": "NG=F",
}

def make_producer() -> Producer:
    return Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

def fetch_current_prices() -> dict[str, dict]:
    prices = {}
    for commodity, ticker in TICKER_MAP.items():
        try:
            data = yf.Ticker(ticker)
            info = data.fast_info
            price = float(info.last_price) # type: ignore
            session_open = float(info.open) if info.open else price
            volume = float(info.three_month_average_volume or 0)
            prices[commodity] = {
                "price": round(price, 4),
                "session_open": round(session_open, 4),
                "volume": round(volume, 2),
            }
            logger.info(f"Fetched {commodity}: ${price:.4f}")
        except Exception as e:
            logger.error(f"Failed to fetch {commodity} ({ticker}): {e}")
    return prices

def simulate_tick(current_price: float, volatility: float = 0.001) -> float:
    """
    Geometric Brownian Motion tick simulation
    Generates a realistic next price from the current price
    volatility=0.001 -> ~0.1% per step, typical for 30-second energy futures
    """
    dt = 1
    random_shock = random.gauss(0, 1)
    drift = 0 # assume no drift for short intervals
    price_change = current_price * (drift * dt + volatility * math.sqrt(dt) * random_shock)
    return round(max(0.01, current_price + price_change), 4)

def delivery_report(err, msg) -> None:
    if err:
        logger.error(f"Delivery failed for {msg.key()}: {err}")

def run():
    producer = make_producer()
    logger.info("Price producer starting - fetching initial prices from yfinance...")

    # seed with real prices
    prices = fetch_current_prices()
    if not prices:
        logger.error("Could not fetch any initial prices. Exiting.")
        return
    
    last_real_fetch = time.time()
    real_fetch_interval = 300 # reseed from yfinance every 5 minutes

    logger.info(f"Seeded prices: {prices}")
    logger.info("Starting tick simulation loop (30s interval)...")

    while True:
        now = time.time()

        # re-seed from yfinance every 5 minutes
        if now - last_real_fetch >= real_fetch_interval:
            logger.info("Re-seeding prices from yfinance...")
            fresh = fetch_current_prices()
            if fresh:
                prices = fresh
                last_real_fetch = now

        for commodity, state in prices.items():
            # simulate next tick from current price
            new_price = simulate_tick(state["price"])
            prices[commodity]["price"] = new_price

            event = {
                "commodity": commodity,
                "price": new_price,
                "session_open": state["session_open"],
                "volume": state["volume"],
                "event_ts": datetime.now(timezone.utc).isoformat(),
            }

            producer.produce(
                topic=Topics.RAW_PRICE_TICKS,
                key=commodity,
                value=json.dumps(event),
                on_delivery=delivery_report,
            )
            logger.info(f"Published {commodity} tick: ${new_price: .4f}")

        producer.poll(0)
        time.sleep(settings.PRICE_POLL_INTERVAL_SECS)

if __name__ == "__main__":
    run()
