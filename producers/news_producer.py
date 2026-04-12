# producers/news_producer.py

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

NEWS_BASE = "https://newsapi.org/v2/everything"

BULLISH_KEYWORDS = [
    "supply cut", "opec cut", "production cut", "shortage", "disruption",
    "sanctions", "outage", "pipeline", "hurricane", "freeze", "cold snap",
    "demand surge", "drawdown", "inventory draw",
]

BEARISH_KEYWORDS = [
    "demand slowdown", "recession", "oversupply", "inventory build",
    "production increase", "opec increase", "surplus", "weak demand",
    "economic slowdown", "stockpile build", "mild weather"
]

COMMODITY_KEYWORDS = {
    "WTI": ["wti", "crude oil", "west texas", "oil price", "barrel"],
    "BRENT": ["brent", "crude", "north sea", "oil price", "barrel"],
    "NATURAL_GAS": ["natural gas", "henry hub", "lng", "gas price", "natgas"],
}

def score_sentiment(headline: str) -> tuple[str, float]:
    text = headline.lower()
    bullish_hits = sum(1 for kw in BULLISH_KEYWORDS if kw in text)
    bearish_hits = sum(1 for kw in BEARISH_KEYWORDS if kw in text)

    if bullish_hits > bearish_hits:
        score = min(1.0, bullish_hits * 0.3)
        return "BULLISH", round(score, 3)
    elif bearish_hits > bullish_hits:
        score = max(-1.0, -bearish_hits * 0.3)
        return "BEARISH", round(score, 3)
    return "NEUTRAL", 0.0

def detect_commodities(headline: str) -> list[str]:
    text = headline.lower()
    return [
        commodity for commodity, keywords in COMMODITY_KEYWORDS.items()
        if any(kw in text for kw in keywords)
    ] or ["WTI"] # default to WTI if no specific commodity detected

def make_producer() -> Producer:
    return Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

def run():
    producer = make_producer()
    logger.info("News producer starting...")
    seen_urls: set[str] = set() # dedup within session

    while True:
        try:
            params = {
                "q": "oil OR gas OR energy OR OPEC or crude",
                "sortBy": "publishedAt",
                "pageSize": 20,
                "language": "en",
                "apiKey": settings.NEWS_API_KEY,
            }
            res = requests.get(NEWS_BASE, params=params, timeout=15)
            res.raise_for_status()
            articles = res.json().get("articles", [])

            new_count = 0
            for article in articles:
                url = article.get("url", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                headline = article.get("title", "")
                source = article.get("source", {}).get("name", "unknown")
                published = article.get("publishedAt", "")
                sentiment, score = score_sentiment(headline)
                commodities = detect_commodities(headline)

                event = {
                    "headline":             headline,
                    "source":               source,
                    "url":                  url,
                    "commodities_mentioned": commodities,
                    "sentiment":            sentiment,
                    "sentiment_score":      score,
                    "published_at":         published,
                    "fetched_at":           datetime.now(timezone.utc).isoformat(),
                }

                producer.produce(
                    topic=Topics.RAW_NEWS,
                    key=commodities[0],
                    value=json.dumps(event),
                    on_delivery=lambda err, msg: (
                        logger.error(f"Delivery failed: {err}") if err else None
                    ),
                )
                new_count += 1

            producer.flush()
            logger.info(f"Published {new_count} new articles. Sleeping {settings.NEWS_POLL_INTERVAL_SECS}s...")

        except Exception as e:
            logger.error(f"News producer error: {e}")

        time.sleep(settings.NEWS_POLL_INTERVAL_SECS)

if __name__ == "__main__":
    run()