# Design Decisions

This document explains the key architectural choices made in the Energy Market Intelligence Pipeline, the tradeoffs considered, and what would change at production scale.

---

## Why merge all context streams into one Kafka topic?

The four non-price streams (FRED, weather, shipments, news) all publish to `validated.context_events` rather than four separate validated topics. The context consumer subscribes to one topic and branches on the `_event_type` tag.

The alternative — four separate validated topics — would require the context consumer to manage four subscriptions, four offset positions, and four consumer group memberships. That complexity buys nothing here because the context consumer's job is identical for all four: update a Redis hash. The merged topic simplifies the consumer substantially at no cost to throughput, since all four streams are low-volume relative to price ticks.

The `_event_type` tag added by the validation consumer is the routing mechanism. It is injected at validation time rather than at the producer, which means producers remain simple — they publish raw domain events without needing to know how downstream consumers will route them.

---

## Why Redis for context state rather than a database lookup?

The enrichment consumer needs the current market context every 30 seconds per commodity (the price tick interval). At three commodities that is six Redis reads per minute, each sub-millisecond. A Postgres lookup would add 2-10ms per read — still fast in absolute terms, but it introduces a synchronous database dependency into the hot path of the streaming pipeline.

More importantly, Redis is the right semantic fit. Context state is not a historical record — it is the current value of a set of slowly-changing signals. Redis hashes (`HSET`/`HGETALL`) map directly to this mental model: one hash per commodity, fields updated in place as new context events arrive. Postgres would require either a wide denormalized row (updated in place) or a query that assembles current state from multiple rows — both more complex than a single `HGETALL`.

The tradeoff is durability. Redis in the default configuration loses in-memory state on restart. For this project that is acceptable — the context consumer replays from `validated.context_events` (Kafka retains 24 hours) and rebuilds state within minutes. In production, Redis AOF persistence or a Redis replica would eliminate this gap.

---

## Why geometric Brownian motion for tick simulation?

GBM is the standard model for short-term asset price simulation in quantitative finance. It produces log-normally distributed returns (prices cannot go negative), incorporates a volatility parameter calibrated to the asset class, and generates realistic-looking price paths without requiring historical data.

The alternative — polling yfinance on every 30-second tick — would hit the Yahoo Finance rate limit within minutes and produce identical prices for every tick within the same trading day (since Yahoo only updates prices every few minutes for futures). GBM gives the pipeline realistic tick-by-tick variation to work with while using real current prices as the seed.

The volatility parameter (`0.001` per step, ~0.1% per 30-second tick) is calibrated to energy futures. Natural gas is more volatile than crude oil in practice — in a production version, commodity-specific volatility parameters would be calibrated from historical realized volatility.

This approach is clearly disclosed in the README and would be replaced with real tick data from a commercial provider (Refinitiv, Bloomberg, or ICE) in production.

---

## Why rolling z-score for anomaly detection rather than a fixed threshold?

A fixed threshold ("alert if price moves more than 1%") breaks down across commodities and market regimes. Natural gas can move 3% in a quiet session; crude oil moving 0.5% during an OPEC announcement is more significant than natural gas moving 3% on a calm day.

A rolling z-score normalizes each price move against the recent distribution of moves for that specific commodity. A z-score of 2.0 means the current move is two standard deviations above the recent mean — statistically unusual regardless of the commodity's absolute volatility level. The lookback period (20 periods) provides enough history to establish a meaningful baseline without being so long that the baseline goes stale during a volatile session.

The tradeoff is that z-scores are slow to adapt to regime changes — a sudden sustained volatility spike will cause the z-score to alert repeatedly until the lookback window absorbs the new regime. In production this would be addressed with adaptive lookback windows or GARCH-based volatility forecasting.

---

## Why keyword-based sentiment rather than an NLP model?

NewsAPI's free tier provides headlines but not article bodies. Sentiment classification from headlines alone is a limited task — headlines are short, often ambiguous, and written for engagement rather than semantic clarity. A sophisticated NLP model on 10-word headlines provides marginal improvement over a well-tuned keyword list, while adding significant complexity (model weights, inference latency, GPU dependency).

The keyword approach is transparent, auditable, and easy to extend. The bullish/bearish keyword lists are domain-specific (OPEC cuts, inventory draws, supply disruptions are reliably bullish signals for crude oil) rather than generic. The sentiment score is used as one of five context signals in the impact classifier — a noisy signal is still informative when combined with cleaner signals like inventory surprises and weather.

In production, this would be replaced with FinBERT (a BERT model fine-tuned on financial news) running as a separate inference service. The pipeline architecture would not change — only the scoring function inside the news producer.

---

## Why VWAP rather than a simple price average for OHLCV?

Volume-weighted average price is the standard reference price in professional trading. It weights each price by the volume traded at that price, giving more weight to prices where significant volume changed hands. In equity markets, VWAP is used as a benchmark for execution quality. In energy futures, it is the primary intraday reference price for desk traders.

A simple average of price ticks weights a tick at 10:00:01 identically to a tick at 10:00:59, regardless of whether one represented 10,000 contracts and the other represented 100. VWAP corrects for this by computing `sum(price × volume) / total_volume` across the window, which is computed incrementally as ticks arrive rather than in a batch at window close.

The current volume data from yfinance is three-month average volume rather than per-tick volume. This is a known limitation disclosed in comments — it means VWAP degrades toward a simple average since volume is roughly constant across ticks. Real tick data includes per-trade volume, making VWAP meaningful.

---

## Why a separate sink consumer rather than writing to Postgres inside each consumer?

Each analytical consumer (enrichment, anomaly, impact, OHLCV) does one thing: compute and publish. Writing to Postgres inside these consumers would introduce database latency into the streaming hot path and couple the analytical logic to the persistence layer.

The sink consumer subscribes to all validated and derived topics and handles all Postgres writes. This separation means:

- Each analytical consumer can be scaled or restarted independently without affecting persistence
- Database connection pooling is managed in one place
- If Postgres is temporarily unavailable, only the sink consumer falls behind — the rest of the pipeline continues processing
- The sink consumer can be replaced with a different persistence layer (DynamoDB, BigQuery) without touching any analytical consumer

The tradeoff is eventual consistency between Kafka and Postgres — there is a small lag between an event being published and it appearing in the database. For this use case (analytical queries, not transactional operations) that lag is acceptable.

---

## What the pipeline does not capture

**Order book data:** The bid/ask spread, order book depth, and trade-level data that professional energy traders use for execution decisions. This requires a direct market data feed from an exchange (CME, ICE) and is not available via free APIs.

**Cross-commodity correlations:** WTI and Brent are highly correlated but diverge on specific events (US export restrictions, North Sea disruptions). Natural gas and crude oil correlate on energy demand signals but diverge on supply. The current pipeline treats each commodity independently — a production system would model cross-commodity spread dynamics as a separate stream.

**Seasonal adjustments:** Natural gas demand is strongly seasonal (heating in winter, cooling in summer). The HDD/CDD signals capture this partially, but the anomaly detector does not adjust its baseline for seasonality. A price move that is anomalous in July may be entirely normal in January. Seasonal adjustment of the z-score baseline would reduce false positive alerts by roughly 30-40%.

**Position and flow data:** Commitment of Traders (COT) reports from the CFTC show whether large speculators are net long or short. Extreme positioning is a well-documented mean-reversion signal in energy markets. COT data is published weekly and is freely available — adding it as a sixth FRED stream would be straightforward within the existing architecture.
