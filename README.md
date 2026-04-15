# Energy Market Intelligence Pipeline

A real-time streaming data pipeline that ingests five concurrent energy market data streams, correlates them, detects statistically anomalous price movements, and classifies each anomaly by its likely fundamental driver — served via a live REST API.

Built as a portfolio project to demonstrate streaming data engineering competencies: multi-topic Kafka architecture, stream enrichment via Redis state, schema validation at stream boundaries, windowed aggregations, and a real-time serving layer.

---

## The Problem This Solves

Energy prices don't move randomly. WTI crude spikes when the EIA reports an unexpected inventory draw. Natural gas surges when a cold front hits the Henry Hub delivery point. Brent moves on tanker congestion at key export ports. A price move without context is noise — a price move with context is a signal.

This pipeline ingests price ticks alongside the fundamental factors that drive them, fuses them in real time, and produces enriched events that carry both the price movement and the market context that explains it. Anomalies are automatically classified by their most likely driver.

---

## Architecture

```text
┌─────────────────────────────── PRODUCERS ───────────────────────────────┐
│                                                                          │
│  price_producer     fred_producer    weather_producer  shipment_producer │
│  yfinance · 30s     FRED API · 1hr   OpenWeather · 15m  synthetic · 30m  │
│       │                  │                 │                  │          │
│  news_producer           │                 │                  │          │
│  NewsAPI · 5m            │                 │                  │          │
└────────┬─────────────────┴─────────────────┴──────────────────┴──────────┘
         │
         ▼
┌─────────────────────────── KAFKA TOPICS ────────────────────────────────┐
│  raw.price_ticks         raw.fred_reports     raw.weather_observations  │
│  raw.shipment_events     raw.news_events                                │
│                                                                         │
│  validated.price_ticks   validated.context_events                       │
│  enriched.price_ticks                                                   │
│  alerts.anomalies        alerts.impact_events                           │
└────────┬────────────────────────────────────────────────────────────────┘
         │
┌────────▼───────────────── CONSUMERS ────────────────────────────────────┐
│  validation_consumer  → Pydantic validation, dead-letters bad events    │
│  context_consumer     → maintains live market context in Redis          │
│  enrichment_consumer  → fuses price ticks with Redis context            │
│  ohlcv_consumer       → computes 1-min OHLCV candles + VWAP             │
│  anomaly_consumer     → rolling z-score anomaly detection               │
│  impact_consumer      → classifies anomalies by fundamental driver      │
│  sink_consumer        → durable writes to Postgres                      │
└────────┬──────────────────────────────┬─────────────────────────────────┘
         │                              │
    PostgreSQL                        Redis
    durable history                   live state (< 1ms reads)
         │
         ▼
      FastAPI
  /prices/latest · /prices/ohlcv · /context · /alerts
```

---

## The Five Data Streams

| Stream | Source | Frequency | What it captures |
| --- | --- | --- | --- |
| Price ticks | yfinance (WTI, Brent, NG futures) | 30 seconds | Real futures prices, simulated tick-by-tick via GBM between polls |
| Inventory reports | FRED API (EIA republications) | Hourly poll | WTI spot, Brent spot, Henry Hub gas price, US gas storage |
| Weather observations | OpenWeatherMap | 15 minutes | Temperature, HDD/CDD at Henry Hub, Cushing OK, Houston Ship Channel |
| Shipment events | Synthetic (disclosed) | ~30 minutes | Tanker departures, pipeline disruptions, refinery outages |
| News sentiment | NewsAPI | 5 minutes | Energy headlines with keyword-based sentiment scoring |

**Note on synthetic shipment data:** Real-time vessel tracking data (AIS) and pipeline SCADA feeds are commercial products. Shipment events are realistically modeled based on actual route economics and infrastructure, and clearly disclosed as synthetic. In production this stream would be replaced with Spire Maritime AIS data or operator API feeds.

---

## Stack

| Layer | Tool | Why |
| --- | --- | --- |
| Event streaming | Apache Kafka 3.7 + Zookeeper | Industry standard, durable log, replay capability |
| Schema validation | Pydantic v2 | Contract enforcement at stream boundaries |
| Live state | Redis 7 | Sub-millisecond context reads for stream enrichment |
| Durable storage | PostgreSQL 15 | ACID guarantees, JSONB for context snapshots |
| Serving layer | FastAPI + uvicorn | Async, auto-docs, production-ready |
| Price seeding | yfinance | Real current futures prices as GBM seed |
| Fundamentals | FRED API | Federal Reserve-hosted EIA data, reliable uptime |
| Weather | OpenWeatherMap | Free tier, three monitoring stations |
| News | NewsAPI | Energy headline stream, keyword sentiment |
| Containerization | Docker Compose | Single-command local stack |

---

## Kafka Topic Design

### Raw topics (immutable, source of truth)

`raw.price_ticks` · `raw.fred_reports` · `raw.weather_observations` · `raw.shipment_events` · `raw.news_events`

Nothing downstream reads these directly. The validation consumer is the only reader. This separation means bad upstream data never reaches analytical models — it lands in the dead letter table instead.

### Validated topics

`validated.price_ticks` — clean price events, confirmed positive prices, timestamp within acceptable bounds.

`validated.context_events` — all non-price streams (FRED, weather, shipments, news) merged into one topic, tagged with `_event_type` for downstream routing. Consumers subscribe to one topic and branch on the tag rather than subscribing to four separate topics.

### Derived topics

`enriched.price_ticks` — price ticks joined with current Redis context. Each event carries both the price movement and the full market context at that moment in time.

`alerts.anomalies` — statistically unusual price moves (rolling z-score |z| ≥ 2.0 over 20 periods).

`alerts.impact_events` — anomalies tagged with their likely fundamental driver and a confidence score.

---

## Stream Enrichment via Redis

The context consumer maintains a live state object in Redis per commodity:

```json
{
  "weather_temp_f": "69.71",
  "weather_hdd": "0",
  "weather_cdd": "4.71",
  "weather_conditions": "clear sky",
  "weather_station": "Cushing Hub",
  "active_disruptions": "1",
  "latest_disruption_type": "PIPELINE_DISRUPTION",
  "sentiment_score_rolling": "-0.3",
  "latest_headline": "OPEC maintains production cut targets..."
}
```

When the enrichment consumer receives a validated price tick, it does a single Redis `HGETALL` — one round trip, typically under 1ms — and attaches the full context to the price event before publishing to `enriched.price_ticks`. This is the core architectural pattern: slow-moving context (weather, news, inventory) is pre-materialized in Redis so fast-moving price events can be enriched without blocking on expensive lookups.

---

## Anomaly Detection

A rolling z-score over 20 price change periods per commodity:

```code
z = (current_change - mean(last_20_changes)) / std(last_20_changes)
```

Alert threshold: |z| ≥ 2.0. At a normal distribution this represents roughly the top/bottom 5% of price moves — statistically unusual but not so rare that alerts are meaningless.

The lookback period (20) and threshold (2.0) are configurable in `config/settings.py`.

---

## Impact Classification

When an anomaly fires, the impact consumer applies a decision tree against the enriched context:

| Context signal | Driver label | Confidence formula |
| --- | --- | --- |
| FRED inventory surprise > 3% | `EIA_INVENTORY_REPORT` | min(0.9, surprise / 10) |
| HDD > 20 | `COLD_WEATHER_DEMAND` | min(0.8, hdd / 50) |
| CDD > 20 | `HOT_WEATHER_DEMAND` | min(0.8, cdd / 50) |
| Active disruptions > 0 | `SUPPLY_DISRUPTION` | min(0.85, disruptions × 0.4) |
| Sentiment score abs > 0.3 | `NEWS_SENTIMENT` | min(0.7, abs_score) |
| No signal | `TECHNICAL_UNKNOWN` | 0.3 |

Multiple signals can be present. The highest-confidence driver wins. All contributing factors are stored in `supporting_factors[]` for auditability.

---

## PostgreSQL Schema

```text
raw schema
  └── dead_letter          ← invalid events from any stream, with rejection reason

events schema
  ├── price_ticks          ← validated price events
  ├── fred_reports         ← FRED/EIA inventory and price data
  ├── weather_observations ← station readings with HDD/CDD
  ├── shipment_events      ← tanker departures and disruption events
  └── news_events          ← headlines with sentiment scores

analytics schema
  ├── ohlcv_1min           ← 1-minute OHLCV candles with VWAP
  ├── anomalies            ← z-score alerts with context snapshots
  └── impact_events        ← driver-classified anomalies
```

---

## API Endpoints

All endpoints served by FastAPI at `http://localhost:8000`.

Interactive documentation at `http://localhost:8000/docs`.

| Endpoint | Source | Description |
| --- | --- | --- |
| `GET /prices/latest` | Redis | Current price + context for all commodities |
| `GET /prices/history/{commodity}` | Postgres | Recent price ticks, configurable limit |
| `GET /prices/ohlcv/{commodity}` | Postgres | 1-minute OHLCV candles, configurable limit |
| `GET /context/{commodity}` | Redis | Full market context for one commodity |
| `GET /alerts/anomalies` | Postgres | Recent anomaly alerts with context snapshots |
| `GET /alerts/impacts` | Postgres | Driver-classified anomaly events |
| `GET /pipeline/health` | Postgres | Row counts and data freshness across all tables |

---

## Local Setup

### Prerequisites

- Docker Desktop (4 CPU, 6 GB RAM minimum)
- Python 3.10+
- uv
- FRED API key — `https://fred.stlouisfed.org/docs/api/api_key.html` (free, instant)
- OpenWeatherMap API key — `https://openweathermap.org/api` (free tier)
- NewsAPI key — `https://newsapi.org` (free tier)

### Step 1 — Clone and configure

```bash
git clone <repo-url>
cd energy-stream-pipeline
cp .env.example .env
# Add your API keys to .env
```

### Step 2 — Start infrastructure

```bash
docker compose up -d
sleep 20
docker compose ps  # all four services should be healthy
```

### Step 3 — Install dependencies

```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

### Step 4 — Start producers

```bash
python -m producers.price_producer &
python -m producers.fred_producer &
python -m producers.weather_producer &
python -m producers.news_producer &
python -m producers.shipment_producer &
```

### Step 5 — Start consumers

```bash
python -m consumers.validation_consumer &
python -m consumers.context_consumer &
python -m consumers.enrichment_consumer &
python -m consumers.ohlcv_consumer &
python -m consumers.anomaly_consumer &
python -m consumers.impact_consumer &
python -m consumers.sink_consumer &
```

### Step 6 — Start API

```bash
uvicorn api.main:app --reload --port 8000
```

Open `http://localhost:8000/docs` to explore the live API.

---

## Project Structure

```text
energy-stream-pipeline/
├── docker-compose.yml
├── .env.example
├── requirements.txt
│
├── config/
│   ├── settings.py          # all configuration, FRED series definitions
│   ├── topics.py            # Kafka topic name constants
│   └── schemas.py           # Pydantic models for all event types
│
├── producers/
│   ├── price_producer.py    # yfinance polling + GBM tick simulation
│   ├── fred_producer.py     # FRED API polling
│   ├── weather_producer.py  # OpenWeatherMap polling
│   ├── news_producer.py     # NewsAPI polling + keyword sentiment
│   └── shipment_producer.py # Synthetic shipment event generation
│
├── consumers/
│   ├── validation_consumer.py   # Pydantic validation, dead-letter
│   ├── context_consumer.py      # Redis context state management
│   ├── enrichment_consumer.py   # Price + context fusion
│   ├── ohlcv_consumer.py        # 1-minute OHLCV + VWAP computation
│   ├── anomaly_consumer.py      # Rolling z-score anomaly detection
│   ├── impact_consumer.py       # Driver classification
│   └── sink_consumer.py         # Postgres writer for all streams
│
├── api/
│   ├── main.py              # FastAPI app
│   ├── routes.py            # All endpoints
│   ├── db.py                # Postgres connection
│   └── redis_client.py      # Redis connection
│
└── sql/
    └── init.sql             # Schema creation
```

---

## Design Decisions

See [DESIGN.md](./DESIGN.md) for a detailed write-up of architectural choices, tradeoffs, and what would change at production scale.

---

## What I'd Do Differently at Scale

**Kafka:** Add schema registry (Confluent Schema Registry or AWS Glitch). Right now schemas are enforced by Pydantic at the consumer — valid but fragile across teams. A schema registry enforces contracts at the broker level.

**Consumers:** Move from Python threads to separate deployable services, one per consumer group. Each consumer would be a containerized microservice with independent scaling. At high tick volume, the enrichment consumer becomes the bottleneck — it could be horizontally scaled by adding partitions to `validated.price_ticks` and running multiple consumer instances.

**State:** Replace in-memory OHLCV window state with Redis sorted sets. The current in-memory approach loses state on consumer restart — acceptable for a local project, not for production. Redis persistence with AOF would make the OHLCV computation crash-safe.

**Sentiment:** Replace keyword matching with a fine-tuned NLP model (FinBERT or similar). Keyword sentiment is directionally useful but misses sarcasm, negation, and domain nuance. The pipeline architecture doesn't change — just the scoring function inside the news producer.

**Monitoring:** Add Prometheus metrics export from each consumer (lag per topic, error rate, throughput) and a Grafana dashboard. Consumer lag is the most important operational metric for a streaming pipeline — if the enrichment consumer falls behind, enriched events carry stale context.
