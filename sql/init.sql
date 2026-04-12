-- sql/init.sql

-- schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS events;
CREATE SCHEMA IF NOT EXISTS analytics;

-- dead letter table
CREATE TABLE IF NOT EXISTS raw.dead_letter (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(100),
    raw_payload JSONB,
    error_message TEXT,
    received_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- raw events (validated, durable)
CREATE TABLE IF NOT EXISTS events.price_ticks (
    id BIGSERIAL PRIMARY KEY,
    commodity VARCHAR(20),
    price NUMERIC(12, 4),
    session_open NUMERIC(12, 4),
    volume NUMERIC(20, 2),
    event_ts TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events.fred_reports (
    id BIGSERIAL PRIMARY KEY,
    fred_series_id VARCHAR(50),
    fred_series_name VARCHAR(200),
    commodity VARCHAR(20),
    observed_value NUMERIC(20, 4),
    prior_value NUMERIC(20, 4),
    units VARCHAR(50),
    observation_date VARCHAR(20),
    event_ts TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events.weather_observations(
    id BIGSERIAL PRIMARY KEY,
    station_name VARCHAR(100),
    station_city VARCHAR(100),
    commodity VARCHAR(20),
    temp_f NUMERIC(6, 2),
    wind_speed_mph NUMERIC(6, 2),
    conditions VARCHAR(100),
    humidity_pct NUMERIC(5, 2),
    hdd NUMERIC(6, 2),
    cdd NUMERIC(6, 2),
    event_ts TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events.shipment_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(50) UNIQUE,
    event_type VARCHAR(50),
    origin_port VARCHAR(100),
    destination_region VARCHAR(100),
    commodity VARCHAR(20),
    volume_barrels NUMERIC(20, 2),
    severity VARCHAR(20),
    expected_duration_hrs NUMERIC(8, 2),
    event_ts TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events.news_events (
    id BIGSERIAL PRIMARY KEY,
    headline TEXT,
    source VARCHAR(20),
    article_url TEXT,
    commodities_mentioned VARCHAR(20)[],
    sentiment VARCHAR(20),
    sentiment_score NUMERIC(4, 3),
    published_at TIMESTAMP WITH TIME ZONE,
    fetched_at TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- analytics table
CREATE TABLE IF NOT EXISTS analytics.ohlcv_1min (
    id BIGSERIAL PRIMARY KEY,
    commodity VARCHAR(20),
    price_open NUMERIC(12, 4),
    price_high NUMERIC(12, 4),
    price_low NUMERIC(12, 4),
    price_close NUMERIC(12, 4),
    volume NUMERIC(20, 2),
    vwap NUMERIC(12, 4),
    window_start TIMESTAMP WITH TIME ZONE,
    window_end TIMESTAMP WITH TIME ZONE,
    UNIQUE (commodity, window_start)
);

CREATE TABLE IF NOT EXISTS analytics.anomalies (
    id BIGSERIAL PRIMARY KEY,
    commodity VARCHAR(20),
    price NUMERIC(12, 4),
    z_score NUMERIC(8, 4),
    price_change_pct NUMERIC(8, 4),
    context_snapshot JSONB,
    event_ts TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.impact_events (
    id BIGSERIAL PRIMARY KEY,
    commodity VARCHAR(20),
    price NUMERIC(12, 4),
    z_score NUMERIC(8, 4),
    likely_driver VARCHAR(100),
    driver_confidence NUMERIC(4, 3),
    supporting_factors TEXT[],
    event_ts TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_price_ticks_commodity_ts
    ON events.price_ticks (commodity, event_ts DESC);
    
CREATE INDEX IF NOT EXISTS idx_ohlcv_commodity_window
    ON analytics.ohlcv_1min (commodity, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_anomalies_commodity_ts
    ON analytics.anomalies (commodity, event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_impact_events_commodity_ts
    ON analytics.impact_events (commodity, event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_dead_letter_topic
    ON raw.dead_letter (topic, received_at DESC);
