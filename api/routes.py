# api/routes.py

from fastapi import APIRouter, HTTPException, Query
import psycopg2.extras

from api.db import get_db_connection
from api.redis_client import get_redis

router = APIRouter()
COMMODITIES = ["WTI", "BRENT", "NATURAL_GAS"]

@router.get("/prices/latest")
def get_latest_prices():
    """Live prices for all commodities - reads from Redis, sub-millisecond latency"""
    r = get_redis()
    result = {}
    for commodity in COMMODITIES:
        price_data = r.hgetall(f"price: {commodity}")
        context = r.hgetall(f"context: {commodity}")
        if price_data:
            result[commodity] = {
                "price": float(price_data.get("price", 0)),
                "price_change_pct": float(price_data.get("price_change_pct", 0)),
                "session_open": float(price_data.get("session_open", 0)),
                "last_updated": price_data.get("last_updated"),
                "weather_temp_f": float(context.get("weather_temp_f", 0) or 0),
                "weather_conditions": context.get("weather_conditions", ""),
                "active_disruptions": int(context.get("active_disruptions", 0) or 0),
                "sentiment": context.get("latest_sentiment", "NEUTRAL"),
                "latest_headline": context.get("latest_headline", ""),
            }
    return {"prices": result, "source": "redis"}

@router.get("/prices/history/{commodity}")
def get_price_history(
    commodity: str,
    limit: int = Query(default=100, le=1000),
):
    """Recent price ticks from Postgres"""
    if commodity not in COMMODITIES:
        raise HTTPException(status_code=400, detail=f"Unknown commodity. Choose from {COMMODITIES}")
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT commodity, price, session_open, event_ts
                FROM events.price_ticks
                WHERE commodity = %s
                ORDER BY event_ts DESC
                LIMIT %s
                """,
                (commodity, limit),
            )
            rows = cur.fetchall()
        return {"commodity": commodity, "ticks": [dict(r) for r in rows]}
    finally:
        conn.close()

@router.get("/prices/ohlcv/{commodity}")
def get_ohlcv(
    commodity: str,
    limit: int = Query(default=60, le=1440),  # default 1 hour of 1-min candles
):
    """1-minute OHLCV candles from Postgres."""
    if commodity not in COMMODITIES:
        raise HTTPException(status_code=400, detail=f"Unknown commodity. Choose from {COMMODITIES}")
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    commodity,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    vwap,
                    window_start,
                    window_end
                FROM analytics.ohlcv_1min
                WHERE commodity = %s
                ORDER BY window_start DESC
                LIMIT %s
                """,
                (commodity, limit),
            )
            rows = cur.fetchall()
        return {"commodity": commodity, "candles": [dict(r) for r in rows]}
    finally:
        conn.close()

@router.get("/context/{commodity}")
def get_market_context(commodity: str):
    """Full market context for a commodity - reads from Redis"""
    if commodity not in COMMODITIES:
        raise HTTPException(status_code=400, detail=f"Unkown commodity. Choose from {COMMODITIES}")
    r = get_redis()
    context = r.hgetall(f"context:{commodity}")
    if not context:
        raise HTTPException(status_code=404, detail="No context available yet")
    return {"commodity": commodity, "context": context}

@router.get("/alerts/anomalies")
def get_anomalies(limit: int = Query(default=20, le=100)):
    """Recent anomaly alerts with context snapshots"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    commodity,
                    price,
                    z_score,
                    price_change_pct,
                    context_snapshot,
                    event_ts
                FROM analytics.anomalies
                ORDER BY event_ts DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return {"anomalies": [dict(r) for r in rows]}
    finally:
        conn.close()

@router.get("/alerts/impacts")
def get_impacts(limit: int = Query(default=20, le=100)):
    """Recent impact-tagged events — anomalies with driver classification."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    commodity,
                    price,
                    z_score,
                    likely_driver,
                    driver_confidence,
                    supporting_factors,
                    event_ts
                FROM analytics.impact_events
                ORDER BY event_ts DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return {"impacts": [dict(r) for r in rows]}
    finally:
        conn.close()

@router.get("/pipeline/health")
def pipeline_health():
    """Data freshness and row counts across all tables."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    'price_ticks' AS table_name,
                    COUNT(*) AS row_count,
                    MAX(event_ts) AS latest_event
                FROM events.price_ticks
                UNION ALL
                SELECT 'fred_reports', COUNT(*), MAX(event_ts) FROM events.fred_reports
                UNION ALL
                SELECT 'weather_obs', COUNT(*), MAX(event_ts) FROM events.weather_observations
                UNION ALL
                SELECT 'news_events', COUNT(*), MAX(fetched_at) FROM events.news_events
                UNION ALL
                SELECT 'shipments', COUNT(*), MAX(event_ts) FROM events.shipment_events
                UNION ALL
                SELECT 'anomalies', COUNT(*), MAX(event_ts) FROM analytics.anomalies
                UNION ALL
                SELECT 'impacts', COUNT(*), MAX(event_ts) FROM analytics.impact_events
                UNION ALL
                SELECT 'dead_letter', COUNT(*), MAX(received_at) FROM raw.dead_letter
            """)
            rows = cur.fetchall()
        return {"tables": [dict(r) for r in rows]}
    finally:
        conn.close()