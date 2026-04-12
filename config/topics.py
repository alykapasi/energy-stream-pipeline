# config/topics.py

class Topics:
    # raw - unvalidated, source of truth
    RAW_PRICE_TICKS = "raw.price_ticks"
    RAW_EIA_REPORTS = "raw.fred_reports"
    RAW_WEATHER = "raw.weather_observations"
    RAW_SHIPMENTS = "raw.shipment_events"
    RAW_NEWS = "raw.news_events"

    # validated - passed schema checks
    VALIDATED_PRICES = "validated.price_ticks"
    VALIDATED_CONTEXT = "validated.context_events"

    # enriched - prices fused with context
    ENRICHED_PRICES = "enriched.price_ticks"

    #alerts
    ALERT_ANOMALIES = "alerts.anomalies"
    ALERT_IMPACTS = "alerts.impact_events"

