# config/settings.py

import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # api keys
    FRED_API_KEYS: str = os.getenv("FRED_API_KEY", "")
    OPENWEATHER_API_KEYS: str = os.getenv("OPENWEATHER_API_KEY", "")
    NEWS_API_KEY: str = os.getenv("NEWS_API_KEY", "")

    # kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # postgres
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5434")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "energy_db")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "energy")
    POSTGRES_PASS: str = os.getenv("POSTGRES_PASSWORD", "energy_secret")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASS}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )
    
    # redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # pipeline tuning
    PRICE_POLL_INTERVAL_SECS: int = 30
    WEATHER_POLL_INTERVAL_SECS: int = 900 # 15min
    NEWS_POLL_INTERVAL_SECS: int = 300 # 5min
    FRED_POLL_INTERVAL_SECS: int = 3600 # 1hr
    ANOMALY_Z_SCORE_THRESHOLD: float = 2.0
    ANOMALY_LOOKBACK_PERIODS: int = 20
    PRICE_ALERT_WINDOW_MINS: int = 5
    PRICE_ALERT_THRESHOLD_PCT: float = 1.0

    # monitoring stations
    WEATHER_STATIONS: list[dict] = [
        {"name": "Henry Hub", "city": "Erath", "state": "LA", "commodity": "NATURAL_GAS", "lat": 29.96, "lon": -91.87},
        {"name": "Cushing Hub", "city": "Cushing", "state": "OK", "commodity": "WTI", "lat": 35.98, "lon": -96.77},
        {"name": "Houston Ship Channel", "city": "Houston", "state": "TX", "commodity": "BRENT", "lat": 29.76, "lon": -95.37},
    ]

    # FRED series
    FRED_SERIES: list[dict] = [
        {
            "id": "DCOILWTICO",
            "name": "WTI Crude Oil Spot Price",
            "commodity": "WTI",
            "units": "dollars_per_barrel",
            "frequency": "daily",
        },
        {
            "id": "DCOILBRENTEU",
            "name": "Brent Crude Oil Spot Price",
            "commodity": "BRENT",
            "units": "dollars_per_barrel",
            "frequency": "daily",
        },
        {
            "id": "DHHNGSP",
            "name": "Henry Hub Natural Gas Spot Price",
            "commodity": "NATURAL_GAS",
            "units": "dollars_per_mmbtu",
            "frequency": "daily",
        },
        {
            "id": "NATURALGAS",
            "name": "US Natural Gas in Storage",
            "commodity": "NATURAL_GAS",
            "units": "billion_cubic_feet",
            "frequency": "monthly",
        },
    ]

    COMMODITY_TICKERS: dict = {
        "WTI": "CL=F",
        "BRENT": "BZ=F",
        "NATURAL_GAS": "NG=F",
    }

settings = Settings()