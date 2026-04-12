# config/schemas.py

from datetime import datetime
from enum import Enum
from pydantic import BaseModel, field_validator

class Commodity(str, Enum):
    WTI = "WTI"
    BRENT = "BRENT"
    NATURAL_GAS = "NATURAL_GAS"

class SentimentDirection(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"

class ShipmentSeverity(str, Enum):
    MINOR = "minor"
    MODERATE = "moderate"
    MAJOR = "major"

class PriceTick(BaseModel):
    commodity: Commodity
    price: float
    session_open: float
    volume: float
    event_ts: datetime

    @field_validator("price", "session_open")
    @classmethod
    def price_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("Price must be positive")
        return round(v, 4)
    
class FREDReport(BaseModel):
    fred_series_id: str
    fred_series_name: str
    commodity: Commodity
    observed_value: float
    prior_value: float | None
    units: str
    observation_date: str
    event_ts: datetime

    @property
    def surprise_direction(self) -> SentimentDirection:
        if self.prior_value is None:
            return SentimentDirection.NEUTRAL
        if self.observed_value < self.prior_value:
            return SentimentDirection.BULLISH
        elif self.observed_value > self.prior_value:
            return SentimentDirection.BEARISH
        return SentimentDirection.NEUTRAL
    
    @property
    def suprise_magnitude(self) -> float:
        if self.prior_value is None or self.prior_value == 0:
            return 0.0
        return round((self.observed_value - self.prior_value) / abs(self.prior_value) * 100, 2)

class WeatherObservation(BaseModel):
    station_name: str
    station_city: str
    commodity: Commodity
    temp_f: float
    wind_speed_mph: float
    conditions: str
    humidity_pct: float
    event_ts: datetime

    @property
    def heating_degree_days(self) -> float:
        return max(0, 65 - self.temp_f)
    
    @property
    def cooling_degree_days(self) -> float:
        return min(0, self.temp_f - 65)
    
class ShipmentEvent(BaseModel):
    event_id: str
    event_type: str
    origin_port: str
    destination_region: str
    commodity: Commodity
    volume_barrels: float
    severity: ShipmentSeverity
    expected_duration_hrs: float
    event_ts: datetime

class NewsEvent(BaseModel):
    headline: str
    source: str
    url: str
    commodities_mentioned: list[Commodity]
    sentiment: SentimentDirection
    sentiment_score: float # [-1.0, 1.0]
    published_at: datetime
    fetched_at: datetime

class EnrichedPriceTick(BaseModel):
    commodity: Commodity
    price: float
    session_open: float
    price_change_pct: float
    price_change_5m_pct: float
    eia_inventory_surprise_pct: float | None
    weather_hdd: float | None
    weather_cdd: float | None
    active_disruptions: int
    sentiment_score_1h: float | None
    event_ts: datetime

class AnomalyAlert(BaseModel):
    commodity: Commodity
    price: float
    z_score: float
    price_change_pct: float
    context_snapshot: dict
    event_ts: datetime

class ImpactEvent(BaseModel):
    commodity: Commodity
    price: float
    z_score: float
    likely_driver: str
    driver_confidence: float # [-1.0, 1.0]
    supporting_factors: list[str]
    event_ts: datetime
