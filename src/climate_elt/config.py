"""Pipeline configuration. Paths can be overridden with environment variables."""

import os
from pathlib import Path

CITIES = [
    {"slug": "dubai", "name": "Dubai", "country": "AE", "lat": 25.2048, "lon": 55.2708},
    {"slug": "delhi", "name": "Delhi", "country": "IN", "lat": 28.6139, "lon": 77.2090},
    {
        "slug": "london",
        "name": "London",
        "country": "GB",
        "lat": 51.5072,
        "lon": -0.1276,
    },
    {
        "slug": "new_york",
        "name": "New York",
        "country": "US",
        "lat": 40.7128,
        "lon": -74.0060,
    },
    {
        "slug": "singapore",
        "name": "Singapore",
        "country": "SG",
        "lat": 1.3521,
        "lon": 103.8198,
    },
    {
        "slug": "sydney",
        "name": "Sydney",
        "country": "AU",
        "lat": -33.8688,
        "lon": 151.2093,
    },
]

WEATHER_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
]
AIR_VARS = ["pm2_5", "pm10", "nitrogen_dioxide", "ozone"]

SOURCES = {
    "weather": {
        "url": "https://archive-api.open-meteo.com/v1/archive",
        "variables": WEATHER_VARS,
    },
    "air_quality": {
        "url": "https://air-quality-api.open-meteo.com/v1/air-quality",
        "variables": AIR_VARS,
    },
}

# The ERA5-based archive is published with a delay, so a run processes data from a week earlier.
DATA_LAG_DAYS = 7


def data_dir() -> Path:
    return Path(os.getenv("CLIMATE_DATA_DIR", "data"))


def warehouse_path() -> Path:
    return Path(os.getenv("CLIMATE_DB_PATH", str(data_dir() / "warehouse.duckdb")))
