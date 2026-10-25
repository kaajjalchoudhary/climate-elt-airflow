import duckdb
import pytest

from climate_elt.config import WEATHER_VARS
from climate_elt.extract import extract_city
from climate_elt.load import load_day, parse_hourly
from climate_elt.transform import build_daily_metrics
from conftest import FakeOpenMeteo, make_payload

DAY = "2024-06-01"


def _value(var, hour):
    if var == "temperature_2m":
        return 20.0 + hour  # 20..43, avg 31.5
    if var == "precipitation":
        return 0.5 if hour < 4 else 0.0
    if var == "pm2_5":
        return 10.0 if hour < 12 else 30.0  # 12 hours above the WHO guideline, avg 20
    return 1.0


@pytest.fixture
def con():
    with duckdb.connect(":memory:") as c:
        yield c


@pytest.fixture
def landed(tmp_path, cities):
    fetch = FakeOpenMeteo(_value)
    for city in cities:
        extract_city(city, DAY, tmp_path, fetch)
    return tmp_path


def test_parse_hourly_rejects_ragged_arrays():
    payload = make_payload(DAY, WEATHER_VARS)
    payload["hourly"]["precipitation"] = payload["hourly"]["precipitation"][:-1]
    payload["_meta"] = {"city": "x", "extracted_at": "2024-06-02T00:00:00+00:00"}
    with pytest.raises(ValueError, match="precipitation"):
        parse_hourly(payload, WEATHER_VARS)


def test_load_day_is_idempotent(con, landed):
    first = load_day(con, landed, DAY)
    second = load_day(con, landed, DAY)
    assert first == second == {"raw_weather_hourly": 48, "raw_air_quality_hourly": 48}
    assert con.execute("SELECT count(*) FROM raw_weather_hourly").fetchone()[0] == 48


def test_build_daily_metrics(con, landed):
    load_day(con, landed, DAY)
    assert build_daily_metrics(con, DAY) == 2
    assert build_daily_metrics(con, DAY) == 2  # re-run replaces, never duplicates

    row = con.execute(
        "SELECT avg_temp_c, min_temp_c, max_temp_c, total_precip_mm, avg_pm2_5, "
        "hours_pm25_above_who, pm25_aqi_category FROM daily_city_metrics WHERE city = 'alpha'"
    ).fetchone()
    assert row == (31.5, 20.0, 43.0, 2.0, 20.0, 12, "moderate")
