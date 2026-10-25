import duckdb
import pytest

from climate_elt.load import load_day
from climate_elt.pipeline import run_day
from climate_elt.quality import DataQualityError, assert_quality, run_checks
from climate_elt.transform import build_daily_metrics
from conftest import FakeOpenMeteo

DAY = "2024-06-01"


def test_run_day_end_to_end(tmp_path, cities, fake_fetch):
    db = tmp_path / "wh.duckdb"
    counts = run_day(DAY, tmp_path, db, cities, fake_fetch)
    assert counts == {
        "raw_weather_hourly": 48,
        "raw_air_quality_hourly": 48,
        "daily_city_metrics": 2,
    }
    with duckdb.connect(str(db)) as con:
        assert con.execute("SELECT count(DISTINCT city) FROM daily_city_metrics").fetchone()[0] == 2


def test_quality_fails_on_missing_city(tmp_path, cities, fake_fetch):
    run_day(DAY, tmp_path, tmp_path / "wh.duckdb", cities[:1], fake_fetch)
    with duckdb.connect(str(tmp_path / "wh.duckdb")) as con:
        results = run_checks(con, DAY, [c["slug"] for c in cities])
    with pytest.raises(DataQualityError, match="beta"):
        assert_quality(results)


def test_quality_catches_impossible_temperatures(tmp_path, cities):
    hot = FakeOpenMeteo(lambda var, hour: 99.0 if var == "temperature_2m" else 1.0)
    with pytest.raises(DataQualityError, match="temperature_in_range"):
        run_day(DAY, tmp_path, tmp_path / "wh.duckdb", cities, hot)


def test_quality_catches_mostly_null_temperatures(tmp_path, cities):
    from climate_elt.extract import extract_city

    gaps = FakeOpenMeteo(lambda var, hour: None if var == "temperature_2m" else 1.0)
    for city in cities:
        extract_city(city, DAY, tmp_path, gaps)
    with duckdb.connect(":memory:") as con:
        load_day(con, tmp_path, DAY)
        build_daily_metrics(con, DAY)
        names = {r.name for r in run_checks(con, DAY, ["alpha", "beta"]) if not r.passed}
    assert names == {"temperature_null_ratio_below_10pct"}


def test_quality_catches_duplicate_rows(tmp_path, cities, fake_fetch):
    run_day(DAY, tmp_path, tmp_path / "wh.duckdb", cities, fake_fetch)
    with duckdb.connect(str(tmp_path / "wh.duckdb")) as con:
        con.execute("INSERT INTO raw_weather_hourly SELECT * FROM raw_weather_hourly LIMIT 1")
        failed = {r.name for r in run_checks(con, DAY, ["alpha", "beta"]) if not r.passed}
    assert "raw_weather_hourly:unique_key" in failed
