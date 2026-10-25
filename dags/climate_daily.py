"""
### climate_daily

Daily ELT of hourly weather and air-quality observations for six cities from the
[Open-Meteo](https://open-meteo.com/) archive and air-quality APIs.

1. **extract_city** (dynamically mapped, one task per city) lands raw JSON per source/day/city.
2. **load_raw** replaces that day's rows in the DuckDB raw tables.
3. **build_metrics** rebuilds the `daily_city_metrics` mart for the day.
4. **check_quality** fails the run if any data quality check fails.

Each run processes the day `DATA_LAG_DAYS` before its logical date, because the reanalysis archive
is published with a delay. Every step is idempotent, so retries and backfills are safe.
"""

from __future__ import annotations

from datetime import date, timedelta

import duckdb
import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

from climate_elt import config
from climate_elt.extract import extract_city as extract_city_fn
from climate_elt.load import load_day
from climate_elt.quality import assert_quality, run_checks
from climate_elt.transform import build_daily_metrics


def target_day(ds: str) -> str:
    return (date.fromisoformat(ds) - timedelta(days=config.DATA_LAG_DAYS)).isoformat()


@dag(
    dag_id="climate_daily",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=5)},
    tags=["elt", "open-meteo", "duckdb"],
    doc_md=__doc__,
)
def climate_daily():
    @task
    def list_cities() -> list[dict]:
        return config.CITIES

    @task(max_active_tis_per_dag=4)
    def extract_city(city: dict, ds: str | None = None) -> list[str]:
        return extract_city_fn(city, target_day(ds), config.data_dir())

    @task
    def load_raw(_paths, ds: str | None = None) -> dict:
        with duckdb.connect(str(config.warehouse_path())) as con:
            return load_day(con, config.data_dir(), target_day(ds))

    @task
    def build_metrics(_counts, ds: str | None = None) -> int:
        with duckdb.connect(str(config.warehouse_path())) as con:
            return build_daily_metrics(con, target_day(ds))

    @task
    def check_quality(_rows, ds: str | None = None) -> None:
        day = target_day(ds)
        with duckdb.connect(str(config.warehouse_path())) as con:
            results = run_checks(con, day, [c["slug"] for c in config.CITIES])
        for r in results:
            print(f"[{'PASS' if r.passed else 'FAIL'}] {r.name}: {r.details}")
        assert_quality(results)

    paths = extract_city.expand(city=list_cities())
    check_quality(build_metrics(load_raw(paths)))


climate_daily()
