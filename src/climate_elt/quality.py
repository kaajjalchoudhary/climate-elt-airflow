"""Data quality checks for one processed day. Failing checks fail the Airflow task."""

from collections.abc import Iterable
from dataclasses import dataclass

import duckdb


class DataQualityError(Exception):
    pass


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str


def _scalar(con, sql: str, params: list):
    return con.execute(sql, params).fetchone()[0]


def run_checks(
    con: duckdb.DuckDBPyConnection, day: str, cities: Iterable[str]
) -> list[CheckResult]:
    cities = sorted(cities)
    results = []

    for table in ("raw_weather_hourly", "raw_air_quality_hourly"):
        rows = con.execute(
            f"SELECT city, count(*) FROM {table} WHERE CAST(ts AS DATE) = CAST(? AS DATE) "
            "GROUP BY city",
            [day],
        ).fetchall()
        per_city = dict(rows)
        bad = {c: per_city.get(c, 0) for c in cities if per_city.get(c, 0) != 24}
        results.append(
            CheckResult(
                f"{table}:24_hours_per_city",
                not bad,
                f"incomplete: {bad}" if bad else "ok",
            )
        )

    metric_cities = {
        r[0]
        for r in con.execute(
            "SELECT city FROM daily_city_metrics WHERE date = CAST(? AS DATE)", [day]
        ).fetchall()
    }
    missing = sorted(set(cities) - metric_cities)
    results.append(
        CheckResult(
            "daily_city_metrics:all_cities",
            not missing,
            f"missing: {missing}" if missing else "ok",
        )
    )

    out_of_range = _scalar(
        con,
        "SELECT count(*) FROM raw_weather_hourly WHERE CAST(ts AS DATE) = CAST(? AS DATE) "
        "AND (temperature_2m < -60 OR temperature_2m > 60)",
        [day],
    )
    results.append(
        CheckResult("temperature_in_range", out_of_range == 0, f"{out_of_range} rows outside")
    )

    negative_pm = _scalar(
        con,
        "SELECT count(*) FROM raw_air_quality_hourly WHERE CAST(ts AS DATE) = CAST(? AS DATE) "
        "AND (pm2_5 < 0 OR pm10 < 0)",
        [day],
    )
    results.append(CheckResult("pm_non_negative", negative_pm == 0, f"{negative_pm} negative"))

    null_temp_ratio = _scalar(
        con,
        "SELECT coalesce(avg(CASE WHEN temperature_2m IS NULL THEN 1.0 ELSE 0.0 END), 1.0) "
        "FROM raw_weather_hourly WHERE CAST(ts AS DATE) = CAST(? AS DATE)",
        [day],
    )
    results.append(
        CheckResult(
            "temperature_null_ratio_below_10pct",
            null_temp_ratio < 0.1,
            f"{null_temp_ratio:.1%} null",
        )
    )
    return results


def assert_quality(results: list[CheckResult]) -> None:
    failed = [r for r in results if not r.passed]
    if failed:
        raise DataQualityError("; ".join(f"{r.name}: {r.details}" for r in failed))
