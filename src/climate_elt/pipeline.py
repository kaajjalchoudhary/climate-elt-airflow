"""Run the pipeline without Airflow, e.g. for local development or backfills.

python -m climate_elt.pipeline --date 2024-06-01
python -m climate_elt.pipeline --date 2024-06-01 --days 7      # backfill a week
"""

import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

import duckdb

from climate_elt import config
from climate_elt.extract import Fetch, extract_city, fetch_json
from climate_elt.load import load_day
from climate_elt.quality import assert_quality, run_checks
from climate_elt.transform import build_daily_metrics

log = logging.getLogger("climate_elt")


def run_day(
    day: str,
    base_dir: Path | None = None,
    db_path: Path | None = None,
    cities: list[dict] | None = None,
    fetch: Fetch = fetch_json,
) -> dict[str, int]:
    base_dir = Path(base_dir or config.data_dir())
    db_path = Path(db_path or config.warehouse_path())
    cities = cities or config.CITIES

    for city in cities:
        extract_city(city, day, base_dir, fetch)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        counts = load_day(con, base_dir, day)
        counts["daily_city_metrics"] = build_daily_metrics(con, day)
        results = run_checks(con, day, [c["slug"] for c in cities])
    for r in results:
        log.info("[%s] %s: %s", "PASS" if r.passed else "FAIL", r.name, r.details)
    assert_quality(results)
    log.info("%s done: %s", day, counts)
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Climate ELT pipeline")
    p.add_argument("--date", required=True, help="first day to process (YYYY-MM-DD)")
    p.add_argument("--days", type=int, default=1, help="number of consecutive days")
    a = p.parse_args()
    start = date.fromisoformat(a.date)
    for i in range(a.days):
        run_day((start + timedelta(days=i)).isoformat())


if __name__ == "__main__":
    main()
