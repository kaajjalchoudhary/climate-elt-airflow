"""Print the mart for a day (or the latest day loaded).

python -m climate_elt.report [--date 2024-06-01] [--min-cities 6]
"""

import argparse
import sys
from datetime import date

import duckdb

from climate_elt import config

COLUMNS = (
    "city, avg_temp_c, max_temp_c, total_precip_mm, avg_pm2_5, "
    "hours_pm25_above_who, pm25_aqi_category"
)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--date")
    p.add_argument("--min-cities", type=int, default=0)
    a = p.parse_args()

    with duckdb.connect(str(config.warehouse_path()), read_only=True) as con:
        day = a.date or con.execute("SELECT max(date) FROM daily_city_metrics").fetchone()[0]
        day = date.fromisoformat(str(day)).isoformat()  # validates the input
        rel = con.sql(
            f"SELECT {COLUMNS} FROM daily_city_metrics WHERE date = DATE '{day}' "
            "ORDER BY avg_pm2_5 DESC"
        )
        print(f"daily_city_metrics for {day}")
        print(rel)
        n = len(rel.fetchall())
    if n < a.min_cities:
        sys.exit(f"Expected at least {a.min_cities} cities, found {n}")


if __name__ == "__main__":
    main()
