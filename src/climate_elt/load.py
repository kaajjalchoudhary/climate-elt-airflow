"""Load raw JSON files for one day into DuckDB raw tables (idempotent per day)."""

import json
from datetime import datetime
from pathlib import Path

import duckdb

from climate_elt.config import AIR_VARS, WEATHER_VARS

TABLES = {
    "weather": ("raw_weather_hourly", WEATHER_VARS),
    "air_quality": ("raw_air_quality_hourly", AIR_VARS),
}


def ensure_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    # No PRIMARY KEY on purpose: DuckDB checks constraints eagerly, so deleting and re-inserting
    # the same keys inside one transaction fails. Uniqueness of (city, ts) is enforced by a data
    # quality check instead, which keeps the day-level replace atomic.
    for table, variables in TABLES.values():
        cols = ", ".join(f"{v} DOUBLE" for v in variables)
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {table} ("
            f"city VARCHAR NOT NULL, ts TIMESTAMP NOT NULL, {cols}, "
            f"extracted_at TIMESTAMPTZ NOT NULL)"
        )


def parse_hourly(payload: dict, variables: list[str]) -> list[tuple]:
    """Turn Open-Meteo's column-oriented ``hourly`` block into row tuples."""
    hourly = payload["hourly"]
    times = hourly["time"]
    for v in variables:
        if len(hourly.get(v, [])) != len(times):
            raise ValueError(
                f"column {v!r} has {len(hourly.get(v, []))} values, expected {len(times)}"
            )
    meta = payload["_meta"]
    extracted_at = datetime.fromisoformat(meta["extracted_at"])
    return [
        (
            meta["city"],
            datetime.fromisoformat(t),
            *(hourly[v][i] for v in variables),
            extracted_at,
        )
        for i, t in enumerate(times)
    ]


def load_day(con: duckdb.DuckDBPyConnection, base_dir: Path, day: str) -> dict[str, int]:
    """Replace one day of raw data with whatever is in the landing zone."""
    ensure_raw_tables(con)
    counts = {}
    for source, (table, variables) in TABLES.items():
        rows: list[tuple] = []
        for path in sorted((Path(base_dir) / "raw" / source / f"date={day}").glob("city=*.json")):
            rows.extend(parse_hourly(json.loads(path.read_text()), variables))
        placeholders = ", ".join(["?"] * (len(variables) + 3))
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"DELETE FROM {table} WHERE CAST(ts AS DATE) = CAST(? AS DATE)", [day])
            if rows:
                con.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        counts[table] = len(rows)
    return counts
