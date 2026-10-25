"""Build the daily_city_metrics mart from the raw hourly tables."""

import duckdb

# PM2.5 thresholds: WHO 2021 24-hour guideline and the US EPA AQI breakpoints (2024 revision).
WHO_PM25_24H = 15.0

DDL = """
CREATE TABLE IF NOT EXISTS daily_city_metrics (
    date                   DATE    NOT NULL,
    city                   VARCHAR NOT NULL,
    avg_temp_c             DOUBLE,
    min_temp_c             DOUBLE,
    max_temp_c             DOUBLE,
    avg_humidity_pct       DOUBLE,
    total_precip_mm        DOUBLE,
    max_wind_kmh           DOUBLE,
    avg_pm2_5              DOUBLE,
    max_pm2_5              DOUBLE,
    avg_pm10               DOUBLE,
    avg_no2                DOUBLE,
    hours_pm25_above_who   INTEGER,
    pm25_aqi_category      VARCHAR,
    built_at               TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (date, city)
)
"""

BUILD_SQL = f"""
INSERT INTO daily_city_metrics
SELECT
    CAST(w.ts AS DATE)                              AS date,
    w.city,
    ROUND(AVG(w.temperature_2m), 2),
    MIN(w.temperature_2m),
    MAX(w.temperature_2m),
    ROUND(AVG(w.relative_humidity_2m), 1),
    ROUND(SUM(w.precipitation), 2),
    MAX(w.wind_speed_10m),
    ROUND(AVG(a.pm2_5), 2),
    MAX(a.pm2_5),
    ROUND(AVG(a.pm10), 2),
    ROUND(AVG(a.nitrogen_dioxide), 2),
    COUNT(*) FILTER (WHERE a.pm2_5 > {WHO_PM25_24H}),
    CASE
        WHEN AVG(a.pm2_5) IS NULL THEN NULL
        WHEN AVG(a.pm2_5) <= 9.0 THEN 'good'
        WHEN AVG(a.pm2_5) <= 35.4 THEN 'moderate'
        WHEN AVG(a.pm2_5) <= 55.4 THEN 'unhealthy_for_sensitive_groups'
        WHEN AVG(a.pm2_5) <= 125.4 THEN 'unhealthy'
        WHEN AVG(a.pm2_5) <= 225.4 THEN 'very_unhealthy'
        ELSE 'hazardous'
    END,
    now()
FROM raw_weather_hourly AS w
LEFT JOIN raw_air_quality_hourly AS a
    ON a.city = w.city AND a.ts = w.ts
WHERE CAST(w.ts AS DATE) = CAST(? AS DATE)
GROUP BY 1, 2
"""


def build_daily_metrics(con: duckdb.DuckDBPyConnection, day: str) -> int:
    """Rebuild one day of the mart. Safe to re-run (delete + insert in one transaction)."""
    con.execute(DDL)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM daily_city_metrics WHERE date = CAST(? AS DATE)", [day])
        con.execute(BUILD_SQL, [day])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return con.execute(
        "SELECT count(*) FROM daily_city_metrics WHERE date = CAST(? AS DATE)", [day]
    ).fetchone()[0]
