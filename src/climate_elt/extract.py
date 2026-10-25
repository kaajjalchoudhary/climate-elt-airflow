"""Extract hourly observations from the Open-Meteo APIs into a raw JSON landing zone.

Raw files are written to ``<data_dir>/raw/<source>/date=<day>/city=<slug>.json`` so every run is
replayable from the landing zone without calling the API again.
"""

import json
import os
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from climate_elt.config import SOURCES

Fetch = Callable[[str, dict], dict]


def build_params(city: dict, day: str, variables: list[str]) -> dict[str, object]:
    return {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "start_date": day,
        "end_date": day,
        "hourly": ",".join(variables),
        "timezone": "GMT",
    }


def _session() -> requests.Session:
    retry = Retry(total=5, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504))
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_json(url: str, params: dict) -> dict:
    with _session() as session:
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
    if payload.get("error"):
        raise RuntimeError(f"Open-Meteo error: {payload.get('reason')}")
    return payload


def raw_path(base_dir: Path, source: str, day: str, slug: str) -> Path:
    return Path(base_dir) / "raw" / source / f"date={day}" / f"city={slug}.json"


def _write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload))
    os.replace(tmp, path)


def extract_city(city: dict, day: str, base_dir: Path, fetch: Fetch = fetch_json) -> list[str]:
    """Fetch every source for one city and day. Returns the written file paths."""
    written = []
    for source, spec in SOURCES.items():
        payload = fetch(spec["url"], build_params(city, day, spec["variables"]))
        payload["_meta"] = {
            "city": city["slug"],
            "source": source,
            "date": day,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }
        path = raw_path(base_dir, source, day, city["slug"])
        _write_atomic(path, payload)
        written.append(str(path))
    return written
