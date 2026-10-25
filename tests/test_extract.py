import json

from climate_elt.config import SOURCES
from climate_elt.extract import build_params, extract_city, raw_path


def test_build_params(cities):
    params = build_params(cities[0], "2024-06-01", ["a", "b"])
    assert params["start_date"] == params["end_date"] == "2024-06-01"
    assert params["hourly"] == "a,b"
    assert params["timezone"] == "GMT"


def test_extract_city_lands_one_file_per_source(tmp_path, cities, fake_fetch):
    paths = extract_city(cities[0], "2024-06-01", tmp_path, fake_fetch)

    assert len(paths) == len(SOURCES) == len(fake_fetch.calls)
    for source in SOURCES:
        path = raw_path(tmp_path, source, "2024-06-01", "alpha")
        assert str(path) in paths
        payload = json.loads(path.read_text())
        assert payload["_meta"]["city"] == "alpha"
        assert payload["_meta"]["source"] == source
        assert len(payload["hourly"]["time"]) == 24
    assert not list(tmp_path.rglob("*.tmp"))  # atomic writes leave no temp files


def test_extract_is_idempotent(tmp_path, cities, fake_fetch):
    first = extract_city(cities[0], "2024-06-01", tmp_path, fake_fetch)
    second = extract_city(cities[0], "2024-06-01", tmp_path, fake_fetch)
    assert first == second
    assert len(list(tmp_path.rglob("*.json"))) == len(SOURCES)
