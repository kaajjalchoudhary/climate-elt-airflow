from datetime import date, datetime, timedelta

import pytest


def make_payload(day: str, variables, value=lambda var, hour: float(hour)):
    """A response shaped like Open-Meteo's: column-oriented hourly arrays, 24 values per day."""
    start = datetime.combine(date.fromisoformat(day), datetime.min.time())
    times = [(start + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in range(24)]
    return {
        "latitude": 0.0,
        "longitude": 0.0,
        "hourly_units": {v: "unit" for v in variables},
        "hourly": {
            "time": times,
            **{v: [value(v, h) for h in range(24)] for v in variables},
        },
    }


class FakeOpenMeteo:
    """Stands in for the HTTP client; records calls and serves synthetic payloads."""

    def __init__(self, value=lambda var, hour: float(hour)):
        self.value = value
        self.calls = []

    def __call__(self, url, params):
        self.calls.append((url, params))
        variables = params["hourly"].split(",")
        return make_payload(params["start_date"], variables, self.value)


@pytest.fixture
def fake_fetch():
    return FakeOpenMeteo()


@pytest.fixture
def cities():
    return [
        {"slug": "alpha", "name": "Alpha", "country": "AA", "lat": 1.0, "lon": 2.0},
        {"slug": "beta", "name": "Beta", "country": "BB", "lat": 3.0, "lon": 4.0},
    ]
