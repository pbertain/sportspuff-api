from datetime import date
import json

from src import api
from src.collectors.cricket import is_expected_cricket_season_window


class _FakeCricketSeasonCollector:
    def __init__(self, payload):
        self._payload = payload

    def get_season(self):
        return dict(self._payload)


def test_cricket_season_window_flags_mlc_june_as_in_season():
    assert is_expected_cricket_season_window("MLC", date(2026, 6, 24)) is True
    assert is_expected_cricket_season_window("MLC", date(2026, 1, 24)) is False


def test_cricket_season_window_flags_ipl_april_as_in_season():
    assert is_expected_cricket_season_window("IPL", date(2026, 4, 10)) is True
    assert is_expected_cricket_season_window("IPL", date(2026, 11, 10)) is False


def test_cricket_season_endpoint_returns_503_for_error_payload(monkeypatch):
    monkeypatch.setattr(
        api,
        "get_collector",
        lambda league: _FakeCricketSeasonCollector({
            "league": "MLC",
            "series_id": "",
            "series_name": "",
            "live": True,
            "matches": [],
            "standings": [],
            "api_stats": {},
            "status": "error",
            "stale": True,
            "reason": "upstream_refresh_failed",
        }),
    )

    response = api.get_cricket_season("mlc")
    payload = json.loads(response.body)

    assert response.status_code == 503
    assert payload["error"]["code"] == "service_unavailable"
    assert payload["error"]["message"] == "MLC season feed unavailable"
    assert payload["error"]["details"]["reason"] == "upstream_refresh_failed"


def test_cricket_season_endpoint_returns_explicit_off_season_success(monkeypatch):
    monkeypatch.setattr(
        api,
        "get_collector",
        lambda league: _FakeCricketSeasonCollector({
            "league": "MLC",
            "series_id": "5401",
            "series_name": "Major League Cricket",
            "live": False,
            "matches": [],
            "standings": [],
            "api_stats": {"provider": "thesportsdb"},
            "status": "off_season",
            "stale": False,
            "reason": "off_season",
        }),
    )

    payload = api.get_cricket_season("mlc")

    assert payload["status"] == "off_season"
    assert payload["live"] is False
    assert payload["matches"] == []
    assert payload["reason"] == "off_season"
