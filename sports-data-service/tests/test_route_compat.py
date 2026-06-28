from starlette.requests import Request

from src import api


def _request(path: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [(b"host", b"testserver")],
            "query_string": b"",
        }
    )


def test_unknown_curl_path_returns_help_not_500():
    response = api.curl_catch_all("definitely/not-a-route", _request("/curl/definitely/not-a-route"))

    assert "Sports Data Service API Help" in response
    assert "testserver" in response


def test_curl_schedules_all_compat_alias(monkeypatch):
    monkeypatch.setattr(
        api,
        "get_schedules_all_sports_curl_v1",
        lambda date, tz=None: f"compat schedule {date} {tz}",
    )

    response = api.get_schedules_all_sports_curl_v1_compat("today", "et")

    assert response == "compat schedule today et"


def test_api_schedules_all_compat_alias(monkeypatch):
    monkeypatch.setattr(
        api,
        "get_schedules_all_sports_api_v1",
        lambda date, tz=None: {"date": "2026-06-20", "sports": {"nba": []}},
    )

    response = api.get_schedules_all_sports_api_v1_compat("today", "et")

    assert response == {"date": "2026-06-20", "sports": {"nba": []}}


def test_curl_cricket_enrichment_round_trips_fields(monkeypatch):
    class Wrapper:
        def __init__(self):
            self.home_team = "Texas Super Kings"
            self.visitor_team = "MI New York"
            self.cricket_home_score = ""
            self.cricket_away_score = ""
            self.cricket_status = ""

    def fake_enricher(sport, games_dicts, target_date):
        assert sport == "ipl"
        games_dicts[0].update(
            {
                "cricket_home_score": "158/6[20]",
                "cricket_away_score": "162/4[19.3]",
                "cricket_status": "MI New York won by 6 wickets",
                "cricket_winner": "MINY",
            }
        )
        return games_dicts

    monkeypatch.setattr(api, "_apply_dict_enrichers", fake_enricher)

    wrappers = [Wrapper()]
    api._enrich_curl_wrappers("ipl", api.date(2026, 6, 21), wrappers)

    assert wrappers[0].cricket_home_score == "158/6[20]"
    assert wrappers[0].cricket_away_score == "162/4[19.3]"
    assert wrappers[0].cricket_status == "MI New York won by 6 wickets"
    assert wrappers[0].cricket_winner == "MINY"
