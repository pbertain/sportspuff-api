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


def test_curl_schedule_all_enriches_world_cup_wrappers(monkeypatch):
    class Wrapper:
        def __init__(self):
            self.league = "WC"
            self.game_id = "wc-1"
            self.home_team = "Germany"
            self.visitor_team = "Paraguay"
            self.home_team_abbrev = "GER"
            self.visitor_team_abbrev = "PAR"
            self.home_wins = 0
            self.home_draws = 0
            self.home_losses = 0
            self.visitor_wins = 0
            self.visitor_draws = 0
            self.visitor_losses = 0
            self.home_score_total = 0
            self.visitor_score_total = 0
            self.game_status = "scheduled"
            self.game_type = "group_matchday_1"
            self.is_final = False
            self.game_time = None

    def fake_get_all_sport_games(target_date, timezone):
        return {"wc": [Wrapper()], "nba": []}

    def fake_enricher(sport, target_date, wrappers):
        if sport == "wc":
            wrappers[0].home_record = "2-1-0"
            wrappers[0].visitor_record = "1-0-2"
        return wrappers

    captured = {}

    def fake_format_schedule(games, target_date, timezone, show_all_sports=False):
        captured["games"] = games
        captured["show_all_sports"] = show_all_sports
        return "ok"

    monkeypatch.setattr(api, "_get_all_sport_games", fake_get_all_sport_games)
    monkeypatch.setattr(api, "_enrich_curl_wrappers", fake_enricher)
    monkeypatch.setattr(api, "format_schedule_curl", fake_format_schedule)

    response = api.get_schedule_curl_v1("all", "tomorrow", None)

    assert response == "ok"
    assert captured["show_all_sports"] is True
    assert captured["games"][0].home_record == "2-1-0"
    assert captured["games"][0].visitor_record == "1-0-2"


def test_world_cup_bracket_endpoint_returns_structured_lattice(monkeypatch):
    class FakeWCCollector:
        def get_knockout_bracket(self):
            return {
                "format": "round_of_32",
                "sides": {"left": [{"match_number": 73}], "right": [{"match_number": 84}]},
                "rounds": [{"name": "Round of 32", "matches": [{"match_number": 73}, {"match_number": 84}]}],
            }

    api._wc_bracket_cache.clear()
    monkeypatch.setattr(api, "get_collector", lambda league: FakeWCCollector() if league == "WC" else None)

    payload = api.get_world_cup_bracket_api_v1()

    assert payload["sport"] == "wc"
    assert payload["knockout_bracket"]["format"] == "round_of_32"
    assert payload["knockout_bracket"]["sides"]["left"][0]["match_number"] == 73
    assert payload["knockout_bracket"]["rounds"][0]["name"] == "Round of 32"


def test_world_cup_season_info_includes_knockout_bracket(monkeypatch):
    class FakeWCCollector:
        def get_season_info(self):
            return {
                "year": 2026,
                "current_phase": "Knockout Stage",
                "season_types": [{"name": "FIFA World Cup", "start_date": "2026-06-11", "end_date": "2026-07-19"}],
            }

        def get_knockout_bracket(self):
            return {
                "format": "round_of_32",
                "sides": {"left": [], "right": []},
                "rounds": [],
            }

    api._season_info_cache.clear()
    api._wc_bracket_cache.clear()
    monkeypatch.setattr(api, "get_collector", lambda league: FakeWCCollector() if league == "WC" else None)
    monkeypatch.setattr("src.services.champions.get_last_champion", lambda league: None)

    payload = api.get_season_info("wc")

    assert payload["year"] == 2026
    assert payload["current_phase"] == "Knockout Stage"
    assert payload["knockout_bracket"]["format"] == "round_of_32"


def test_wnba_season_info_accepts_string_year(monkeypatch):
    from src.collectors.wnba import WNBACollector

    class FakeResponse:
        status_code = 200

        def json(self):
            return {
                "seasons": [
                    {
                        "year": "2026",
                        "types": [
                            {
                                "name": "Regular Season",
                                "startDate": "2026-05-01T00:00:00Z",
                                "endDate": "2026-10-01T00:00:00Z",
                            }
                        ],
                    }
                ]
            }

    monkeypatch.setattr(WNBACollector, "_check_rate_limit", lambda self: None)
    monkeypatch.setattr(WNBACollector, "_tracked_get", lambda self, *args, **kwargs: FakeResponse())

    collector = WNBACollector()
    payload = collector.get_season_info(2026)

    assert payload is not None
    assert payload["season_types"]
    assert payload["current_phase"] == "Regular Season"
