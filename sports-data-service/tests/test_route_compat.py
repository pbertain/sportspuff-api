import json
from starlette.requests import Request
from types import SimpleNamespace
from datetime import date
from pathlib import Path

import pytest

from src import api
from src.collectors.tennis_thesportsdb import TennisTheSportsDBCollector
from src.services import tennis_scores
from src.services.tour_de_france import GiroDItaliaDataService, LaVueltaDataService, TourDeFranceDataService


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


def test_cycling_uses_bundled_template_overlay_by_default(monkeypatch):
    class FakeCyclingBaseCollector:
        def set_timezone(self, timezone):
            self.timezone = timezone

        def get_schedule(self, target_date):
            return []

        def get_live_scores(self, target_date):
            return []

    monkeypatch.setattr("src.collectors.cycling_thesportsdb.CyclingTheSportsDBCollector", FakeCyclingBaseCollector)
    monkeypatch.setattr(api.settings, "cycling_data_dir", "")

    collector = api.get_collector("CYCLING")
    rows = collector.get_schedule(date(2026, 7, 4))

    assert rows
    assert rows[0]["cycling_race"] == "Tour de France"
    assert rows[0]["cycling_stage_label"] == "Stage 1"
    assert rows[0]["cycling_url"] == "https://www.letour.fr/en/stage-1"


def test_tennis_schedule_falls_back_to_espn_rows(monkeypatch):
    monkeypatch.setattr(TennisTheSportsDBCollector, "_season_events", lambda self, season: [])
    monkeypatch.setattr(
        tennis_scores,
        "build_schedule_games",
        lambda sport, target_date: [
            {
                "league": sport.upper(),
                "game_id": f"{sport}-espn-1",
                "game_date": target_date.isoformat(),
                "game_time": None,
                "game_type": "match",
                "home_team": "Carlos Alcaraz",
                "visitor_team": "Novak Djokovic",
                "game_status": "scheduled",
                "is_final": False,
                "home_score_total": 0,
                "visitor_score_total": 0,
                "home_full_name": "Carlos Alcaraz",
                "visitor_full_name": "Novak Djokovic",
                "tennis_tournament": "Wimbledon",
            }
        ],
    )

    collector = TennisTheSportsDBCollector("ATP")
    rows = collector.get_schedule(date(2026, 7, 3))

    assert rows
    assert rows[0]["tennis_tournament"] == "Wimbledon"
    assert rows[0]["home_full_name"] == "Carlos Alcaraz"
    assert rows[0]["visitor_full_name"] == "Novak Djokovic"


def test_tennis_espn_fetch_filters_atp_and_wta_tours(monkeypatch):
    class _FakeResponse:
        status_code = 200

        def json(self):
            return {
                "events": [
                    {
                        "name": "Wimbledon",
                        "groupings": [
                            {
                                "grouping": {"slug": "mens-singles"},
                                "competitions": [
                                    {
                                        "date": "2026-07-04T10:00:00Z",
                                        "competitors": [
                                            {"athlete": {"displayName": "Carlos Alcaraz"}, "linescores": [], "winner": False},
                                            {"athlete": {"displayName": "Novak Djokovic"}, "linescores": [], "winner": True},
                                        ],
                                    }
                                ],
                            },
                            {
                                "grouping": {"slug": "womens-singles"},
                                "competitions": [
                                    {
                                        "date": "2026-07-04T11:00:00Z",
                                        "competitors": [
                                            {"athlete": {"displayName": "Aryna Sabalenka"}, "linescores": [], "winner": False},
                                            {"athlete": {"displayName": "Iga Swiatek"}, "linescores": [], "winner": True},
                                        ],
                                    }
                                ],
                            },
                        ],
                    }
                ]
            }

    tennis_scores._cache.clear()
    monkeypatch.setattr(tennis_scores.requests, "get", lambda *args, **kwargs: _FakeResponse())

    atp_matches = tennis_scores._fetch_matches("ATP", date(2026, 7, 4))
    wta_matches = tennis_scores._fetch_matches("WTA", date(2026, 7, 4))

    assert len(atp_matches or []) == 1
    assert atp_matches[0]["side1_name"] == "Carlos Alcaraz"
    assert atp_matches[0]["side2_name"] == "Novak Djokovic"
    assert atp_matches[0]["side1_seed"] == 1
    assert atp_matches[0]["side2_seed"] == 2
    assert len(wta_matches or []) == 1
    assert wta_matches[0]["side1_name"] == "Aryna Sabalenka"
    assert wta_matches[0]["side2_name"] == "Iga Swiatek"
    assert wta_matches[0]["side1_seed"] == 1
    assert wta_matches[0]["side2_seed"] == 2


def test_tennis_format_includes_seeds_and_ranks():
    game = SimpleNamespace(
        home_full_name="Carlos Alcaraz",
        visitor_full_name="Novak Djokovic",
        home_team="Alcaraz",
        visitor_team="Djokovic",
        home_seed=3,
        visitor_seed=2,
        tennis_set_scores=[],
        tennis_winner=None,
        game_status="scheduled",
        game_time=None,
        is_final=False,
    )

    text = api._format_tennis_match(game, api.pytz.timezone("US/Pacific"))

    assert "[3]" in text
    assert "[2]" in text


def test_tennis_contract_exposes_rank_aliases():
    games = [
        {
            "league": "ATP",
            "home_team": "Alcaraz",
            "visitor_team": "Djokovic",
            "home_full_name": "Carlos Alcaraz",
            "visitor_full_name": "Novak Djokovic",
            "home_seed": 1,
            "visitor_seed": 2,
            "tennis_tournament": "Wimbledon",
            "tennis_set_scores": [
                {"set": 1, "home": 6, "visitor": 4},
                {"set": 2, "home": 3, "visitor": 6},
                {"set": 3, "home": 6, "visitor": 3},
                {"set": 4, "home": 7, "visitor": 5},
            ],
            "home_sets_won": 3,
            "visitor_sets_won": 1,
            "tennis_winner": "home",
        }
    ]

    api._apply_tennis_contract("atp", games)

    assert games[0]["player1_rank"] == 2
    assert games[0]["player2_rank"] == 1
    assert games[0]["visitor_rank"] == 2
    assert games[0]["home_rank"] == 1
    assert games[0]["visitor_score"] == 1
    assert games[0]["home_score"] == 3
    assert games[0]["player1_score"] == [4, 6, 3, 5]
    assert games[0]["player2_score"] == [6, 3, 6, 7]
    assert games[0]["winner"] == "player2"


def test_tennis_scores_api_uses_sets_won_as_score_totals(monkeypatch):
    class Wrapper:
        def __init__(self):
            self.game_id = "atp-1"
            self.game_date = date(2026, 7, 8)
            self.game_time = None
            self.home_team = "Alcaraz"
            self.home_team_abbrev = ""
            self.visitor_team = "Djokovic"
            self.visitor_team_abbrev = ""
            self.game_status = "final"
            self.game_type = "match"
            self.home_score_total = 0
            self.visitor_score_total = 0
            self.is_final = True
            self.current_period = ""
            self.time_remaining = ""
            self.home_wins = 0
            self.home_losses = 0
            self.visitor_wins = 0
            self.visitor_losses = 0
            self.home_period_scores = {}
            self.visitor_period_scores = {}
            self.tennis_tournament = "Wimbledon"
            self.tennis_match_label = "Wimbledon Alcaraz vs Djokovic"
            self.tennis_round = ""
            self.tennis_country = ""
            self.tennis_video = ""
            self.home_full_name = "Carlos Alcaraz"
            self.visitor_full_name = "Novak Djokovic"
            self.home_seed = None
            self.visitor_seed = None
            self.tennis_set_scores = None
            self.home_sets_won = None
            self.visitor_sets_won = None
            self.tennis_summary = None
            self.tennis_winner = None

    def fake_get_games_for_curl(league, target_date, timezone, include_metadata=False, **kwargs):
        payload = [Wrapper()]
        meta = {"timestamp": 0, "empty_state": None, "source_updated_at": None}
        return (payload, meta) if include_metadata else payload

    def fake_fetch_matches(sport, target_date):
        assert sport == "atp"
        return [
            {
                "competition_date": "2026-07-08",
                "competition_time": "2026-07-08T10:00:00Z",
                "tournament": "Wimbledon",
                "side1_name": "Novak Djokovic",
                "side2_name": "Carlos Alcaraz",
                "side1_sets_won": 1,
                "side2_sets_won": 3,
                "side1_winner": False,
                "side2_winner": True,
                "side1_seed": 2,
                "side2_seed": 1,
                "set_scores": [
                    {"set": 1, "side1": 4, "side2": 6},
                    {"set": 2, "side1": 6, "side2": 3},
                    {"set": 3, "side1": 3, "side2": 6},
                    {"set": 4, "side1": 5, "side2": 7},
                ],
                "summary": "Carlos Alcaraz bt Novak Djokovic 6-4 3-6 6-3 7-5",
                "is_final": True,
                "state": "post",
                "venue_name": "Centre Court",
                "court_name": "Centre Court",
            }
        ]

    monkeypatch.setattr(api, "_get_games_for_curl", fake_get_games_for_curl)
    monkeypatch.setattr(tennis_scores, "_fetch_matches", fake_fetch_matches)

    payload = api.get_scores_api_v1("atp", "2026-07-08", None)
    score = payload["scores"][0]

    assert score["visitor_score"] == 1
    assert score["home_score"] == 3
    assert score["player1_score"] == [4, 6, 3, 5]
    assert score["player2_score"] == [6, 3, 6, 7]
    assert score["player1_sets_won"] == 1
    assert score["player2_sets_won"] == 3


def test_wc_curl_wrappers_keep_pk_scores(monkeypatch):
    class Wrapper:
        def __init__(self):
            self.league = "WC"
            self.home_team = "Australia"
            self.visitor_team = "Egypt"
            self.home_team_abbrev = "AUS"
            self.visitor_team_abbrev = "EGY"
            self.home_score_total = 1
            self.visitor_score_total = 1
            self.home_shootout_score = 2
            self.visitor_shootout_score = 4
            self.is_final = True
            self.game_status = "final"
            self.game_time = None

    monkeypatch.setattr(api, "_apply_dict_enrichers", lambda sport, games_dicts, target_date: games_dicts)

    wrappers = [Wrapper()]
    api._enrich_curl_wrappers("wc", date(2026, 7, 3), wrappers)

    assert wrappers[0].home_shootout_score == 2
    assert wrappers[0].visitor_shootout_score == 4


def test_cycling_pretty_link_formatting():
    game = SimpleNamespace(
        cycling_race="Tour de France",
        cycling_stage_label="Stage 1",
        cycling_event_label="Tour de France Stage 1",
        game_date="2026-07-04",
        start_city="Barcelone",
        finish_city="Barcelone",
        race_type="Team Time-Trial",
        cycling_distance_km="19.6",
        game_status="scheduled",
        is_final=False,
        cycling_url="https://www.letour.fr/en/stage-1",
        cycling_url_label="Stage 1 details",
    )

    text = api._format_cycling_game(game, api.pytz.timezone("US/Pacific"))

    assert "Stage 1 details -> https://www.letour.fr/en/stage-1" in text


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


def test_tour_de_france_bundle_endpoint_uses_letour_scraper_bundle(monkeypatch):
    api._tour_de_france_cache.clear()
    monkeypatch.setattr(
        api.settings,
        "tour_de_france_data_dir",
        str(Path(__file__).resolve().parents[2] / "letour-scraper"),
    )

    payload = api.get_tour_de_france_bundle_api_v1(2026)

    assert payload["race"] == "Tour de France"
    assert payload["year"] == 2026
    assert payload["stages"]
    assert payload["stages"][0]["stage"]["date"] == "2026-07-04"
    assert payload["stages"][0]["stage"]["status"] == "final"
    assert payload["stages"][0]["stage"]["stage_timezone"] == "Europe/Paris"
    assert payload["stages"][0]["stage"]["stage_timezone_abbrev"] == "CEST"
    assert payload["stages"][1]["stage"]["stage_start_utc"] == "2026-07-05T11:45:00Z"
    assert payload["stages"][0]["schedule"]["stage_number"] == 1
    assert payload["stages"][0]["schedule"]["stage_timezone"] == "Europe/Paris"
    assert payload["latest_classifications"]["stage"]
    assert payload["meta"]["source_updated_at"]


def test_tour_de_france_stage_endpoint_returns_stage_detail(monkeypatch):
    api._tour_de_france_cache.clear()
    monkeypatch.setattr(
        api.settings,
        "tour_de_france_data_dir",
        str(Path(__file__).resolve().parents[2] / "letour-scraper"),
    )

    payload = api.get_tour_de_france_stage_api_v1(2026, 2)

    assert payload["race"] == "Tour de France"
    assert payload["year"] == 2026
    assert payload["stage"]["stage_number"] == 2
    assert payload["stage"]["date"] == "2026-07-05"
    assert payload["stage"]["stage_timezone"] == "Europe/Paris"
    assert payload["stage"]["stage_timezone_abbrev"] == "CEST"
    assert payload["stage"]["stage_start_utc"] == "2026-07-05T11:45:00Z"
    assert payload["schedule"]["stage_number"] == 2
    assert payload["schedule"]["stage_timezone"] == "Europe/Paris"
    assert payload["classifications"]
    assert payload["classifications"][0]["classification_type"] == "stage"
    assert payload["classifications"][0]["rows"]
    assert payload["classification_rows"]
    assert payload["stage_results"]
    assert payload["stage_results"][0]["classification_type"] == "stage"
    assert payload["stage_results"][0]["rank"] == 1
    assert payload["overall_classifications"]["stage"]
    assert payload["meta"]["source_updated_at"]


def test_tour_de_france_stage_payload_has_no_nan_or_merge_helpers(monkeypatch):
    api._tour_de_france_cache.clear()
    monkeypatch.setattr(
        api.settings,
        "tour_de_france_data_dir",
        str(Path(__file__).resolve().parents[2] / "letour-scraper"),
    )

    payload = api.get_tour_de_france_stage_api_v1(2026, 7)

    def walk(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                assert not key.endswith("_lk")
                walk(value)
        elif isinstance(obj, list):
            for value in obj:
                walk(value)
        elif isinstance(obj, float):
            assert obj == obj
            assert obj not in (float("inf"), float("-inf"))

    walk(payload)


def test_tour_de_france_stage_endpoint_404s_for_unknown_stage(monkeypatch):
    api._tour_de_france_cache.clear()
    monkeypatch.setattr(
        api.settings,
        "tour_de_france_data_dir",
        str(Path(__file__).resolve().parents[2] / "letour-scraper"),
    )

    with pytest.raises(api.HTTPException) as exc:
        api.get_tour_de_france_stage_api_v1(2026, 99)

    assert exc.value.status_code == 404


def test_la_vuelta_bundle_endpoint_uses_lavuelta_bundle(monkeypatch):
    api._tour_de_france_cache.clear()
    monkeypatch.setattr(
        api.settings,
        "la_vuelta_data_dir",
        str(Path(__file__).resolve().parents[2] / "lavuelta-scraper"),
    )

    payload = api.get_la_vuelta_bundle_api_v1(2026)

    assert payload["race"] == "La Vuelta"
    assert payload["year"] == 2026
    assert len(payload["stages"]) == 21
    assert payload["stages"]
    assert payload["stages"][0]["stage"]["stage_number"] == 1
    assert payload["meta"]["source_updated_at"]


def test_la_vuelta_stage_endpoint_returns_stage_results(monkeypatch):
    api._tour_de_france_cache.clear()
    monkeypatch.setattr(
        api.settings,
        "la_vuelta_data_dir",
        str(Path(__file__).resolve().parents[2] / "lavuelta-scraper"),
    )

    payload = api.get_la_vuelta_stage_api_v1(2026, 1)

    assert payload["race"] == "La Vuelta"
    assert payload["year"] == 2026
    assert payload["stage"]["stage_number"] == 1
    assert payload["classifications"]
    assert payload["classifications"][0]["classification_type"] == "stage"
    assert payload["stage_results"]
    assert payload["stage_results"][0]["classification_type"] == "stage"
    assert payload["stage_results"][0]["rank"] == 1
    assert payload["stage_results"][0]["bib"] == 71
    assert payload["stage_results"][0]["team_name"] == "ALPECIN-DECEUNINCK"
    assert payload["stage_results"][0]["time"] == "04h 09' 12''"
    assert payload["stage_results"][0]["gap"] == "-"
    assert payload["meta"]["source_updated_at"]


def test_la_vuelta_2025_bundle_does_not_fall_back_to_2026(monkeypatch):
    api._tour_de_france_cache.clear()
    monkeypatch.setattr(
        api.settings,
        "la_vuelta_data_dir",
        str(Path(__file__).resolve().parents[2] / "lavuelta-scraper"),
    )

    with pytest.raises(api.HTTPException) as exc:
        api.get_la_vuelta_bundle_api_v1(2025)

    assert exc.value.status_code == 404


def test_giro_bundle_endpoint_404s_without_bundle(monkeypatch, tmp_path):
    api._tour_de_france_cache.clear()
    monkeypatch.setattr(api.settings, "giro_d_italia_data_dir", str(tmp_path))

    with pytest.raises(api.HTTPException) as exc:
        api.get_giro_d_italia_bundle_api_v1(2026)

    assert exc.value.status_code == 404


def test_giro_bundle_endpoint_uses_giro_bundle(monkeypatch, tmp_path):
    api._tour_de_france_cache.clear()
    monkeypatch.setattr(api.settings, "giro_d_italia_data_dir", str(tmp_path))

    bundle = {
        "race": "Giro d'Italia",
        "year": 2026,
        "source": "giroditalia.it",
        "generated_at": "2026-07-11T19:00:00Z",
        "source_updated_at": "2026-07-11T19:00:00Z",
        "generated_files": ["stages.csv"],
        "teams": [],
        "riders": [],
        "stages": [
            {
                "stage": {
                    "race": "Giro d'Italia",
                    "stage_number": 1,
                    "stage_name": "Monaco > Monaco",
                    "date": "2026-08-22",
                    "status": "scheduled",
                    "winner": None,
                    "winner_url": None,
                    "team": None,
                    "team_url": None,
                    "distance_km": "9",
                    "race_type": "Individual Time-Trial",
                    "start_city": "Monaco",
                    "finish_city": "Monaco",
                    "cycling_event_label": "Giro d'Italia 2026 - Stage 1",
                    "cycling_country": None,
                    "cycling_url": "https://www.giroditalia.it/en/stage-1",
                    "rankings_url": "https://www.giroditalia.it/en/stage-1",
                    "stage_page_title": "Stage 1 - Monaco > Monaco - Giro d'Italia 2026",
                    "rankings_page_title": "Official classifications of Giro d'Italia 2026 - Stage 1",
                    "poll_state": "pre_stage",
                    "recommended_poll_minutes": 60,
                },
                "schedule": [
                    {
                        "stage_number": 1,
                        "stage_name": "Monaco > Monaco",
                        "cycling_url": "https://www.giroditalia.it/en/stage-1",
                        "rankings_url": "https://www.giroditalia.it/en/stage-1",
                        "stage_start_local": None,
                        "stage_finish_expected_local": None,
                        "stage_first_start_local": None,
                        "stage_last_arrival_local": None,
                        "poll_state": "pre_stage",
                        "recommended_poll_minutes": 60,
                    }
                ],
                "classifications": [],
            }
        ],
    }
    (tmp_path / "giro_app_bundle_2026.json").write_text(json.dumps(bundle), encoding="utf-8")

    payload = api.get_giro_d_italia_bundle_api_v1(2026)

    assert payload["race"] == "Giro d'Italia"
    assert payload["year"] == 2026
    assert payload["stages"][0]["stage"]["stage_number"] == 1
    assert payload["stages"][0]["stage"]["stage_name"] == "Monaco > Monaco"


def test_cycling_bundle_contract_preserves_country_fields(tmp_path):
    bundle_template = {
        "year": 2026,
        "source": "example",
        "generated_at": "2026-07-11T19:00:00Z",
        "source_updated_at": "2026-07-11T19:00:00Z",
        "generated_files": [],
        "teams": [{"team_name": "Test Team", "team_slug": "test-team", "team_url": "https://example.com/team"}],
        "riders": [
            {
                "rider_name": "Test Rider",
                "rider_slug": "test-rider",
                "rider_url": "https://example.com/rider",
                "rider_country_code": "ITA",
                "rider_country_flag": "ita",
            }
        ],
        "stages": [
            {
                "stage": {
                    "race": "placeholder",
                    "stage_number": 1,
                    "stage_name": "Test Stage",
                    "date": "2026-07-11",
                    "status": "final",
                    "winner": "Test Rider",
                    "winner_url": "https://example.com/rider",
                    "winner_country_code": "ITA",
                    "winner_country_flag": "ita",
                    "team": "Test Team",
                    "team_url": "https://example.com/team",
                    "distance_km": "10",
                    "race_type": "Hilly",
                    "start_city": "A",
                    "finish_city": "B",
                    "cycling_event_label": "Stage 1",
                    "cycling_country": "France",
                    "cycling_url": "https://example.com/stage-1",
                    "rankings_url": "https://example.com/stage-1",
                    "stage_page_title": "Stage 1",
                    "rankings_page_title": "Rankings",
                    "poll_state": "post_stage",
                    "recommended_poll_minutes": 60,
                },
                "schedule": [
                    {
                        "stage_number": 1,
                        "stage_name": "Test Stage",
                        "cycling_url": "https://example.com/stage-1",
                        "rankings_url": "https://example.com/stage-1",
                        "stage_start_local": None,
                        "stage_finish_expected_local": None,
                        "stage_first_start_local": None,
                        "stage_last_arrival_local": None,
                        "poll_state": "post_stage",
                        "recommended_poll_minutes": 60,
                    }
                ],
                "classifications": [
                    {
                        "race": "placeholder",
                        "stage_number": 1,
                        "classification_type": "stage",
                        "rank": 1,
                        "rider_name": "Test Rider",
                        "rider_slug": "test-rider",
                        "rider_url": "https://example.com/rider",
                        "rider_country_code": "ITA",
                        "rider_country_flag": "ita",
                        "bib": 7,
                        "team_name": "Test Team",
                        "team_slug": "test-team",
                        "team_url": "https://example.com/team",
                        "time": "3:00:00",
                        "gap": "0:00",
                        "points": None,
                        "bonus": None,
                        "source_url": "https://example.com/stage-1",
                    }
                ],
            }
        ],
    }

    services = [
        (TourDeFranceDataService, "letour_app_bundle.json"),
        (LaVueltaDataService, "lavuelta_app_bundle.json"),
        (GiroDItaliaDataService, "giro_app_bundle.json"),
    ]

    for service_cls, filename in services:
        data_dir = tmp_path / filename.replace(".json", "")
        data_dir.mkdir()
        payload = dict(bundle_template)
        payload["race"] = service_cls.default_race
        payload["source"] = service_cls.bundle_basename
        payload["stages"] = [
            {
                **bundle_template["stages"][0],
                "stage": {
                    **bundle_template["stages"][0]["stage"],
                    "race": service_cls.default_race,
                },
                "classifications": [
                    {
                        **bundle_template["stages"][0]["classifications"][0],
                        "race": service_cls.default_race,
                    }
                ],
            }
        ]
        (data_dir / filename).write_text(json.dumps(payload), encoding="utf-8")

        service = service_cls(str(data_dir))
        result = service.get_bundle(2026)
        stage = result["stages"][0]["stage"]
        row = result["stages"][0]["classification_rows"][0]

        assert stage["winner_country_code"] == "ITA"
        assert stage["winner_country_flag"] == "ita"
        assert row["rider_country_code"] == "ITA"
        assert row["rider_country_flag"] == "ita"


def test_world_cup_round_of_32_matches_keep_shootout_winners(monkeypatch):
    from src.collectors.world_cup_thesportsdb import WorldCupTheSportsDBCollector

    collector = WorldCupTheSportsDBCollector()

    raw_event = {
        "idEvent": "123",
        "intMatch": "73",
        "intRound": "32",
        "dateEvent": "2026-07-04",
        "strTime": "11:00:00",
        "strStatus": "FT",
        "strHomeTeam": "Egypt",
        "strAwayTeam": "Australia",
        "intHomeScore": "1",
        "intAwayScore": "1",
    }

    def fake_season_events(_season):
        return [raw_event]

    def fake_enrich_games(_sport, _date, games):
        games[0]["home_shootout_score"] = 2
        games[0]["visitor_shootout_score"] = 4
        return games

    monkeypatch.setattr(collector, "_season_events", fake_season_events)
    monkeypatch.setattr(collector, "get_team_records", lambda: {
        "EGYPT": {"wins": 3, "draws": 0, "losses": 0, "record": "3-0-0", "group": "G", "group_rank": 1, "currently_advancing": True},
        "AUSTRALIA": {"wins": 1, "draws": 1, "losses": 1, "record": "1-1-1", "group": "D", "group_rank": 2, "currently_advancing": True},
    })
    monkeypatch.setattr("src.services.box_score.enrich_games", fake_enrich_games)

    bracket = collector.get_knockout_bracket()
    match = bracket["rounds"][0]["matches"][0]

    assert match["match_number"] == 74
    assert match["home_team"] == "Egypt"
    assert match["away_team"] == "Australia"
    assert match["home_score"] == 1
    assert match["visitor_score"] == 1
    assert match["home_shootout_score"] == 2
    assert match["visitor_shootout_score"] == 4
    assert match["winner"] == "Australia"
    assert match["wc_winner"] == "Australia"
    assert match["home_record"] == "3-0-0"
    assert match["away_record"] == "1-1-1"


def test_world_cup_later_round_matches_keep_shootout_winners(monkeypatch):
    from src.collectors.world_cup_thesportsdb import WorldCupTheSportsDBCollector

    collector = WorldCupTheSportsDBCollector()

    raw_event = {
        "idEvent": "2513671",
        "intMatch": "96",
        "intRound": "16",
        "dateEvent": "2026-07-07",
        "strTime": "20:00:00",
        "strStatus": "AP",
        "strHomeTeam": "Switzerland",
        "strAwayTeam": "Colombia",
        "intHomeScore": "0",
        "intAwayScore": "0",
    }

    def fake_season_events(_season):
        return [raw_event]

    def fake_enrich_games(_sport, _date, games):
        games[0]["home_shootout_score"] = 4
        games[0]["visitor_shootout_score"] = 3
        return games

    monkeypatch.setattr(collector, "_season_events", fake_season_events)
    monkeypatch.setattr(collector, "get_team_records", lambda: {
        "SWITZERLAND": {"wins": 2, "draws": 1, "losses": 0, "record": "2-1-0", "group": "B", "group_rank": 1, "currently_advancing": True},
        "COLOMBIA": {"wins": 2, "draws": 1, "losses": 0, "record": "2-1-0", "group": "K", "group_rank": 1, "currently_advancing": True},
    })
    monkeypatch.setattr("src.services.box_score.enrich_games", fake_enrich_games)

    bracket = collector.get_knockout_bracket()
    round_of_16 = bracket["rounds"][1]["matches"]
    match = next(m for m in round_of_16 if m["game_id"] == "2513671")

    assert match["home_team"] == "Switzerland"
    assert match["away_team"] == "Colombia"
    assert match["home_score"] == 0
    assert match["visitor_score"] == 0
    assert match["home_shootout_score"] == 4
    assert match["visitor_shootout_score"] == 3
    assert match["winner"] == "Switzerland"
    assert match["wc_winner"] == "Switzerland"


def test_world_cup_prefers_fifa_seasonbracket_payload(monkeypatch):
    from src.collectors.world_cup_thesportsdb import WorldCupTheSportsDBCollector

    collector = WorldCupTheSportsDBCollector()

    payload = {
        "GroupsStages": [
            {
                "Name": [{"Locale": "en-GB", "Description": "First Stage"}],
                "Groups": [
                    {
                        "Name": [{"Locale": "en-GB", "Description": "Group A"}],
                        "Matches": [
                            {
                                "IdMatch": "400021443",
                                "Date": "2026-06-11T19:00:00Z",
                                "Stadium": {
                                    "Name": [{"Locale": "en-GB", "Description": "Mexico City Stadium"}],
                                },
                                "HomeTeam": {
                                    "IdTeam": "43911",
                                    "TeamName": [{"Locale": "en-GB", "Description": "Mexico"}],
                                    "Abbreviation": "MEX",
                                },
                                "AwayTeam": {
                                    "IdTeam": "43929",
                                    "TeamName": [{"Locale": "en-GB", "Description": "South Africa"}],
                                    "Abbreviation": "RSA",
                                },
                                "HomeTeamScore": 2,
                                "AwayTeamScore": 0,
                                "MatchTimeStatus": 10,
                                "Winner": "43911",
                            }
                        ],
                    }
                ],
            }
        ],
        "KnockoutStages": [
            {
                "SequenceOrder": 4,
                "Name": [{"Locale": "en-GB", "Description": "Quarter-final"}],
                "Matches": [
                    {
                        "IdMatch": "2517651",
                        "MatchNumber": 99,
                        "Date": "2026-07-11T21:00:00Z",
                        "Stadium": {
                            "Name": [{"Locale": "en-GB", "Description": "AT&T Stadium"}],
                        },
                        "HomeTeam": {
                            "IdTeam": "43967",
                            "TeamName": [{"Locale": "en-GB", "Description": "Norway"}],
                            "Abbreviation": "NOR",
                        },
                        "AwayTeam": {
                            "IdTeam": "43942",
                            "TeamName": [{"Locale": "en-GB", "Description": "England"}],
                            "Abbreviation": "ENG",
                        },
                        "HomeTeamScore": 1,
                        "AwayTeamScore": 2,
                        "MatchTimeStatus": 10,
                        "Winner": "43942",
                    }
                ],
            }
        ],
    }

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return payload

    def fake_get(url, timeout=30, headers=None):
        assert url == "https://api.fifa.com/api/v3/seasonbracket/season/285023?language=en"
        return FakeResponse()

    monkeypatch.setattr("src.collectors.world_cup_thesportsdb.requests.get", fake_get)

    events = collector._season_events("2026")
    assert len(events) == 2

    knockout = next(event for event in events if event.get("intMatch") == "99")
    parsed = collector._parse_event(knockout)

    assert parsed["home_team"] == "Norway"
    assert parsed["visitor_team"] == "England"
    assert parsed["game_status"] == "final"
    assert parsed["wc_winner"] == "England"


def test_world_cup_bracket_propagates_winners_through_later_rounds(monkeypatch):
    from src.collectors.world_cup_thesportsdb import WorldCupTheSportsDBCollector

    collector = WorldCupTheSportsDBCollector()

    def _event(match_number: int, round_value: int, home: str, away: str, home_score: int = 2, away_score: int = 1):
        return {
            "idEvent": str(1000 + match_number),
            "intMatch": str(match_number),
            "intRound": str(round_value),
            "dateEvent": "2026-07-04",
            "strTime": "12:00:00",
            "strStatus": "FT",
            "strEvent": f"WC {round_value}",
            "strHomeTeam": home,
            "strAwayTeam": away,
            "intHomeScore": str(home_score),
            "intAwayScore": str(away_score),
        }

    round32_teams = [
        "Team A", "Team B", "Team C", "Team D",
        "Team E", "Team F", "Team G", "Team H",
        "Team I", "Team J", "Team K", "Team L",
        "Team M", "Team N", "Team O", "Team P",
        "Team Q", "Team R", "Team S", "Team T",
        "Team U", "Team V", "Team W", "Team X",
        "Team Y", "Team Z", "Team AA", "Team AB",
        "Team AC", "Team AD", "Team AE", "Team AF",
    ]

    events = []
    current_winners = []
    for index, match_number in enumerate(range(73, 89)):
        home = round32_teams[index * 2]
        away = round32_teams[index * 2 + 1]
        events.append(_event(match_number, 32, home, away))
        current_winners.append(home)

    round_specs = [
        (89, 16),
        (97, 8),
        (101, 4),
        (104, 2),
    ]
    next_winners = current_winners
    for start_match_number, round_value in round_specs:
        stage_winners = []
        for offset in range(0, len(next_winners), 2):
            home = next_winners[offset]
            away = next_winners[offset + 1]
            events.append(_event(start_match_number + offset // 2, round_value, home, away))
            stage_winners.append(home)
        next_winners = stage_winners

    monkeypatch.setattr(collector, "_season_events", lambda _season: events)
    monkeypatch.setattr(collector, "get_team_records", lambda: {})

    bracket = collector.get_knockout_bracket()
    rounds = {round_info["name"]: round_info["matches"] for round_info in bracket["rounds"]}

    round_of_16 = rounds["Round of 16"]
    quarterfinals = rounds["Quarter-final"]
    semifinals = rounds["Semi-final"]
    finals = rounds["Final"]

    assert round_of_16[0]["home_team"] == "Team A"
    assert round_of_16[0]["away_team"] == "Team C"
    assert round_of_16[0]["home_slot"] == "Winner Match 74"
    assert round_of_16[0]["away_slot"] == "Winner Match 76"

    assert quarterfinals[0]["home_team"] == "Team A"
    assert quarterfinals[0]["away_team"] == "Team E"
    assert quarterfinals[0]["home_slot"] == "Winner Match 89"
    assert quarterfinals[0]["away_slot"] == "Winner Match 90"

    assert semifinals[0]["home_team"] == "Team A"
    assert semifinals[0]["away_team"] == "Team I"
    assert semifinals[0]["home_slot"] == "Winner Match 97"
    assert semifinals[0]["away_slot"] == "Winner Match 98"

    assert finals[0]["home_team"] == "Team A"
    assert finals[0]["away_team"] == "Team Q"
    assert finals[0]["home_slot"] == "Winner Match 101"
    assert finals[0]["away_slot"] == "Winner Match 102"


def test_world_cup_round_of_32_layout_matches_fifa_visual_order(monkeypatch):
    from src.collectors.world_cup_thesportsdb import WorldCupTheSportsDBCollector

    collector = WorldCupTheSportsDBCollector()

    def _event(match_number: int, home: str, away: str):
        return {
            "idEvent": str(2000 + match_number),
            "intMatch": str(match_number),
            "intRound": "32",
            "dateEvent": "2026-07-01",
            "strTime": "12:00:00",
            "strStatus": "FT",
            "strEvent": f"WC Match {match_number}",
            "strHomeTeam": home,
            "strAwayTeam": away,
            "intHomeScore": "2",
            "intAwayScore": "1",
        }

    events = []
    for match_number in range(73, 89):
        events.append(_event(match_number, f"Team {match_number}A", f"Team {match_number}B"))

    monkeypatch.setattr(collector, "_season_events", lambda _season: events)
    monkeypatch.setattr(collector, "get_team_records", lambda: {})

    bracket = collector.get_knockout_bracket()
    round_of_32 = bracket["rounds"][0]["matches"]

    assert [m["match_number"] for m in round_of_32] == [74, 76, 77, 78, 73, 79, 75, 80, 83, 86, 84, 88, 81, 85, 82, 87]
    assert round_of_32[0]["match_number"] == 74
    assert round_of_32[1]["match_number"] == 76


def test_world_cup_bracket_uses_match_numbers_when_upstream_round_labels_are_wrong(monkeypatch):
    from src.collectors.world_cup_thesportsdb import WorldCupTheSportsDBCollector

    collector = WorldCupTheSportsDBCollector()

    events = [
        {
            "idEvent": "1089",
            "intMatch": "89",
            "intRound": "99",
            "dateEvent": "2026-07-05",
            "strTime": "12:00:00",
            "strStatus": "FT",
            "strEvent": "WC Match 89",
            "strHomeTeam": "Canada",
            "strAwayTeam": "Morocco",
            "intHomeScore": "0",
            "intAwayScore": "1",
        },
        {
            "idEvent": "1090",
            "intMatch": "90",
            "intRound": "99",
            "dateEvent": "2026-07-05",
            "strTime": "16:00:00",
            "strStatus": "FT",
            "strEvent": "WC Match 90",
            "strHomeTeam": "Paraguay",
            "strAwayTeam": "France",
            "intHomeScore": "0",
            "intAwayScore": "2",
        },
        {
            "idEvent": "1097",
            "intMatch": "97",
            "intRound": "16",
            "dateEvent": "2026-07-09",
            "strTime": "19:00:00",
            "strStatus": "FT",
            "strEvent": "WC Match 97",
            "strHomeTeam": "Morocco",
            "strAwayTeam": "France",
            "intHomeScore": "0",
            "intAwayScore": "2",
        },
    ]

    monkeypatch.setattr(collector, "_season_events", lambda _season: events)
    monkeypatch.setattr(collector, "get_team_records", lambda: {})

    bracket = collector.get_knockout_bracket()
    quarterfinals = {match["match_number"]: match for match in bracket["rounds"][2]["matches"]}

    assert quarterfinals[97]["home_team"] == "Morocco"
    assert quarterfinals[97]["away_team"] == "France"
    assert quarterfinals[97]["game_status"] == "final"
    assert quarterfinals[97]["winner"] == "France"


def test_world_cup_bracket_maps_observed_upstream_knockout_round_codes(monkeypatch):
    from src.collectors.world_cup_thesportsdb import WorldCupTheSportsDBCollector

    collector = WorldCupTheSportsDBCollector()

    events = [
        {
            "idEvent": "2505183",
            "intMatch": None,
            "intRound": "16",
            "dateEvent": "2026-07-04",
            "strTime": "17:00:00",
            "strStatus": "FT",
            "strEvent": "Canada vs Morocco",
            "strHomeTeam": "Canada",
            "strAwayTeam": "Morocco",
            "intHomeScore": "0",
            "intAwayScore": "3",
        },
        {
            "idEvent": "2505624",
            "intMatch": None,
            "intRound": "16",
            "dateEvent": "2026-07-04",
            "strTime": "21:00:00",
            "strStatus": "FT",
            "strEvent": "Paraguay vs France",
            "strHomeTeam": "Paraguay",
            "strAwayTeam": "France",
            "intHomeScore": "0",
            "intAwayScore": "1",
        },
        {
            "idEvent": "2515305",
            "intMatch": None,
            "intRound": "125",
            "dateEvent": "2026-07-09",
            "strTime": "20:00:00",
            "strStatus": "FT",
            "strEvent": "France vs Morocco",
            "strHomeTeam": "France",
            "strAwayTeam": "Morocco",
            "intHomeScore": "2",
            "intAwayScore": "0",
        },
        {
            "idEvent": "2528031",
            "intMatch": "98",
            "intRound": "8",
            "dateEvent": "2026-07-14",
            "strTime": "19:00:00",
            "strStatus": "FT",
            "strEvent": "Spain vs Portugal",
            "strHomeTeam": "Spain",
            "strAwayTeam": "Portugal",
            "intHomeScore": "1",
            "intAwayScore": "0",
        },
    ]

    monkeypatch.setattr(collector, "_season_events", lambda _season: events)
    monkeypatch.setattr(collector, "get_team_records", lambda: {})

    bracket = collector.get_knockout_bracket()
    quarterfinals = {match["match_number"]: match for match in bracket["rounds"][2]["matches"]}
    semifinals = {match["match_number"]: match for match in bracket["rounds"][3]["matches"]}

    assert quarterfinals[97]["home_team"] == "France"
    assert quarterfinals[97]["away_team"] == "Morocco"
    assert quarterfinals[97]["game_status"] == "final"
    assert quarterfinals[97]["winner"] == "France"
    assert semifinals[101]["home_team"] == "France"
    assert semifinals[101]["away_team"] == "Spain"
    assert semifinals[101]["game_status"] == "scheduled"
    assert semifinals[101]["game_status"] == "scheduled"


def test_world_cup_bracket_ignores_stale_scheduled_knockout_participants(monkeypatch):
    from src.collectors.world_cup_thesportsdb import WorldCupTheSportsDBCollector

    collector = WorldCupTheSportsDBCollector()

    events = [
        {
            "idEvent": "2001",
            "intMatch": "73",
            "intRound": "32",
            "dateEvent": "2026-07-01",
            "strTime": "12:00:00",
            "strStatus": "FT",
            "strEvent": "WC Match 73",
            "strHomeTeam": "Team A",
            "strAwayTeam": "Team B",
            "intHomeScore": "2",
            "intAwayScore": "1",
        },
        {
            "idEvent": "2002",
            "intMatch": "74",
            "intRound": "32",
            "dateEvent": "2026-07-01",
            "strTime": "16:00:00",
            "strStatus": "FT",
            "strEvent": "WC Match 74",
            "strHomeTeam": "Team C",
            "strAwayTeam": "Team D",
            "intHomeScore": "1",
            "intAwayScore": "0",
        },
        {
            "idEvent": "2003",
            "intMatch": "89",
            "intRound": "16",
            "dateEvent": "2026-07-05",
            "strTime": "12:00:00",
            "strStatus": "FT",
            "strEvent": "WC Match 89",
            "strHomeTeam": "Team A",
            "strAwayTeam": "Team C",
            "intHomeScore": "2",
            "intAwayScore": "0",
        },
        {
            "idEvent": "2004",
            "intMatch": "90",
            "intRound": "16",
            "dateEvent": "2026-07-05",
            "strTime": "16:00:00",
            "strStatus": "FT",
            "strEvent": "WC Match 90",
            "strHomeTeam": "Team E",
            "strAwayTeam": "Team F",
            "intHomeScore": "1",
            "intAwayScore": "0",
        },
        {
            "idEvent": "2005",
            "intMatch": "97",
            "intRound": "8",
            "dateEvent": "2026-07-09",
            "strTime": "19:00:00",
            "strStatus": "NS",
            "strEvent": "WC Match 97",
            "strHomeTeam": "Brazil",
            "strAwayTeam": "Portugal",
            "intHomeScore": None,
            "intAwayScore": None,
        },
        {
            "idEvent": "2006",
            "intMatch": "98",
            "intRound": "8",
            "dateEvent": "2026-07-09",
            "strTime": "23:00:00",
            "strStatus": "FT",
            "strEvent": "WC Match 98",
            "strHomeTeam": "Team E",
            "strAwayTeam": "Team F",
            "intHomeScore": "1",
            "intAwayScore": "0",
        },
    ]

    monkeypatch.setattr(collector, "_season_events", lambda _season: events)
    monkeypatch.setattr(collector, "get_team_records", lambda: {})

    bracket = collector.get_knockout_bracket()
    quarterfinals = {match["match_number"]: match for match in bracket["rounds"][2]["matches"]}

    assert quarterfinals[97]["home_team"] == "Team A"
    assert quarterfinals[97]["away_team"] == "Team E"
    assert quarterfinals[97]["game_status"] == "scheduled"
    assert quarterfinals[97]["winner"] is None


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
