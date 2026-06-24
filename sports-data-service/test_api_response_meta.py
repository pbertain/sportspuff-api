from datetime import date, datetime, timedelta, timezone

import pytz

from src import api, schemas


class _EmptyQuery:
    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def all(self):
        return []

    def first(self):
        return None


class _EmptyDb:
    def add(self, *args, **kwargs):
        return None

    def rollback(self):
        return None

    def query(self, *args, **kwargs):
        return _EmptyQuery()


class _EmptySession:
    def __enter__(self):
        return _EmptyDb()

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeCricketCollector:
    def __init__(self):
        self.timezone = pytz.timezone("US/Pacific")

    def set_timezone(self, timezone):
        self.timezone = timezone

    def get_source_updated_at(self, context=None):
        return "2026-06-24T07:00:00Z"

    def get_live_scores(self, target_date):
        if target_date != datetime.now(self.timezone).date():
            return []
        return [{
            "game_id": "mlc-live-1",
            "game_date": target_date.isoformat(),
            "game_time": datetime.now(timezone.utc),
            "home_team": "Texas Super Kings",
            "home_team_abbrev": "TSK",
            "visitor_team": "Mi New York",
            "visitor_team_abbrev": "MINY",
            "home_score_total": 152,
            "visitor_score_total": 149,
            "game_status": "in_progress",
            "is_final": False,
            "current_period": "",
            "time_remaining": "",
            "cricket_home_score": "152/6",
            "cricket_away_score": "149/8",
            "cricket_status": "TSK lead by 3 runs",
        }]

    def get_schedule(self, target_date):
        return [{
            "game_id": "mlc-schedule-1",
            "game_date": target_date.isoformat(),
            "game_time": datetime(2099, 6, 20, 20, 0, tzinfo=timezone.utc),
            "home_team": "Texas Super Kings",
            "home_team_abbrev": "TSK",
            "visitor_team": "San Francisco Unicorns",
            "visitor_team_abbrev": "SFU",
            "home_score_total": 0,
            "visitor_score_total": 0,
            "game_status": "scheduled",
            "is_final": False,
            "current_period": "",
            "time_remaining": "",
            "cricket_home_score": "",
            "cricket_away_score": "",
            "cricket_status": "scheduled",
            "cricket_start_time": {"pt": "1:00PM PDT", "ist": "1:30AM IST"},
        }]


class _FakeWorldCupCollector:
    def get_source_updated_at(self, context=None):
        return "2026-06-24T06:30:00Z"

    def get_standings(self):
        return [{
            "rank": 1,
            "team_name": "Brazil",
            "abbreviation": "BRA",
            "matches": 1,
            "wins": 1,
            "draws": 0,
            "losses": 0,
            "goals_for": 2,
            "goals_against": 0,
            "goal_difference": 2,
            "points": 3,
            "record": "1-0-0",
            "group": "C",
            "group_rank": 1,
            "currently_advancing": True,
            "advancement_path": "top_two",
            "third_place_rank": None,
        }]

    def get_group_standings(self):
        return [{
            "group": "C",
            "teams": [{
                "abbreviation": "BRA",
                "matches": 1,
                "wins": 1,
                "draws": 0,
                "losses": 0,
                "goals_for": 2,
                "goals_against": 0,
                "goal_difference": 2,
                "points": 3,
                "group_rank": 1,
                "currently_advancing": True,
            }],
        }]

    def get_knockout_bracket(self):
        return {"round_of_32": []}


class _FakeSeasonInfoCollector:
    def get_source_updated_at(self, context=None):
        return "2026-06-24T05:45:00Z"

    def get_season_info(self):
        return {
            "year": 2026,
            "current_phase": "Regular Season",
            "season_types": [{
                "name": "Regular Season",
                "start_date": "2026-04-01",
                "end_date": "2026-09-30",
            }],
        }


def _collector_cache_key(league: str, target_date: date, timezone_name: str) -> str:
    return f"{league}:{target_date.isoformat()}:{timezone_name}"


def test_schedule_response_includes_meta(monkeypatch):
    api._collector_cache.clear()
    api._inflight_fetches.clear()
    monkeypatch.setattr(api, "get_collector", lambda league: _FakeCricketCollector())
    monkeypatch.setattr(api, "get_db_session", lambda: _EmptySession())
    monkeypatch.setattr(api, "_apply_dict_enrichers", lambda sport, target_date, batch: None)

    payload = api.get_schedule_api_v1("mlc", "2099-06-20", None)
    validated = schemas.ScheduleResponse.model_validate(payload)

    assert validated.games[0].game_id == "mlc-schedule-1"
    assert validated.meta is not None
    assert validated.meta.cache_age_seconds == 0
    assert validated.meta.stale is False
    assert validated.meta.source_updated_at == "2026-06-24T07:00:00Z"


def test_scores_response_meta_reflects_cached_age(monkeypatch):
    api._collector_cache.clear()
    api._inflight_fetches.clear()
    pacific = pytz.timezone("US/Pacific")
    today = datetime.now(pacific).date()
    cache_key = _collector_cache_key("MLC", today, pacific.zone)

    monkeypatch.setattr(api, "get_collector", lambda league: _FakeCricketCollector())
    monkeypatch.setattr(api, "get_db_session", lambda: _EmptySession())
    monkeypatch.setattr(api, "_apply_dict_enrichers", lambda sport, target_date, batch: None)

    first_payload = api.get_scores_api_v1("mlc", today.isoformat(), None)
    assert first_payload["meta"]["cache_age_seconds"] == 0

    api._collector_cache[cache_key]["timestamp"] = api._time.time() - 180
    second_payload = api.get_scores_api_v1("mlc", today.isoformat(), None)
    validated = schemas.ScoresResponse.model_validate(second_payload)

    assert validated.scores[0].game_id == "mlc-live-1"
    assert validated.meta is not None
    assert validated.meta.cache_age_seconds >= 180
    assert validated.meta.stale is True
    assert validated.meta.source_updated_at == "2026-06-24T07:00:00Z"


def test_standings_response_meta_reflects_cache_age(monkeypatch):
    api._standings_cache.clear()
    monkeypatch.setattr(api, "get_collector", lambda league: _FakeWorldCupCollector())

    first_payload = api.get_standings_api_v1("wc")
    assert first_payload["meta"]["cache_age_seconds"] == 0

    api._standings_cache["wc"]["timestamp"] = api._time.time() - 1200
    second_payload = api.get_standings_api_v1("wc")
    validated = schemas.StandingsResponse.model_validate(second_payload)

    assert validated.groups is not None
    assert validated.groups[0]["group"] == "C"
    assert validated.meta is not None
    assert validated.meta.cache_age_seconds >= 1200
    assert validated.meta.stale is True
    assert validated.meta.source_updated_at == "2026-06-24T06:30:00Z"


def test_season_info_response_meta_reflects_cache_age(monkeypatch):
    from src.services import champions

    api._season_info_cache.clear()
    monkeypatch.setattr(api, "get_collector", lambda league: _FakeSeasonInfoCollector())
    monkeypatch.setattr(champions, "get_last_champion", lambda league: None)

    first_payload = api.get_season_info("nba")
    assert first_payload["meta"]["cache_age_seconds"] == 0

    api._season_info_cache["NBA"]["timestamp"] = api._time.time() - 42
    second_payload = api.get_season_info("nba")
    validated = schemas.SeasonInfoResponse.model_validate(second_payload)

    assert validated.current_phase == "Regular Season"
    assert validated.meta is not None
    assert validated.meta.cache_age_seconds >= 42
    assert validated.meta.stale is False
    assert validated.meta.source_updated_at == "2026-06-24T05:45:00Z"
