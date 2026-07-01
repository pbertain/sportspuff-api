from datetime import datetime
from types import SimpleNamespace

from src.utils import adaptive_polling


class _FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 6, 30, 12, 0, 0, tzinfo=tz)


class _FakeQuery:
    def __init__(self, games):
        self._games = games

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        return self._games


class _FakeDB:
    def __init__(self, games):
        self._games = games

    def query(self, model):
        return _FakeQuery(self._games)


def test_nfl_scheduled_games_use_schedule_poll_interval(monkeypatch):
    monkeypatch.setattr(adaptive_polling, "datetime", _FrozenDateTime)

    manager = adaptive_polling.AdaptivePollingManager()
    db = _FakeDB(
        [
            SimpleNamespace(game_status="scheduled", is_final=False),
        ]
    )

    assert manager.determine_poll_interval(db, "NFL") == adaptive_polling.settings.scheduled_game_poll_interval


def test_nfl_live_games_use_live_poll_interval(monkeypatch):
    monkeypatch.setattr(adaptive_polling, "datetime", _FrozenDateTime)

    manager = adaptive_polling.AdaptivePollingManager()
    db = _FakeDB(
        [
            SimpleNamespace(game_status="in_progress", is_final=False),
        ]
    )

    assert manager.determine_poll_interval(db, "NFL") == adaptive_polling.settings.close_game_poll_interval

