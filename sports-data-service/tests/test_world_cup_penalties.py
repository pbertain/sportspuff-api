from datetime import date

from src import api
from src.services import box_score


class _FakeResponse:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def test_wc_box_score_enricher_captures_shootout_scores(monkeypatch):
    box_score._cache.clear()

    payload = {
        "events": [
            {
                "competitions": [
                    {
                        "competitors": [
                            {
                                "team": {"displayName": "Argentina"},
                                "shootoutScore": 4,
                            },
                            {
                                "team": {"displayName": "France"},
                                "shootoutScore": 2,
                            },
                        ]
                    }
                ]
            }
        ]
    }

    monkeypatch.setattr(box_score.requests, "get", lambda *args, **kwargs: _FakeResponse(payload))

    games = [{"home_team": "Argentina", "visitor_team": "France"}]
    box_score.enrich_games("wc", date(2022, 12, 18), games)

    assert games[0]["home_shootout_score"] == 4
    assert games[0]["visitor_shootout_score"] == 2


def test_wc_box_score_includes_pk_column_when_shootout_scores_exist():
    games = [
        {
            "home_team": "Argentina",
            "visitor_team": "France",
            "home_score": 3,
            "visitor_score": 3,
            "home_shootout_score": 4,
            "visitor_shootout_score": 2,
        }
    ]

    api._apply_box_score("wc", games)

    assert games[0]["box_score"] == {
        "columns": ["PK", "F"],
        "home": [4, 3],
        "visitor": [2, 3],
    }
