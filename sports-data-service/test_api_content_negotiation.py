import asyncio
import json
from types import SimpleNamespace

from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request

from src import api, schemas


def _request_with_accept(accept: str | None) -> Request:
    headers = []
    if accept is not None:
        headers.append((b"accept", accept.encode("latin-1")))
    scope = {"type": "http", "headers": headers}
    return Request(scope)


def test_all_sports_scores_payload_matches_schema(monkeypatch):
    game = SimpleNamespace(
        game_id="nba-1",
        game_date="2026-06-16",
        game_time=None,
        home_team="Lakers",
        home_team_abbrev="LAL",
        visitor_team="Celtics",
        visitor_team_abbrev="BOS",
        game_status="final",
        game_type="regular",
        home_score_total=112,
        visitor_score_total=108,
        is_final=True,
        current_period="4",
        time_remaining="0:00",
        home_wins=50,
        home_losses=20,
        home_otl=0,
        visitor_wins=48,
        visitor_losses=22,
        visitor_otl=0,
        home_period_scores={},
        visitor_period_scores={},
    )

    monkeypatch.setattr(api, "_get_all_sport_games", lambda target_date, timezone: {"nba": [game]})

    payload = api.get_scores_all_sports_api_v1("2026-06-16", None)
    validated = schemas.AllSportsScoresResponse.model_validate(payload)

    assert validated.date == "2026-06-16"
    assert validated.sports["nba"][0].game_id == "nba-1"


def test_v1_scores_defaults_to_json(monkeypatch):
    monkeypatch.setattr(api, "get_scores_all_sports_api_v1", lambda date, tz: {"mode": "json", "date": date})
    monkeypatch.setattr(api, "get_scores_all_sports_curl_v1", lambda date, tz: "plain")

    response = api.get_scores_all_sports_v1(_request_with_accept(None), "today", None)

    assert response == {"mode": "json", "date": "today"}


def test_v1_scores_honors_text_plain_accept(monkeypatch):
    monkeypatch.setattr(api, "get_scores_all_sports_api_v1", lambda date, tz: {"mode": "json", "date": date})
    monkeypatch.setattr(api, "get_scores_all_sports_curl_v1", lambda date, tz: "plain")

    response = api.get_scores_all_sports_v1(_request_with_accept("text/plain"), "today", None)

    assert response.media_type == "text/plain"
    assert response.body.decode("utf-8") == "plain"


def test_json_routes_return_structured_internal_errors(monkeypatch):
    def boom(target_date, timezone):
        raise RuntimeError("db password leaked")

    monkeypatch.setattr(api, "_get_all_sport_games", boom)

    response = api.get_scores_all_sports_api_v1("today", None)

    assert response.status_code == 500
    assert response.body.decode("utf-8") == (
        '{"error":{"code":"internal_server_error","message":"Internal server error",'
        '"route":"/api/v1/scores/{date}"}}'
    )


def test_help_json_marks_v1_canonical_and_legacy_routes():
    help_json = api.get_help_json()

    assert "GET /v1/scores/{date} with Accept: application/json or text/plain" in help_json["endpoints"]["scores"]["canonical"]
    assert "/api/v1/scores/{date} - JSON compatibility route" in help_json["endpoints"]["scores"]["legacy_compatibility"]
    assert "GET /v1/season-info/{league} - Season dates for a league" in help_json["endpoints"]["season_info"]["canonical"]


def test_http_exception_handler_returns_structured_json():
    request = _request_with_accept("application/json")
    request.scope["path"] = "/v1/scores/bad-date"
    request.scope["raw_path"] = b"/v1/scores/bad-date"

    response = asyncio.run(
        api.api_http_exception_handler(
            request,
            HTTPException(status_code=400, detail="Invalid date format"),
        )
    )

    assert response.status_code == 400
    assert json.loads(response.body) == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid date format",
            "route": "/v1/scores/bad-date",
        }
    }


def test_http_exception_handler_returns_plain_text_for_legacy_curl():
    request = _request_with_accept(None)
    request.scope["path"] = "/curl/v1/scores/bad-date"
    request.scope["raw_path"] = b"/curl/v1/scores/bad-date"

    response = asyncio.run(
        api.api_http_exception_handler(
            request,
            HTTPException(status_code=404, detail="Not found"),
        )
    )

    assert response.status_code == 404
    assert response.body.decode("utf-8") == "not_found: Not found (/curl/v1/scores/bad-date)"


def test_validation_exception_handler_returns_structured_json():
    request = _request_with_accept("application/json")
    request.scope["path"] = "/v1/scores/today"
    request.scope["raw_path"] = b"/v1/scores/today"
    exc = RequestValidationError([{"loc": ("query", "tz"), "msg": "Field required", "type": "missing"}])

    response = asyncio.run(api.api_validation_exception_handler(request, exc))
    payload = json.loads(response.body)

    assert response.status_code == 422
    assert payload["error"]["code"] == "validation_error"
    assert payload["error"]["message"] == "Validation failed"
    assert payload["error"]["route"] == "/v1/scores/today"
    assert payload["error"]["details"][0]["type"] == "missing"
