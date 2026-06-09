"""
Pydantic response models for the public JSON routes.

Each model mirrors what the corresponding route currently returns. We keep
`extra="allow"` on response models so future-added fields pass through to
clients without requiring a schemas.py change first — but the named fields
are documented and typed so OpenAPI codegen (e.g. openapi-typescript) gives
v6 autocomplete and contract-drift detection at build time.

Convention:
- Required fields: present on every response
- Optional fields with `= None` / default: appear when the route can compute
  them, omitted otherwise
- Sport-specific fields (cricket_*, tennis_*, cycling_*, wc_*, series_*)
  are all optional — a single GameOut model spans every sport for codegen
  simplicity. v6 should branch on `league` (or the sport-specific marker
  field's presence) to decide which subset to render.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class HealthOut(BaseModel):
    model_config = ConfigDict(extra="allow")
    status: str = Field(description='Always "healthy" when the API is up.')


# ---------------------------------------------------------------------------
# Game / scores / schedule
# ---------------------------------------------------------------------------

class CricketStartTime(BaseModel):
    model_config = ConfigDict(extra="allow")
    local: Optional[str] = Field(default=None, description="Local-timezone start time (per-request tz).")
    pt: Optional[str] = Field(default=None, description="Pacific time start time.")
    utc: Optional[str] = Field(default=None, description="UTC start time.")
    ist: Optional[str] = Field(default=None, description="India Standard Time start time.")


class GameOut(BaseModel):
    """One game/match/event across every sport. Sport-specific fields are
    all optional; v6 should look at `league` and the relevant prefixed
    fields (cricket_*, tennis_*, cycling_*, wc_*, series_*)."""
    model_config = ConfigDict(extra="allow")

    # Identity
    game_id: str
    game_date: str = Field(description="ISO date YYYY-MM-DD in the requesting timezone.")
    game_time: Optional[str] = Field(default=None, description="ISO 8601 timestamp UTC.")
    game_status: str = Field(description='"scheduled" | "in_progress" | "final" | "postponed"')
    game_type: Optional[str] = Field(default=None, description="regular | preseason | playoffs | one_day | stage | match | group_matchday_1 | round_of_16 | etc.")
    is_final: bool = False
    is_overtime: Optional[bool] = False

    # Teams (or players for tennis, race+stage for cycling)
    home_team: str
    home_team_abbrev: Optional[str] = ""
    home_team_id: Optional[str] = ""
    visitor_team: str
    visitor_team_abbrev: Optional[str] = ""
    visitor_team_id: Optional[str] = ""

    # Scores
    home_score: Union[int, str, None] = Field(default=0, description="Integer for most sports; cricket may use string like '161' or '161/3 (19.5 ov)'.")
    visitor_score: Union[int, str, None] = Field(default=0, description="Integer for most sports; cricket may use string.")

    # Records (overall season record for non-playoff games)
    home_wins: int = 0
    home_losses: int = 0
    home_otl: Optional[int] = None
    home_draws: Optional[int] = None
    visitor_wins: int = 0
    visitor_losses: int = 0
    visitor_otl: Optional[int] = None
    visitor_draws: Optional[int] = None

    # Live state
    current_period: Optional[str] = ""
    time_remaining: Optional[str] = ""

    # Common metadata
    venue: Optional[str] = None
    home_team_badge: Optional[str] = Field(default=None, description="URL to the team's logo/badge.")
    visitor_team_badge: Optional[str] = None
    league_badge: Optional[str] = None

    # ---- Per-period scoring (NBA/WNBA: q1..q4, NHL: period_1..period_3, MLB: inning_1..., NFL: q1..q4) ----
    home_period_scores: Optional[Dict[str, int]] = Field(default=None, description="Per-period score dict; key shape varies per league.")
    visitor_period_scores: Optional[Dict[str, int]] = None
    # v6-facing box_score block: aligned arrays with column labels.
    # {columns:["Q1","Q2","Q3","Q4","F"], home:[29,31,22,34,116], visitor:[24,28,25,32,109]}.
    box_score: Optional[Dict[str, Any]] = None

    # ---- Playoff series records (NBA/WNBA/WC/NHL/MLB knockouts) ----
    is_playoff: Optional[bool] = Field(default=None, description="True when this game is a playoff/knockout match.")
    series_summary: Optional[str] = Field(default=None, description='ESPN-derived series summary, e.g. "NY leads series 1-0".')
    series_round: Optional[str] = Field(default=None, description='Round name, e.g. "NBA Finals - Game 1", "Western Conference Finals - Game 7".')
    series_total: Optional[int] = Field(default=None, description="Best-of-N total games (e.g. 7 for NBA Finals).")
    series_completed: Optional[bool] = None
    home_series_wins: Optional[int] = Field(default=None, description="Wins in the current playoff series (not season).")
    home_series_losses: Optional[int] = None
    visitor_series_wins: Optional[int] = None
    visitor_series_losses: Optional[int] = None

    # ---- Cricket (IPL/MLC) ----
    cricket_home_score: Optional[str] = Field(default=None, description="Run total or full format like '161/3 (19.5 ov)' during live matches.")
    cricket_away_score: Optional[str] = None
    cricket_status: Optional[str] = Field(default=None, description='Live status text, e.g. "RCB need 12 runs in 18 balls" or "Final".')
    cricket_venue: Optional[str] = None
    cricket_winner: Optional[str] = Field(default=None, description="Abbreviation of the winning team for finished matches.")
    cricket_start_time: Optional[CricketStartTime] = None
    cricket_home_nr: Optional[int] = None
    cricket_away_nr: Optional[int] = None

    # ---- Tennis (ATP/WTA) — note: home_team / visitor_team carry player names ----
    tennis_tournament: Optional[str] = Field(default=None, description='Parsed tournament name, e.g. "Roland Garros".')
    tennis_match_label: Optional[str] = Field(default=None, description="Full strEvent string from upstream.")
    tennis_round: Optional[Union[int, str]] = Field(default=None, description="Bracket round number from upstream.")
    tennis_country: Optional[str] = None
    tennis_video: Optional[str] = None
    # Set-by-set scoring + match summary, sourced from ESPN's tennis scoreboard.
    # `tennis_set_scores` is per-set: [{"set": 1, "home": 6, "visitor": 1}, ...].
    # `home_sets_won` / `visitor_sets_won` are the counts (1 vs 2 for sets won).
    # `tennis_summary` is ESPN's one-line result (e.g. "Onclin bt Luz 6-1 6-3").
    # `tennis_winner` is "home" / "visitor" / None.
    tennis_set_scores: Optional[List[Dict[str, int]]] = None
    home_sets_won: Optional[int] = None
    visitor_sets_won: Optional[int] = None
    tennis_summary: Optional[str] = None
    tennis_winner: Optional[str] = None

    # ---- Tennis v6-facing contract (additive aliases of the fields above) ----
    # Convention: player1 = visitor, player2 = home. Populated for ATP/WTA.
    tournament_name: Optional[str] = None
    match_status: Optional[str] = None
    player1_name: Optional[str] = None
    player1_last_name: Optional[str] = None
    player1_seed: Optional[int] = None
    player1_score: Optional[List[int]] = None
    player1_sets_won: Optional[int] = None
    player2_name: Optional[str] = None
    player2_last_name: Optional[str] = None
    player2_seed: Optional[int] = None
    player2_score: Optional[List[int]] = None
    player2_sets_won: Optional[int] = None
    winner: Optional[str] = Field(default=None, description='"player1" or "player2" once final.')
    winner_name: Optional[str] = None
    venue_name: Optional[str] = None
    court_name: Optional[str] = None
    tennis_score: Optional[Dict[str, Any]] = Field(
        default=None,
        description='Combined block: {columns:["S1","S2"], player1:[...], player2:[...], winner:"player1"|"player2"}.',
    )

    # ---- Cycling (UCI World Tour) ----
    cycling_race: Optional[str] = Field(default=None, description='Parsed race name, e.g. "Tour de France".')
    cycling_stage_label: Optional[str] = Field(default=None, description='"Stage 1" / "Prologue" / "" for one-day classics.')
    cycling_stage_number: Optional[int] = None
    cycling_event_label: Optional[str] = None
    cycling_country: Optional[str] = None
    cycling_video: Optional[str] = None

    # ---- World Cup ----
    wc_round: Optional[Union[int, str]] = None
    wc_round_label: Optional[str] = Field(default=None, description='group_matchday_{1,2,3} | round_of_16 | quarterfinal | semifinal | third_place | final')


class ScoresResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    sport: str
    date: str
    scores: List[GameOut]


class ScheduleResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    sport: str
    date: str
    games: List[GameOut]


class AllSportsScheduleResponse(BaseModel):
    """Response from GET /api/v1/schedules/{date} (all sports)."""
    model_config = ConfigDict(extra="allow")
    date: str
    sports: Dict[str, List[GameOut]] = Field(description="Per-sport game lists keyed by sport slug (mlb, nba, ...).")


class AllSportsScoresResponse(BaseModel):
    """Response from GET /api/v1/scores/all/{date} (sport=all)."""
    model_config = ConfigDict(extra="allow")
    sport: str = "all"
    date: str
    scores: List[GameOut] = Field(description="Flat list of games across all sports; each entry has a `sport` field.")


# ---------------------------------------------------------------------------
# Standings
# ---------------------------------------------------------------------------

class StandingsTeamOut(BaseModel):
    """Team row in a standings table. Sport-specific fields all optional."""
    model_config = ConfigDict(extra="allow")
    rank: Optional[int] = None
    team_name: Optional[str] = None
    abbreviation: Optional[str] = None
    record: Optional[str] = Field(default=None, description='Sport-specific record string, e.g. "32-15", "10-2-2", or "5-0-1" (cricket: W-L-NR).')

    # Common
    wins: Optional[int] = None
    losses: Optional[int] = None
    win_pct: Optional[Union[float, str]] = None
    games_back: Optional[Union[float, str]] = None
    streak: Optional[str] = None
    conference: Optional[str] = None
    division: Optional[str] = None
    points: Optional[int] = None
    ties: Optional[int] = None
    ot: Optional[int] = None

    # Cricket
    matches: Optional[int] = None
    no_result: Optional[int] = None
    nrr: Optional[str] = Field(default=None, description="Net run rate (cricket).")

    # Soccer (MLS, WC)
    draws: Optional[int] = None
    goals_for: Optional[int] = None
    goals_against: Optional[int] = None
    goal_difference: Optional[int] = None


class StandingsResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    sport: str
    teams: List[StandingsTeamOut]
    available: Optional[bool] = Field(default=None, description="False for sports with no league table (tennis, cycling).")
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# Season info
# ---------------------------------------------------------------------------

class SeasonTypeOut(BaseModel):
    model_config = ConfigDict(extra="allow")
    name: str
    start_date: str
    end_date: str


class LastChampion(BaseModel):
    model_config = ConfigDict(extra="allow")
    team: str = Field(description="Full team name of the previous champion.")
    abbreviation: str = Field(description="Standard abbreviation when known.")
    year: Union[int, str] = Field(description="Season identifier (int for single-year sports, string for split-year like NBA 2024-2025).")


class SeasonInfoResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    year: Union[int, str]
    current_phase: str = Field(description='Current phase name, e.g. "Regular Season" / "Postseason" / "Off Season" / "Wimbledon" (tennis) / "Tour de France" (cycling).')
    season_types: List[SeasonTypeOut]
    last_champion: Optional[LastChampion] = Field(default=None, description="Most recent champion when known.")


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

class StatusSummary(BaseModel):
    model_config = ConfigDict(extra="allow")
    error: int = 0
    warning: int = 0
    ok: int = 0


class UpstreamRow(BaseModel):
    model_config = ConfigDict(extra="allow")
    name: str
    category: str = Field(description='"ok" | "warning" | "error"')
    last_success_at: Optional[str] = None
    last_error_at: Optional[str] = None
    last_error: Optional[str] = None
    age_seconds: Optional[int] = None
    stale: bool = False
    detail: str = ""


class ResultMeta(BaseModel):
    model_config = ConfigDict(extra="allow")
    cached_at: Optional[str] = None
    age_seconds: Optional[int] = None
    ttl_seconds: Optional[int] = None
    stale: bool = False
    source: Optional[str] = Field(default=None, description='"live" | "cache" | "db"')


class ResultRow(BaseModel):
    model_config = ConfigDict(extra="allow")
    name: str
    url: str
    category: str = Field(description='"ok" | "warning" | "error"')
    status_code: Optional[int] = None
    count: Optional[int] = None
    detail: str = ""
    upstream: Optional[str] = None
    meta: Optional[ResultMeta] = None


class StatusResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    api_base_url: str
    checked_at: str = Field(description="ISO-8601 UTC, ends with 'Z'.")
    summary: StatusSummary
    upstreams: List[UpstreamRow]
    results: List[ResultRow]


# ---------------------------------------------------------------------------
# Cricket bulk feed (for CricketPuff)
# ---------------------------------------------------------------------------

class CricketSeasonApiStats(BaseModel):
    model_config = ConfigDict(extra="allow")
    hits_today: Optional[int] = None
    hits_used: Optional[int] = None
    hits_limit: Optional[int] = None
    date: Optional[str] = None
    provider: Optional[str] = None


class CricketSeasonResponse(BaseModel):
    """Bulk feed at /api/v1/cricket/{league}/season — what CricketPuff consumes."""
    model_config = ConfigDict(extra="allow")
    league: str
    series_id: str
    series_name: str
    live: bool = Field(description="True when the response contains fresh upstream data; false when serving stale cache after upstream failure.")
    matches: List[GameOut]
    standings: List[StandingsTeamOut]
    api_stats: CricketSeasonApiStats
