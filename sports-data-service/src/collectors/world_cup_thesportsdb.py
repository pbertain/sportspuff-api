"""
FIFA World Cup collector backed by TheSportsDB (league_id 4429).

Soccer / football scoring (3 points for a win, 1 for a draw). Group-stage
standings derived from completed matches. TheSportsDB does not expose group
letters, so we keep an explicit official group map for 2026 and fall back to
fixture-graph inference for other seasons.

Knockout-round events (round of 32, 16, etc.) are added by TheSportsDB
once group standings are decided; until then only group matches appear
in eventsseason.php.
"""

from __future__ import annotations

import logging
import unicodedata
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytz

from .thesportsdb import TheSportsDBCollector

logger = logging.getLogger(__name__)


WC_TEAM_ABBREVS: Dict[str, str] = {
    "Algeria": "ALG",
    "Argentina": "ARG",
    "Australia": "AUS",
    "Austria": "AUT",
    "Belgium": "BEL",
    "Bosnia and Herzegovina": "BIH",
    "Bosnia-Herzegovina": "BIH",
    "Brazil": "BRA",
    "Canada": "CAN",
    "Cabo Verde": "CPV",
    "Cape Verde": "CPV",
    "Colombia": "COL",
    "Congo DR": "COD",
    "DR Congo": "COD",
    "Curacao": "CUW",
    "Czech Republic": "CZE",
    "Czechia": "CZE",
    "Cote d'Ivoire": "CIV",
    "Ivory Coast": "CIV",
    "Ecuador": "ECU",
    "Egypt": "EGY",
    "England": "ENG",
    "France": "FRA",
    "Germany": "GER",
    "Ghana": "GHA",
    "Haiti": "HAI",
    "Iran": "IRN",
    "Iraq": "IRQ",
    "Japan": "JPN",
    "Jordan": "JOR",
    "Korea Republic": "KOR",
    "South Korea": "KOR",
    "Saudi Arabia": "KSA",
    "Morocco": "MAR",
    "Mexico": "MEX",
    "Netherlands": "NED",
    "Norway": "NOR",
    "New Zealand": "NZL",
    "Panama": "PAN",
    "Paraguay": "PAR",
    "Portugal": "POR",
    "Qatar": "QAT",
    "Scotland": "SCO",
    "Senegal": "SEN",
    "South Africa": "RSA",
    "Spain": "ESP",
    "Sweden": "SWE",
    "Switzerland": "SUI",
    "Tunisia": "TUN",
    "Turkey": "TUR",
    "Turkiye": "TUR",
    "Uruguay": "URU",
    "USA": "USA",
    "United States": "USA",
    "United States of America": "USA",
    "Uzbekistan": "UZB",
}

WC_2026_GROUPS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("A", ("Mexico", "South Korea", "Czech Republic", "South Africa")),
    ("B", ("Canada", "Switzerland", "Bosnia-Herzegovina", "Qatar")),
    ("C", ("Brazil", "Morocco", "Scotland", "Haiti")),
    ("D", ("United States", "Australia", "Paraguay", "Turkey")),
    ("E", ("Germany", "Ivory Coast", "Ecuador", "Curacao")),
    ("F", ("Netherlands", "Japan", "Sweden", "Tunisia")),
    ("G", ("Egypt", "Iran", "Belgium", "New Zealand")),
    ("H", ("Spain", "Uruguay", "Cape Verde", "Saudi Arabia")),
    ("I", ("France", "Norway", "Senegal", "Iraq")),
    ("J", ("Argentina", "Austria", "Algeria", "Jordan")),
    ("K", ("Portugal", "DR Congo", "Uzbekistan", "Colombia")),
    ("L", ("England", "Croatia", "Ghana", "Panama")),
)

WC_2026_GROUP_ALIASES: Dict[str, str] = {
    "Bosnia and Herzegovina": "Bosnia-Herzegovina",
    "Cabo Verde": "Cape Verde",
    "Congo DR": "DR Congo",
    "Cote d'Ivoire": "Ivory Coast",
    "Curaçao": "Curacao",
    "Czechia": "Czech Republic",
    "Korea Republic": "South Korea",
    "Turkiye": "Turkey",
    "United States of America": "United States",
    "USA": "United States",
}


class WorldCupTheSportsDBCollector(TheSportsDBCollector):
    LEAGUE_ID = 4429
    SPORTSPUFF_CODE = "WC"
    GROUP_LABELS = tuple("ABCDEFGHIJKL")
    ROUND_OF_32_SLOTS = (
        (73, "Runner-up Group A", "Runner-up Group B"),
        (74, "Winner Group C", "Runner-up Group F"),
        (75, "Winner Group E", "3rd Group A/B/C/D/F"),
        (76, "Winner Group F", "Runner-up Group C"),
        (77, "Runner-up Group E", "Runner-up Group I"),
        (78, "Winner Group I", "3rd Group C/D/F/G/H"),
        (79, "Winner Group A", "3rd Group C/E/F/H/I"),
        (80, "Winner Group L", "3rd Group E/H/I/J/K"),
        (81, "Winner Group G", "3rd Group A/E/H/I/J"),
        (82, "Winner Group D", "3rd Group B/E/F/I/J"),
        (83, "Winner Group H", "Runner-up Group J"),
        (84, "Runner-up Group K", "Runner-up Group L"),
        (85, "Winner Group B", "3rd Group E/F/G/I/J"),
        (86, "Runner-up Group D", "Runner-up Group G"),
        (87, "Winner Group J", "Runner-up Group H"),
        (88, "Winner Group K", "3rd Group D/E/I/J/L"),
    )
    ROUND_OF_16_MATCHES = tuple(range(89, 97))
    QUARTERFINAL_MATCHES = tuple(range(97, 101))
    SEMIFINAL_MATCHES = tuple(range(101, 103))
    THIRD_PLACE_MATCH = 103
    FINAL_MATCH = 104

    def __init__(self):
        super().__init__("WC")
        self.timezone = pytz.timezone("US/Pacific")

    def current_season(self) -> str:
        """FIFA World Cup runs every 4 years. Use the most recent year that
        is a WC year and <= current year. Hosted years: 2022, 2026, 2030.
        For 2025 we'd return '2026' (the upcoming WC); for 2027 we'd
        return '2026' (the most recently completed WC)."""
        n = datetime.now(timezone.utc)
        # The set of WC years TheSportsDB has seasons for. Update as the
        # tournament rolls forward.
        wc_years = (2014, 2018, 2022, 2026, 2030)
        # If we're inside a WC year, use it; otherwise use the latest
        # past WC year.
        if n.year in wc_years:
            return str(n.year)
        past = [y for y in wc_years if y <= n.year]
        return str(max(past)) if past else "2026"

    def _parse_event(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            home = raw.get("strHomeTeam") or ""
            away = raw.get("strAwayTeam") or ""
            if not home or not away:
                return None

            dt = self._parse_event_datetime(raw)
            game_date = self._local_date(raw) or (dt.date() if dt else datetime.now().date())

            home_score = self._parse_int(raw.get("intHomeScore"))
            away_score = self._parse_int(raw.get("intAwayScore"))
            status = self._normalize_status(raw)
            is_final = status == "final"

            game_type = self._round_label(raw)

            return {
                "league": "WC",
                "game_id": str(raw.get("idEvent") or ""),
                "game_date": game_date.strftime("%Y-%m-%d"),
                "game_time": dt,
                "game_type": game_type,
                "home_team": home,
                "home_team_abbrev": self._team_abbrev(home, raw.get("strHomeTeamShort")),
                "home_team_id": str(raw.get("idHomeTeam") or ""),
                "home_wins": 0,
                "home_losses": 0,
                "home_score_total": home_score,
                "visitor_team": away,
                "visitor_team_abbrev": self._team_abbrev(away, raw.get("strAwayTeamShort")),
                "visitor_team_id": str(raw.get("idAwayTeam") or ""),
                "visitor_wins": 0,
                "visitor_losses": 0,
                "visitor_score_total": away_score,
                "game_status": status,
                "current_period": "",
                "time_remaining": "",
                "is_final": is_final,
                "is_overtime": False,
                "home_period_scores": {},
                "visitor_period_scores": {},
                "venue": raw.get("strVenue") or "",
                "home_team_badge": raw.get("strHomeTeamBadge") or "",
                "visitor_team_badge": raw.get("strAwayTeamBadge") or "",
                # World Cup-specific
                "wc_round": raw.get("intRound") or "",
                "wc_round_label": game_type,
                "wc_winner": self._winner_from_scores(home, away, home_score, away_score, is_final),
            }
        except Exception as e:
            logger.error("WorldCup parse error: %s", e)
            return None

    @staticmethod
    def _round_label(raw: Dict[str, Any]) -> str:
        """Map intRound to a human-readable phase. TheSportsDB uses 1/2/3
        for matchdays 1-3 of the group stage; knockout round numbers vary
        per tournament. Fall back to 'group_stage' or 'knockout' generically."""
        event_name = (raw.get("strEvent") or raw.get("strFilename") or "").lower()
        if "third" in event_name or "3rd" in event_name:
            return "third_place"
        if "round of 32" in event_name:
            return "round_of_32"
        if "round of 16" in event_name:
            return "round_of_16"
        if "quarter" in event_name:
            return "quarterfinal"
        if "semi" in event_name:
            return "semifinal"
        if "final" in event_name and "semi" not in event_name and "quarter" not in event_name and "round of 16" not in event_name and "round of 32" not in event_name:
            return "final"
        try:
            r = int(raw.get("intRound") or 0)
        except Exception:
            return "group_stage"
        if r in (1, 2, 3):
            return f"group_matchday_{r}"
        if r in (32,):
            return "round_of_32"
        if r in (16,):
            return "round_of_16"
        if r in (8,):
            return "quarterfinal"
        if r in (4,):
            return "semifinal"
        if r in (2, 1):
            return "final"
        return "knockout"

    # ---- soccer-style standings (3-1-0 points) -----------------------------
    def get_standings(self) -> List[Dict[str, Any]]:
        groups = self.get_group_standings()
        flat = [team for group in groups for team in group["teams"]]
        flat.sort(key=lambda r: (-r["points"], -r["goal_difference"], -r["goals_for"], r["team_name"]))
        for rank, rec in enumerate(flat, 1):
            rec["rank"] = rank
        return flat

    def get_team_records(self) -> Dict[str, Dict[str, Any]]:
        """Return a lookup table for team records keyed by normalized name and abbreviation.

        This is used to decorate live schedule rows and the knockout lattice with
        the current group-stage record for every team we know about.
        """
        records: Dict[str, Dict[str, Any]] = {}
        for group in self.get_group_standings():
            for rec in group.get("teams", []):
                payload = {
                    "team_name": rec.get("team_name", ""),
                    "abbreviation": rec.get("abbreviation", ""),
                    "group": rec.get("group", ""),
                    "group_rank": rec.get("group_rank"),
                    "rank": rec.get("rank"),
                    "matches": rec.get("matches", 0),
                    "wins": rec.get("wins", 0),
                    "draws": rec.get("draws", 0),
                    "losses": rec.get("losses", 0),
                    "goals_for": rec.get("goals_for", 0),
                    "goals_against": rec.get("goals_against", 0),
                    "goal_difference": rec.get("goal_difference", 0),
                    "points": rec.get("points", 0),
                    "record": rec.get("record", ""),
                    "currently_advancing": rec.get("currently_advancing", False),
                    "advancement_path": rec.get("advancement_path", "not_advancing"),
                    "third_place_rank": rec.get("third_place_rank"),
                }
                for key in {
                    self._normalize_team_name(rec.get("team_name", "")),
                    (rec.get("abbreviation") or "").strip().upper(),
                }:
                    if key:
                        records[key] = payload
        return records

    def _lookup_team_record(self, team_name: str, records: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Resolve a team name or abbreviation against the World Cup record map."""
        normalized = self._normalize_team_name(team_name)
        if not normalized:
            return None
        direct = records.get(normalized)
        if direct:
            return direct
        abbrev = normalized.strip().upper()
        if abbrev:
            return records.get(abbrev)
        return None

    def get_group_standings(self) -> List[Dict[str, Any]]:
        """Return group-stage standings split into World Cup groups."""
        season = self.current_season()
        try:
            events = self._season_events(season)
        except Exception as e:
            logger.error("WC standings: cannot fetch season: %s", e)
            return []

        group_lookup, group_order = self._infer_groups(events)
        records: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"matches": 0, "wins": 0, "draws": 0, "losses": 0,
                     "goals_for": 0, "goals_against": 0}
        )

        seeded_teams = set()
        for raw in events:
            try:
                r = int(raw.get("intRound") or 0)
            except Exception:
                r = 0
            if r not in (1, 2, 3):
                continue
            home = raw.get("strHomeTeam") or ""
            away = raw.get("strAwayTeam") or ""
            if home:
                seeded_teams.add(home)
            if away:
                seeded_teams.add(away)

        for team in seeded_teams:
            group = self._group_for_team(team, group_lookup)
            if group:
                records[team]["group"] = group

        for raw in events:
            if self._normalize_status(raw) != "final":
                continue
            try:
                r = int(raw.get("intRound") or 0)
            except Exception:
                r = 0
            # Standings are only meaningful for the group stage (rounds 1-3).
            # Knockouts don't roll up to a points table.
            if r not in (1, 2, 3):
                continue
            home = raw.get("strHomeTeam") or ""
            away = raw.get("strAwayTeam") or ""
            hs = self._parse_int(raw.get("intHomeScore"), default=-1)
            as_ = self._parse_int(raw.get("intAwayScore"), default=-1)
            if not home or not away or hs < 0 or as_ < 0:
                continue
            r_h = records[home]; r_a = records[away]
            r_h["group"] = self._group_for_team(home, group_lookup) or r_h.get("group", "")
            r_a["group"] = self._group_for_team(away, group_lookup) or r_a.get("group", "")
            r_h["matches"] += 1; r_a["matches"] += 1
            r_h["goals_for"] += hs; r_h["goals_against"] += as_
            r_a["goals_for"] += as_; r_a["goals_against"] += hs
            if hs == as_:
                r_h["draws"] += 1; r_a["draws"] += 1
            elif hs > as_:
                r_h["wins"] += 1; r_a["losses"] += 1
            else:
                r_a["wins"] += 1; r_h["losses"] += 1

        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for team_name, rec in records.items():
            group = rec.get("group") or self._group_for_team(team_name, group_lookup) or ""
            points = rec["wins"] * 3 + rec["draws"]
            gd = rec["goals_for"] - rec["goals_against"]
            grouped[group].append({
                "team_name": team_name,
                "abbreviation": self._team_abbrev(team_name),
                "group": group,
                "matches": rec["matches"],
                "wins": rec["wins"],
                "draws": rec["draws"],
                "losses": rec["losses"],
                "goals_for": rec["goals_for"],
                "goals_against": rec["goals_against"],
                "goal_difference": gd,
                "points": points,
                "record": f"{rec['wins']}-{rec['draws']}-{rec['losses']}",
            })

        groups = []
        for group in group_order:
            teams = grouped.get(group, [])
            teams.sort(key=lambda r: (-r["points"], -r["goal_difference"], -r["goals_for"], r["team_name"]))
            for rank, rec in enumerate(teams, 1):
                rec["group_rank"] = rank
                rec["rank"] = rank
                rec["currently_advancing"] = rank <= 2
                rec["advancement_path"] = "top_two" if rank <= 2 else "not_advancing"
                rec["third_place_rank"] = None
            groups.append({"group": group, "teams": teams})

        # If upstream data is incomplete and a team could not be assigned, keep
        # the data visible instead of silently dropping it.
        if grouped.get(""):
            teams = grouped[""]
            teams.sort(key=lambda r: (-r["points"], -r["goal_difference"], -r["goals_for"], r["team_name"]))
            for rank, rec in enumerate(teams, 1):
                rec["group_rank"] = rank
                rec["rank"] = rank
                rec["currently_advancing"] = False
                rec["advancement_path"] = "not_advancing"
                rec["third_place_rank"] = None
            groups.append({"group": "", "teams": teams})

        self._mark_best_third_place_advancers(groups)
        return groups

    @staticmethod
    def _team_abbrev(team_name: str, upstream_short: Optional[str] = None) -> str:
        team_name = WorldCupTheSportsDBCollector._normalize_team_name(team_name)
        if team_name in WC_TEAM_ABBREVS:
            return WC_TEAM_ABBREVS[team_name]
        upstream_short = (upstream_short or "").strip().upper()
        if upstream_short:
            return upstream_short
        return (team_name[:3] or "").upper()

    @staticmethod
    def _normalize_team_name(team_name: str) -> str:
        normalized = unicodedata.normalize("NFKD", team_name or "")
        ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
        return " ".join(ascii_name.strip().split())

    @staticmethod
    def _mark_best_third_place_advancers(groups: List[Dict[str, Any]]) -> None:
        third_place_rows = []
        for group in groups:
            for rec in group.get("teams", []):
                if rec.get("group_rank") == 3:
                    third_place_rows.append(rec)

        third_place_rows.sort(
            key=lambda r: (
                -r.get("points", 0),
                -r.get("goal_difference", 0),
                -r.get("goals_for", 0),
                r.get("team_name", ""),
            )
        )

        for index, rec in enumerate(third_place_rows, 1):
            rec["third_place_rank"] = index
            if index <= 8:
                rec["currently_advancing"] = True
                rec["advancement_path"] = "best_third_place"

    def get_knockout_bracket(self) -> Dict[str, Any]:
        """Return bracket data for the 32-team knockout stage.

        The bracket is synthesized from the Round of 32 and then propagated
        forward so later rounds have the right lattice even before TheSportsDB
        publishes the downstream fixtures. If upstream later-round events are
        present, they are merged into the generated structure.
        """
        from ..services.box_score import enrich_games as _enrich_box

        season = self.current_season()
        try:
            events = self._season_events(season)
        except Exception as e:
            logger.error("WC bracket: cannot fetch season: %s", e)
            events = []

        team_records = self.get_team_records()
        by_match = {}
        by_round: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        round_of_32_events = []
        for raw in events:
            try:
                round_num = int(raw.get("intRound") or 0)
            except Exception:
                round_num = 0
            if round_num <= 3:
                continue
            parsed = self._parse_event(raw)
            if not parsed:
                continue
            round_label = parsed.get("wc_round_label") or "knockout"
            by_round[round_label].append(parsed)
            if round_label == "round_of_32":
                round_of_32_events.append((raw.get("dateEvent") or "", raw.get("strTime") or "", parsed))
            match_number = self._parse_int(raw.get("intMatch") or raw.get("intEvent") or raw.get("idEvent"), default=0)
            if match_number:
                by_match[match_number] = parsed

        for index, (_, _, parsed) in enumerate(sorted(round_of_32_events, key=lambda item: (item[0], item[1])), 0):
            if index >= len(self.ROUND_OF_32_SLOTS):
                break
            match_number = self.ROUND_OF_32_SLOTS[index][0]
            by_match.setdefault(match_number, parsed)

        round_of_32 = []
        for match_number, home_slot, away_slot in self.ROUND_OF_32_SLOTS:
            actual = by_match.get(match_number)
            home_record = self._lookup_team_record(actual.get("home_team"), team_records) if actual else None
            away_record = self._lookup_team_record(actual.get("visitor_team"), team_records) if actual else None
            home_score = actual.get("home_score_total") if actual else None
            away_score = actual.get("visitor_score_total") if actual else None
            home_so = None
            away_so = None
            if actual and actual.get("game_date") and actual.get("home_team") and actual.get("visitor_team"):
                try:
                    temp_game = [{
                        "home_team": actual.get("home_team"),
                        "visitor_team": actual.get("visitor_team"),
                        "game_date": actual.get("game_date"),
                        "home_period_scores": {},
                        "visitor_period_scores": {},
                        "home_shootout_score": None,
                        "visitor_shootout_score": None,
                    }]
                    _enrich_box("wc", datetime.strptime(actual.get("game_date"), "%Y-%m-%d").date(), temp_game)
                    home_so = temp_game[0].get("home_shootout_score")
                    away_so = temp_game[0].get("visitor_shootout_score")
                except Exception:
                    home_so = None
                    away_so = None
            winner = actual.get("wc_winner") if actual else None
            if not winner and home_so is not None and away_so is not None and home_so != away_so and actual:
                winner = actual.get("home_team") if int(home_so or 0) > int(away_so or 0) else actual.get("visitor_team")
            if not winner and actual:
                winner = self._winner_from_game(actual)
            round_of_32.append({
                "match_number": match_number,
                "home_slot": home_slot,
                "away_slot": away_slot,
                "home_team": actual.get("home_team") if actual else None,
                "away_team": actual.get("visitor_team") if actual else None,
                "game_id": actual.get("game_id") if actual else None,
                "game_date": actual.get("game_date") if actual else None,
                "game_time": actual.get("game_time") if actual else None,
                "game_status": actual.get("game_status") if actual else "scheduled",
                "home_score": home_score,
                "visitor_score": away_score,
                "away_score": away_score,
                "home_score_total": home_score,
                "visitor_score_total": away_score,
                "home_shootout_score": home_so,
                "visitor_shootout_score": away_so,
                "winner": winner,
                "wc_winner": winner,
                "home_wins": home_record.get("wins") if home_record else None,
                "home_draws": home_record.get("draws") if home_record else None,
                "home_losses": home_record.get("losses") if home_record else None,
                "home_record": home_record.get("record") if home_record else None,
                "away_record": away_record.get("record") if away_record else None,
                "away_wins": away_record.get("wins") if away_record else None,
                "away_draws": away_record.get("draws") if away_record else None,
                "away_losses": away_record.get("losses") if away_record else None,
                "home_group": home_record.get("group") if home_record else None,
                "away_group": away_record.get("group") if away_record else None,
                "home_group_rank": home_record.get("group_rank") if home_record else None,
                "away_group_rank": away_record.get("group_rank") if away_record else None,
                "home_currently_advancing": home_record.get("currently_advancing") if home_record else None,
                "away_currently_advancing": away_record.get("currently_advancing") if away_record else None,
            })

        round_of_16 = self._build_knockout_round(
            round_name="round_of_16",
            match_numbers=self.ROUND_OF_16_MATCHES,
            source_round=round_of_32,
            by_round=by_round,
            team_records=team_records,
        )
        quarterfinal = self._build_knockout_round(
            round_name="quarterfinal",
            match_numbers=self.QUARTERFINAL_MATCHES,
            source_round=round_of_16,
            by_round=by_round,
            team_records=team_records,
        )
        semifinal = self._build_knockout_round(
            round_name="semifinal",
            match_numbers=self.SEMIFINAL_MATCHES,
            source_round=quarterfinal,
            by_round=by_round,
            team_records=team_records,
        )
        third_place = self._build_third_place_match(
            source_round=semifinal,
            by_round=by_round,
            team_records=team_records,
        )
        final = self._build_knockout_round(
            round_name="final",
            match_numbers=(self.FINAL_MATCH,),
            source_round=semifinal,
            by_round=by_round,
            team_records=team_records,
        )

        return {
            "format": "round_of_32",
            "sides": {
                "left": round_of_32[:8],
                "right": round_of_32[8:],
            },
            "rounds": [
                {"name": "Round of 32", "matches": round_of_32},
                {"name": "Round of 16", "matches": round_of_16},
                {"name": "Quarter-final", "matches": quarterfinal},
                {"name": "Semi-final", "matches": semifinal},
                {"name": "Third Place", "matches": [third_place] if third_place else []},
                {"name": "Final", "matches": final if final else []},
            ],
        }

    def _build_knockout_round(
        self,
        round_name: str,
        match_numbers: Tuple[int, ...],
        source_round: List[Dict[str, Any]],
        by_round: Dict[str, List[Dict[str, Any]]],
        team_records: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        actuals = sorted(by_round.get(round_name, []), key=lambda g: ((g.get("game_date") or ""), (g.get("game_time") or "")))
        matches: List[Dict[str, Any]] = []
        for index, match_number in enumerate(match_numbers):
            source_index = index * 2
            home_source = source_round[source_index] if source_index < len(source_round) else None
            away_source = source_round[source_index + 1] if source_index + 1 < len(source_round) else None
            actual = actuals[index] if index < len(actuals) else None
            matches.append(self._build_knockout_match(
                match_number=match_number,
                round_name=round_name,
                actual=actual,
                home_source=home_source,
                away_source=away_source,
                team_records=team_records,
            ))
        return matches

    def _build_third_place_match(
        self,
        source_round: List[Dict[str, Any]],
        by_round: Dict[str, List[Dict[str, Any]]],
        team_records: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        actuals = sorted(by_round.get("third_place", []), key=lambda g: ((g.get("game_date") or ""), (g.get("game_time") or "")))
        actual = actuals[0] if actuals else None
        home_source = source_round[0] if len(source_round) > 0 else None
        away_source = source_round[1] if len(source_round) > 1 else None
        return self._build_knockout_match(
            match_number=self.THIRD_PLACE_MATCH,
            round_name="third_place",
            actual=actual,
            home_source=home_source,
            away_source=away_source,
            team_records=team_records,
            source_team_mode="loser",
        )

    def _build_knockout_match(
        self,
        match_number: int,
        round_name: str,
        actual: Optional[Dict[str, Any]],
        home_source: Optional[Dict[str, Any]],
        away_source: Optional[Dict[str, Any]],
        team_records: Dict[str, Dict[str, Any]],
        source_team_mode: str = "winner",
    ) -> Dict[str, Any]:
        if source_team_mode == "loser":
            home_team = self._source_match_loser(home_source)
            away_team = self._source_match_loser(away_source)
        else:
            home_team = self._source_match_winner(home_source)
            away_team = self._source_match_winner(away_source)
        home_record = self._lookup_team_record(home_team, team_records) if home_team else None
        away_record = self._lookup_team_record(away_team, team_records) if away_team else None

        home_game = self._coalesce_knockout_team(actual, "home_team", home_team)
        away_game = self._coalesce_knockout_team(actual, "visitor_team", away_team)
        slot_prefix = "Loser" if source_team_mode == "loser" else "Winner"
        home_slot = self._source_slot_label(home_source, prefix=slot_prefix)
        away_slot = self._source_slot_label(away_source, prefix=slot_prefix)

        home_score = actual.get("home_score_total") if actual else None
        away_score = actual.get("visitor_score_total") if actual else None
        home_so = actual.get("home_shootout_score") if actual else None
        away_so = actual.get("visitor_shootout_score") if actual else None
        winner = actual.get("wc_winner") if actual else None
        if not winner and actual:
            if home_so is not None and away_so is not None and home_so != away_so:
                winner = home_game if int(home_so or 0) > int(away_so or 0) else away_game
            else:
                winner = self._winner_from_game(actual)
        if not winner and actual and actual.get("game_status") == "final":
            winner = self._winner_from_game(actual)

        match = {
            "match_number": match_number,
            "home_slot": home_slot,
            "away_slot": away_slot,
            "home_team": home_game,
            "away_team": away_game,
            "game_id": actual.get("game_id") if actual else None,
            "game_date": actual.get("game_date") if actual else None,
            "game_time": actual.get("game_time") if actual else None,
            "game_status": actual.get("game_status") if actual else "scheduled",
            "home_score": home_score,
            "visitor_score": away_score,
            "away_score": away_score,
            "home_score_total": home_score,
            "visitor_score_total": away_score,
            "home_shootout_score": home_so,
            "visitor_shootout_score": away_so,
            "winner": winner,
            "wc_winner": winner,
            "home_wins": home_record.get("wins") if home_record else None,
            "home_draws": home_record.get("draws") if home_record else None,
            "home_losses": home_record.get("losses") if home_record else None,
            "home_record": home_record.get("record") if home_record else None,
            "away_record": away_record.get("record") if away_record else None,
            "away_wins": away_record.get("wins") if away_record else None,
            "away_draws": away_record.get("draws") if away_record else None,
            "away_losses": away_record.get("losses") if away_record else None,
            "home_group": home_record.get("group") if home_record else None,
            "away_group": away_record.get("group") if away_record else None,
            "home_group_rank": home_record.get("group_rank") if home_record else None,
            "away_group_rank": away_record.get("group_rank") if away_record else None,
            "home_currently_advancing": home_record.get("currently_advancing") if home_record else None,
            "away_currently_advancing": away_record.get("currently_advancing") if away_record else None,
        }
        return match

    @staticmethod
    def _source_match_winner(match: Optional[Dict[str, Any]]) -> Optional[str]:
        if not match:
            return None
        if match.get("wc_winner"):
            return match.get("wc_winner")
        if match.get("winner"):
            return match.get("winner")
        if match.get("game_status") == "final":
            home_so = match.get("home_shootout_score")
            away_so = match.get("visitor_shootout_score")
            if home_so is not None and away_so is not None and home_so != away_so:
                return match.get("home_team") if int(home_so or 0) > int(away_so or 0) else match.get("away_team") or match.get("visitor_team")
            return WorldCupTheSportsDBCollector._winner_from_game(match)
        return None

    @staticmethod
    def _coalesce_knockout_team(actual: Optional[Dict[str, Any]], key: str, fallback: Optional[str]) -> Optional[str]:
        if actual and actual.get(key):
            return actual.get(key)
        return fallback

    @staticmethod
    def _source_slot_label(source_match: Optional[Dict[str, Any]], prefix: str) -> str:
        if source_match and source_match.get("match_number"):
            return f"{prefix} Match {source_match['match_number']}"
        return "TBD"

    @staticmethod
    def _source_match_loser(match: Optional[Dict[str, Any]]) -> Optional[str]:
        if not match:
            return None
        home = match.get("home_team")
        away = match.get("away_team")
        if match.get("game_status") != "final":
            return None
        home_so = match.get("home_shootout_score")
        away_so = match.get("visitor_shootout_score")
        if home_so is not None and away_so is not None and home_so != away_so:
            return away if int(home_so or 0) > int(away_so or 0) else home
        if home and away:
            winner = WorldCupTheSportsDBCollector._winner_from_game(match)
            if winner == home:
                return away
            if winner == away:
                return home
        return None

    def _infer_groups(self, events: List[Dict[str, Any]]) -> Tuple[Dict[str, str], List[str]]:
        official_lookup, official_order = self._official_groups_for_current_season()
        if official_lookup:
            return official_lookup, official_order

        graph: Dict[str, set] = defaultdict(set)
        first_seen: Dict[str, Tuple[str, int]] = {}

        for index, raw in enumerate(events):
            try:
                r = int(raw.get("intRound") or 0)
            except Exception:
                r = 0
            if r not in (1, 2, 3):
                continue
            home = raw.get("strHomeTeam") or ""
            away = raw.get("strAwayTeam") or ""
            if not home or not away:
                continue
            graph[home].add(away)
            graph[away].add(home)
            sort_date = raw.get("dateEvent") or ""
            first_seen.setdefault(home, (sort_date, index))
            first_seen.setdefault(away, (sort_date, index))

        components = []
        visited = set()
        for team in graph:
            if team in visited:
                continue
            queue = deque([team])
            visited.add(team)
            component = []
            while queue:
                current = queue.popleft()
                component.append(current)
                for neighbor in graph[current]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append(neighbor)
            first = min((first_seen.get(t, ("9999-99-99", 9999)) for t in component), default=("9999-99-99", 9999))
            components.append((first, sorted(component)))

        components.sort(key=lambda item: item[0])
        lookup = {}
        order = []
        for index, (_, teams) in enumerate(components):
            label = self.GROUP_LABELS[index] if index < len(self.GROUP_LABELS) else f"Group {index + 1}"
            order.append(label)
            for team in teams:
                lookup[team] = label
        return lookup, order

    def _official_groups_for_current_season(self) -> Tuple[Dict[str, str], List[str]]:
        if self.current_season() != "2026":
            return {}, []

        lookup: Dict[str, str] = {}
        order: List[str] = []
        for group, teams in WC_2026_GROUPS:
            order.append(group)
            for team in teams:
                lookup[self._normalize_team_name(team)] = group
        for alias, canonical in WC_2026_GROUP_ALIASES.items():
            group = lookup.get(self._normalize_team_name(canonical))
            if group:
                lookup[self._normalize_team_name(alias)] = group
        return lookup, order

    def _group_for_team(self, team_name: str, group_lookup: Dict[str, str]) -> str:
        if not team_name:
            return ""
        exact = group_lookup.get(team_name)
        if exact:
            return exact
        normalized = self._normalize_team_name(team_name)
        if normalized in group_lookup:
            return group_lookup[normalized]
        alias = WC_2026_GROUP_ALIASES.get(normalized)
        if alias:
            return group_lookup.get(self._normalize_team_name(alias), "")
        return ""

    @staticmethod
    def _winner_from_game(game: Optional[Dict[str, Any]]) -> Optional[str]:
        if not game or not game.get("is_final"):
            return None
        home_score = game.get("home_score_total")
        away_score = game.get("visitor_score_total")
        if home_score is None or away_score is None or home_score == away_score:
            return None
        return game.get("home_team") if home_score > away_score else game.get("visitor_team")

    @staticmethod
    def _winner_from_scores(
        home_team: str,
        away_team: str,
        home_score: int,
        away_score: int,
        is_final: bool,
    ) -> Optional[str]:
        if not is_final:
            return None
        if home_score == away_score:
            return None
        return home_team if home_score > away_score else away_team

    def get_season_info(self, year: int = None) -> Optional[Dict[str, Any]]:
        """Build season_types from the bulk events (start = first match,
        end = latest knockout). Until knockouts are populated, this just
        spans the group stage; it'll auto-extend when TheSportsDB adds
        knockout fixtures."""
        season = year or self.current_season()
        if isinstance(season, int):
            season = str(season)
        try:
            events = self._season_events(season)
        except Exception:
            return None
        if not events:
            return None
        dates = sorted({(e.get("dateEvent") or "")[:10] for e in events if e.get("dateEvent")})
        if not dates:
            return None
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        current_phase = "Tournament" if dates[0] <= today <= dates[-1] else (
            "Upcoming" if today < dates[0] else "Off Season"
        )
        return {
            "year": int(season) if season.isdigit() else season,
            "current_phase": current_phase,
            "season_types": [
                {"name": "FIFA World Cup", "start_date": dates[0], "end_date": dates[-1]},
            ],
        }
