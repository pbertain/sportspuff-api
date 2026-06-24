from src.collectors.world_cup_thesportsdb import WorldCupTheSportsDBCollector


def test_world_cup_abbrevs_use_agreed_codes_for_aliases():
    assert WorldCupTheSportsDBCollector._team_abbrev("South Africa") == "RSA"
    assert WorldCupTheSportsDBCollector._team_abbrev("Korea Republic") == "KOR"
    assert WorldCupTheSportsDBCollector._team_abbrev("Cote d'Ivoire") == "CIV"
    assert WorldCupTheSportsDBCollector._team_abbrev("Curaçao") == "CUW"
    assert WorldCupTheSportsDBCollector._team_abbrev("Turkiye") == "TUR"


def test_world_cup_parse_event_prefers_agreed_codes_over_upstream_short_codes():
    collector = WorldCupTheSportsDBCollector()

    parsed = collector._parse_event({
        "idEvent": "123",
        "dateEvent": "2026-06-11",
        "strTime": "19:00:00",
        "intRound": "1",
        "strHomeTeam": "Mexico",
        "strHomeTeamShort": "MEX",
        "idHomeTeam": "1",
        "strAwayTeam": "South Africa",
        "strAwayTeamShort": "SOU",
        "idAwayTeam": "2",
    })

    assert parsed is not None
    assert parsed["home_team_abbrev"] == "MEX"
    assert parsed["visitor_team_abbrev"] == "RSA"


def test_world_cup_group_standings_use_official_2026_group_labels(monkeypatch):
    collector = WorldCupTheSportsDBCollector()
    monkeypatch.setattr(collector, "current_season", lambda: "2026")
    monkeypatch.setattr(
        collector,
        "_season_events",
        lambda season: [
            {"intRound": "1", "strHomeTeam": "United States", "strAwayTeam": "Australia", "intHomeScore": "1", "intAwayScore": "0", "strStatus": "FT"},
            {"intRound": "1", "strHomeTeam": "Paraguay", "strAwayTeam": "Turkey", "intHomeScore": "1", "intAwayScore": "1", "strStatus": "FT"},
            {"intRound": "1", "strHomeTeam": "Brazil", "strAwayTeam": "Morocco", "intHomeScore": "2", "intAwayScore": "1", "strStatus": "FT"},
            {"intRound": "1", "strHomeTeam": "Scotland", "strAwayTeam": "Haiti", "intHomeScore": "0", "intAwayScore": "0", "strStatus": "FT"},
            {"intRound": "1", "strHomeTeam": "England", "strAwayTeam": "Croatia", "intHomeScore": "1", "intAwayScore": "0", "strStatus": "FT"},
            {"intRound": "1", "strHomeTeam": "Ghana", "strAwayTeam": "Panama", "intHomeScore": "2", "intAwayScore": "2", "strStatus": "FT"},
            {"intRound": "1", "strHomeTeam": "Portugal", "strAwayTeam": "DR Congo", "intHomeScore": "3", "intAwayScore": "1", "strStatus": "FT"},
            {"intRound": "1", "strHomeTeam": "Uzbekistan", "strAwayTeam": "Colombia", "intHomeScore": "0", "intAwayScore": "1", "strStatus": "FT"},
        ],
    )

    standings = collector.get_standings()
    by_abbrev = {team["abbreviation"]: team["group"] for team in standings}

    assert by_abbrev["BRA"] == "C"
    assert by_abbrev["MAR"] == "C"
    assert by_abbrev["SCO"] == "C"
    assert by_abbrev["HAI"] == "C"
    assert by_abbrev["USA"] == "D"
    assert by_abbrev["PAR"] == "D"
    assert by_abbrev["AUS"] == "D"
    assert by_abbrev["TUR"] == "D"
    assert by_abbrev["POR"] == "K"
    assert by_abbrev["COD"] == "K"
    assert by_abbrev["UZB"] == "K"
    assert by_abbrev["COL"] == "K"
    assert by_abbrev["ENG"] == "L"
    assert by_abbrev["CRO"] == "L"
    assert by_abbrev["GHA"] == "L"
    assert by_abbrev["PAN"] == "L"

    grouped = {
        group["group"]: {team["abbreviation"] for team in group["teams"]}
        for group in collector.get_group_standings()
    }

    assert grouped["C"] == {"BRA", "MAR", "SCO", "HAI"}
    assert grouped["D"] == {"USA", "AUS", "PAR", "TUR"}
    assert grouped["K"] == {"POR", "COD", "UZB", "COL"}
    assert grouped["L"] == {"ENG", "CRO", "GHA", "PAN"}
