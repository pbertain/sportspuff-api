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

