from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "lavuelta_multi_stage_builder.py"
SPEC = spec_from_file_location("lavuelta_multi_stage_builder", MODULE_PATH)
builder = module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(builder)


def test_parse_classification_rows_repairs_shifted_rankings_layout():
    html = """
    <table>
      <thead>
        <tr><th>Rank</th><th>Rider</th><th>Rider No.</th><th>Team</th><th>Times</th><th>Gap</th><th>B</th><th>P</th></tr>
      </thead>
      <tbody>
        <tr>
          <td>1</td>
          <td><a href="/en/rider/71/alpecin-deceuninck/jasper-philipsen">J. PHILIPSEN</a></td>
          <td>1</td>
          <td>71</td>
          <td><a href="/en/team/APD/alpecin-deceuninck">ALPECIN-DECEUNINCK</a></td>
          <td>04h 09' 12''</td>
          <td>-</td>
          <td>B : 10''</td>
          <td>-</td>
        </tr>
      </tbody>
    </table>
    """

    rows = builder.parse_classification_rows(html, 1, "https://www.lavuelta.es/en/rankings/stage-1", "points")

    assert len(rows) == 1
    row = rows[0]
    assert row["rank"] == "1"
    assert row["rider_name"] == "J. PHILIPSEN"
    assert row["rider_slug"] == "jasper-philipsen"
    assert row["bib"] == "71"
    assert row["team_name"] == "ALPECIN-DECEUNINCK"
    assert row["time"] == "04h 09' 12''"
    assert row["gap"] == "-"
    assert row["bonus"] == "B : 10''"
    assert row["points"] == "-"


def test_parse_classification_rows_handles_team_table_without_shift():
    html = """
    <table>
      <thead>
        <tr><th>Rank</th><th>Team</th><th>Times</th><th>Gap</th><th>P</th></tr>
      </thead>
      <tbody>
        <tr>
          <td>1</td>
          <td><a href="/en/team/TVL/team-visma-lease-a-bike">TEAM VISMA | LEASE A BIKE</a></td>
          <td>32h 47' 34''</td>
          <td>-</td>
          <td>-</td>
        </tr>
      </tbody>
    </table>
    """

    rows = builder.parse_classification_rows(html, 3, "https://www.lavuelta.es/en/rankings/stage-3", "teams")

    assert len(rows) == 1
    row = rows[0]
    assert row["team_name"] == "TEAM VISMA | LEASE A BIKE"
    assert row["team_slug"] == "team-visma-lease-a-bike"
    assert row["time"] == "32h 47' 34''"


def test_parse_route_calendar_reads_full_stage_list():
    html = """
    <table>
      <thead>
        <tr>
          <th>Stage</th><th>Type</th><th>Date</th><th>Start and Finish</th><th>Distance</th><th>Details</th>
        </tr>
      </thead>
      <tbody>
        <tr><td>1</td><td>Individual time-trial</td><td>Sat 08/22/2026</td><td>Monaco > Monaco</td><td>9 km</td><td>Stage 1</td></tr>
        <tr><td>2</td><td>Hilly</td><td>Sun 08/23/2026</td><td>Monaco > Manosque</td><td>215.2 km</td><td>Stage 2</td></tr>
        <tr><td>-</td><td>Rest Day</td><td>Mon 08/31/2026</td><td>Descanso</td><td></td><td>Rest 1</td></tr>
      </tbody>
    </table>
    """

    route = builder.parse_route_calendar(html, 2026)

    assert len(route) == 2
    assert route.iloc[0]["stage_number"] == 1
    assert route.iloc[0]["date"] == "2026-08-22"
    assert route.iloc[0]["race_type"] == "Individual time-trial"
    assert route.iloc[0]["start_city"] == "Monaco"
    assert route.iloc[0]["finish_city"] == "Monaco"
    assert route.iloc[1]["stage_number"] == 2
    assert route.iloc[1]["distance_km"] == "215.2"


def test_infer_stage_status_distinguishes_past_today_and_future():
    reference_now = builder.datetime(2026, 8, 23, 12, 0)

    assert builder.infer_stage_status("2026-08-22", today=reference_now) == "completed"
    assert builder.infer_stage_status("2026-08-23", today=reference_now) == "in_progress"
    assert builder.infer_stage_status("2026-08-24", today=reference_now) == "scheduled"


def test_write_app_bundle_handles_empty_classifications_without_stage_number(tmp_path):
    stages = pd.DataFrame(
        [
            {
                "stage_number": 1,
                "stage_name": "Monaco > Monaco",
                "date": "2026-08-22",
                "status": "scheduled",
            }
        ]
    )
    empty_classifications = pd.DataFrame()
    empty_teams = pd.DataFrame()
    empty_riders = pd.DataFrame()

    builder.write_app_bundle(tmp_path, 2026, stages, empty_classifications, empty_teams, empty_riders)

    payload = (tmp_path / "lavuelta_app_bundle_2026.json").read_text(encoding="utf-8")
    assert '"classifications": []' in payload


def test_country_code_from_html_reads_rider_flag_markup():
    html = """
    <div class="riderInfos__country">
      <span class="flag js-display-lazy" data-class="flag--ita"></span>
      <span class="riderInfos__country__name">(ita)</span>
    </div>
    """

    assert builder._country_code_from_html(html) == "ITA"


def test_backfill_classification_rider_countries_uses_rider_url(monkeypatch):
    classifications = pd.DataFrame(
        [
            {
                "race": "La Vuelta",
                "stage_number": 1,
                "classification_type": "gc",
                "rank": 1,
                "rider_name": None,
                "rider_slug": "juan-ayuso-pesquera",
                "rider_url": "https://www.lavuelta.es/en/rider/juan-ayuso-pesquera/",
                "rider_country_code": None,
                "rider_country_flag": None,
            }
        ]
    )
    riders = pd.DataFrame(
        [
            {
                "rider_name": "Juan AYUSO",
                "rider_slug": "juan-ayuso-pesquera",
                "rider_url": "https://www.lavuelta.es/en/rider/juan-ayuso-pesquera/",
                "rider_country_code": None,
                "rider_country_flag": None,
            }
        ]
    )

    monkeypatch.setattr(
        builder,
        "_rider_country_fields",
        lambda rider_url: {"rider_country_code": "ESP", "rider_country_flag": "esp"},
    )

    enriched = builder._backfill_classification_rider_countries(classifications, riders)

    assert enriched.iloc[0]["rider_country_code"] == "ESP"
    assert enriched.iloc[0]["rider_country_flag"] == "esp"
