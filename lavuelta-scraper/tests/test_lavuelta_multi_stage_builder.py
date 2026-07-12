from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "lavuelta_multi_stage_builder.py"
SPEC = spec_from_file_location("lavuelta_multi_stage_builder", MODULE_PATH)
builder = module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(builder)


def test_normalize_rider_table_repairs_shifted_rankings_layout():
    df = pd.DataFrame(
        [
            {
                "Rank": 1,
                "Rider": "J. PHILIPSEN",
                "Rider No.": 1,
                "Team": 71,
                "Times": "ALPECIN-DECEUNINCK",
                "Gap": "04h 09' 12''",
                "B": "-",
                "P": "B : 10''",
                "Unnamed: 8": "-",
            }
        ]
    )

    rows = builder.normalize_rider_table(df, 1, "https://www.lavuelta.es/en/rankings/stage-1", "stage")

    row = rows.iloc[0].to_dict()
    assert row["rank"] == 1
    assert row["rider_name"] == "J. PHILIPSEN"
    assert row["bib"] == 71
    assert row["team_name"] == "ALPECIN-DECEUNINCK"
    assert row["time"] == "04h 09' 12''"
    assert row["gap"] == "-"
    assert row["bonus"] == "B : 10''"
    assert row["points"] == "-"


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
