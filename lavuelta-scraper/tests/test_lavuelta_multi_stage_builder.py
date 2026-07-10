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
