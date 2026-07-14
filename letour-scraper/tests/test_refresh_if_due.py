from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "refresh_if_due.py"
SPEC = spec_from_file_location("letour_refresh_if_due", MODULE_PATH)
refresh = module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(refresh)


def test_bundle_generated_at_controls_due_logic(tmp_path):
    bundle = tmp_path / "letour_app_bundle_2026.json"
    bundle.write_text(
        """
        {"generated_at":"2026-07-09T17:45:51Z","stages":[{"stage":{"poll_state":"pre_stage","recommended_poll_minutes":60}}]}
        """.strip(),
        encoding="utf-8",
    )

    assert refresh._is_due(bundle, 60, now=refresh.datetime(2026, 7, 11, 12, 30, tzinfo=refresh.TOUR_TIMEZONE))


def test_bundle_generated_at_skips_recent_content_even_if_mtime_is_old(tmp_path):
    bundle = tmp_path / "letour_app_bundle_2026.json"
    bundle.write_text(
        """
        {"generated_at":"2026-07-11T19:10:00Z","stages":[{"stage":{"poll_state":"pre_stage","recommended_poll_minutes":60}}]}
        """.strip(),
        encoding="utf-8",
    )

    assert not refresh._is_due(bundle, 60, now=refresh.datetime(2026, 7, 11, 12, 30, tzinfo=refresh.TOUR_TIMEZONE))


def test_due_when_past_stage_is_not_final_even_if_bundle_is_recent(tmp_path):
    bundle = tmp_path / "letour_app_bundle_2026.json"
    bundle.write_text(
        """
        {
          "generated_at":"2026-07-12T18:00:00Z",
          "stages":[
            {"stage":{"date":"2026-07-11","status":"scheduled","race_type":"Flat","poll_state":"post_stage","recommended_poll_minutes":60}}
          ]
        }
        """.strip(),
        encoding="utf-8",
    )

    assert refresh._is_due(bundle, 60, now=refresh.datetime(2026, 7, 12, 18, 15, tzinfo=refresh.TOUR_TIMEZONE))


def test_due_when_finished_individual_stage_has_no_winner_even_if_bundle_is_recent(tmp_path):
    bundle = tmp_path / "letour_app_bundle_2026.json"
    bundle.write_text(
        """
        {
          "generated_at":"2026-07-14T16:30:00Z",
          "stages":[
            {
              "stage":{
                "date":"2026-07-14",
                "status":"final",
                "race_type":"Mountain",
                "stage_finish_expected_local":"15:00",
                "winner":null,
                "poll_state":"post_stage",
                "recommended_poll_minutes":60
              }
            }
          ]
        }
        """.strip(),
        encoding="utf-8",
    )

    assert refresh._is_due(bundle, 60, now=refresh.datetime(2026, 7, 14, 16, 35, tzinfo=refresh.TOUR_TIMEZONE))
