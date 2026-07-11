from __future__ import annotations

import json
from datetime import datetime, timezone

from giro_multi_stage_builder import parse_route_calendar
from refresh_if_due import _is_due


def test_parse_route_calendar_extracts_rows_and_links():
    html = """
    <html>
      <body>
        <table>
          <thead>
            <tr>
              <th>Stage</th>
              <th>Type</th>
              <th>Date</th>
              <th>Start and Finish</th>
              <th>Distance</th>
              <th>Details</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>1</td>
              <td>Individual Time-Trial</td>
              <td>Sat 08/22/2026</td>
              <td>Monaco &gt; Monaco</td>
              <td>9 KM</td>
              <td><a href="/en/stage-1">Stage 1</a></td>
            </tr>
            <tr>
              <td>2</td>
              <td>Hilly</td>
              <td>Sun 08/23/2026</td>
              <td>Monaco &gt; Manosque</td>
              <td>215.2 KM</td>
              <td><a href="/en/stage-2">Stage 2</a></td>
            </tr>
          </tbody>
        </table>
      </body>
    </html>
    """

    rows = parse_route_calendar(html, 2026).to_dict(orient="records")

    assert len(rows) == 2
    assert rows[0]["stage_number"] == 1
    assert rows[0]["date"] == "2026-08-22"
    assert rows[0]["race_type"] == "Individual Time-Trial"
    assert rows[0]["cycling_url"].endswith("/en/stage-1")
    assert rows[1]["stage_number"] == 2
    assert rows[1]["start_city"] == "Monaco"
    assert rows[1]["finish_city"] == "Manosque"
    assert rows[1]["distance_km"] == "215.2"


def test_refresh_if_due_uses_generated_at(tmp_path):
    bundle = tmp_path / "giro_app_bundle_2026.json"
    bundle.write_text(
        json.dumps({"generated_at": "2026-07-11T19:00:00Z", "year": 2026}),
        encoding="utf-8",
    )

    recent_now = datetime(2026, 7, 11, 19, 30, tzinfo=timezone.utc)

    assert not _is_due(bundle, recent_now)
