from __future__ import annotations

import json
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from giro_multi_stage_builder import _parse_ranking_rows, parse_route_calendar
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


def test_parse_route_calendar_extracts_stage_items_from_archive_dom():
    html = """
    <html>
      <body>
        <div class="stage-item" data-stage="1" data-nometappa="Nessebar / Несебър - Burgas / Бургас">
          <div class="stage-data">
            <span class="is-pink h5">Stage1</span>
            <span class="label-4">08/05/2026</span>
            <p class="p-3">Nessebar / Несебър - Burgas / Бургас</p>
          </div>
        </div>
        <div class="stage-item" data-stage="2" data-nometappa="Burgas / Бургас - Veliko Tarnovo / Велико Търново">
          <div class="stage-data">
            <span class="is-pink h5">Stage2</span>
            <span class="label-4">09/05/2026</span>
            <p class="p-3">Burgas / Бургас - Veliko Tarnovo / Велико Търново</p>
          </div>
        </div>
      </body>
    </html>
    """

    rows = parse_route_calendar(html, 2026).to_dict(orient="records")

    assert len(rows) == 2
    assert rows[0]["stage_number"] == 1
    assert rows[0]["date"] == "2026-05-08"
    assert rows[0]["stage_name"] == "Nessebar / Несебър - Burgas / Бургас"
    assert rows[1]["stage_number"] == 2
    assert rows[1]["stage_name"] == "Burgas / Бургас - Veliko Tarnovo / Велико Търново"
    assert rows[1]["cycling_url"].endswith("/en/classifiche/di-tappa/2")


def test_parse_ranking_rows_extracts_stage_and_gc_tables():
    html = """
    <html>
      <body>
        <div class="single-tab js-tab-classifica-ORARR is-active">
          <div class="table type-4">
            <div class="line-table">
              <div class="corridore p-3"><a href="https://www.giroditalia.it/en/atleti/magnier-paul/">1 Paul MAGNIER</a></div>
              <div class="team p-3"><a href="https://www.giroditalia.it/en/squadre/soudal-quick-step/">SOUDAL QUICK-STEP</a></div>
              <div class="tempo p-3 is-text-right">3:21:08</div>
              <div class="abbuono p-3 is-text-right">0:10</div>
              <div class="distacco p-3 is-text-right">0:00</div>
            </div>
          </div>
        </div>
        <div class="single-tab js-tab-classifica-CLGEN is-active">
          <div class="table type-4">
            <div class="line-table">
              <div class="corridore p-3"><a href="https://www.giroditalia.it/en/atleti/vingegaard-jonas/">1 Jonas VINGEGAARD</a></div>
              <div class="team p-3"><a href="https://www.giroditalia.it/en/squadre/team-visma-lease-a-bike/">TEAM VISMA - LEASE A BIKE</a></div>
              <div class="tempo p-3 is-text-right">83:22:51</div>
              <div class="distacco p-3 is-text-right">0:00</div>
            </div>
          </div>
        </div>
      </body>
    </html>
    """

    soup = BeautifulSoup(html, "html.parser")
    stage_rows = _parse_ranking_rows(
        soup.select_one(".js-tab-classifica-ORARR"),
        classification_type="stage",
        stage_number=1,
        source_url="https://www.giroditalia.it/en/classifiche/di-tappa/1",
    ).to_dict(orient="records")
    gc_rows = _parse_ranking_rows(
        soup.select_one(".js-tab-classifica-CLGEN"),
        classification_type="gc",
        stage_number=8,
        source_url="https://www.giroditalia.it/en/classifiche/?classifica=CLGEN",
    ).to_dict(orient="records")

    assert stage_rows[0]["classification_type"] == "stage"
    assert stage_rows[0]["rank"] == 1
    assert stage_rows[0]["rider_name"] == "Paul MAGNIER"
    assert stage_rows[0]["team_name"] == "SOUDAL QUICK-STEP"
    assert stage_rows[0]["bonus"] == "0:10"
    assert gc_rows[0]["classification_type"] == "gc"
    assert gc_rows[0]["rank"] == 1
    assert gc_rows[0]["rider_name"] == "Jonas VINGEGAARD"
    assert gc_rows[0]["gap"] == "0:00"


def test_refresh_if_due_uses_generated_at(tmp_path):
    bundle = tmp_path / "giro_app_bundle_2026.json"
    bundle.write_text(
        json.dumps({"generated_at": "2026-07-11T19:00:00Z", "year": 2026}),
        encoding="utf-8",
    )

    recent_now = datetime(2026, 7, 11, 19, 30, tzinfo=timezone.utc)

    assert not _is_due(bundle, recent_now)
