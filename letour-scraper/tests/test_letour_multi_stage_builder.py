from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "letour_multi_stage_builder.py"
SPEC = spec_from_file_location("letour_multi_stage_builder", MODULE_PATH)
builder = module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(builder)


def test_extract_ranking_tab_urls_picks_expected_classifications():
    html = """
    <div>
      <span class="js-tabs-ranking" data-ajax-stack='{"itg":"/en/ajax/ranking/3/itg/hash/none","ipg":"/en/ajax/ranking/3/ipg/hash/none"}'></span>
      <span class="js-tabs-ranking-nested" data-type="ite" data-tabs-ajax="/en/ajax/ranking/3/ite/hash/subtab"></span>
      <span class="js-tabs-ranking-nested" data-type="foo" data-tabs-ajax="/ignore"></span>
    </div>
    """

    urls = builder.extract_ranking_tab_urls(html)

    assert urls["ite"] == "https://www.letour.fr/en/ajax/ranking/3/ite/hash/subtab"
    assert urls["itg"] == "https://www.letour.fr/en/ajax/ranking/3/itg/hash/none"
    assert urls["ipg"] == "https://www.letour.fr/en/ajax/ranking/3/ipg/hash/none"
    assert "foo" not in urls


def test_parse_classification_rows_parses_rider_table():
    html = """
    <table>
      <thead>
        <tr>
          <th>Rank</th><th>Rider</th><th>Rider No.</th><th>Team</th><th>Times</th><th>Gap</th><th>B</th><th>P</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>1</td>
          <td><a href="/en/rider/1/uae-team-emirates-xrg/tadej-pogacar">T. POGACAR</a></td>
          <td>1</td>
          <td><a href="/en/team/UEX/uae-team-emirates-xrg">UAE TEAM EMIRATES XRG</a></td>
          <td>08h 46' 55''</td>
          <td>-</td>
          <td>B : 16''</td>
          <td>-</td>
        </tr>
      </tbody>
    </table>
    """

    rows = builder.parse_classification_rows(html, 3, "https://www.letour.fr/example", "gc")

    assert rows[0]["classification_type"] == "gc"
    assert rows[0]["rank"] == "1"
    assert rows[0]["rider_name"] == "T. POGACAR"
    assert rows[0]["rider_slug"] == "tadej-pogacar"
    assert rows[0]["team_slug"] == "uae-team-emirates-xrg"
    assert rows[0]["time"] == "08h 46' 55''"
    assert rows[0]["bonus"] == "B : 16''"


def test_country_code_from_html_reads_rider_flag_markup():
    html = """
    <div class="riderInfos__country">
      <span class="flag js-display-lazy" data-class="flag--esp"></span>
      <span class="riderInfos__country__name">(esp)</span>
    </div>
    """

    assert builder._country_code_from_html(html) == "ESP"


def test_parse_classification_rows_parses_team_table():
    html = """
    <table>
      <thead>
        <tr>
          <th>Rank</th><th>Team</th><th>Times</th><th>Gap</th><th>P</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>1</td>
          <td><a href="/en/team/UEX/uae-team-emirates-xrg">UAE TEAM EMIRATES XRG</a></td>
          <td>14h 16' 27''</td>
          <td>-</td>
          <td>-</td>
        </tr>
      </tbody>
    </table>
    """

    rows = builder.parse_classification_rows(html, 3, "https://www.letour.fr/example", "teams")

    assert rows[0]["classification_type"] == "teams"
    assert rows[0]["rider_name"] is None
    assert rows[0]["team_name"] == "UAE TEAM EMIRATES XRG"
    assert rows[0]["team_slug"] == "uae-team-emirates-xrg"
    assert rows[0]["time"] == "14h 16' 27''"


def test_best_rider_dimension_rows_prefers_country_enriched_duplicate():
    riders = builder.pd.DataFrame(
        [
            {
                "rider_name": "T. POGACAR",
                "rider_slug": "tadej-pogacar",
                "rider_url": "https://www.letour.fr/en/rider/1/uae-team-emirates-xrg/tadej-pogacar",
                "rider_country_code": None,
                "rider_country_flag": None,
                "norm_name": "t. pogacar",
            },
            {
                "rider_name": "T. POGACAR",
                "rider_slug": "tadej-pogacar",
                "rider_url": "https://www.letour.fr/en/rider/1/uae-team-emirates-xrg/tadej-pogacar",
                "rider_country_code": "SLO",
                "rider_country_flag": "slo",
                "norm_name": "t. pogacar",
            },
        ]
    )

    selected = builder._best_rider_dimension_rows(riders)

    assert len(selected) == 1
    assert selected.iloc[0]["rider_country_code"] == "SLO"
    assert selected.iloc[0]["rider_country_flag"] == "slo"


def test_backfill_classification_rider_countries_uses_rider_url(monkeypatch):
    classifications = builder.pd.DataFrame(
        [
            {
                "rider_name": None,
                "rider_slug": "juan-ayuso-pesquera",
                "rider_url": "https://www.letour.fr/en/rider/31/lidl-trek/juan-ayuso-pesquera",
                "rider_country_code": None,
                "rider_country_flag": None,
            }
        ]
    )
    riders = builder.pd.DataFrame(
        [
            {
                "rider_name": None,
                "rider_slug": "juan-ayuso-pesquera",
                "rider_url": "https://www.letour.fr/en/rider/31/lidl-trek/juan-ayuso-pesquera",
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


def test_stage_date_and_state_use_stage_day_not_today():
    stage_row = {
        "date": builder.parse_stage_date("Stage 3 - 07/06 - Granollers > Les Angles", 2026),
        "stage_first_start_local": "12:10",
        "stage_last_arrival_local": "16:54",
    }

    state_before = builder.infer_stage_state(stage_row, now_local=builder.datetime(2026, 7, 6, 11, 45))
    state_during = builder.infer_stage_state(stage_row, now_local=builder.datetime(2026, 7, 6, 13, 0))
    state_after = builder.infer_stage_state(stage_row, now_local=builder.datetime(2026, 7, 7, 10, 0))

    assert stage_row["date"] == "2026-07-06"
    assert state_before == "active_window"
    assert state_during == "active_window"
    assert state_after == "post_stage"


def test_parse_stage_metrics_scopes_to_main_stage_header():
    html = """
    <a class="stageHeader__stage">
      <p class="stageHeader__length__text">
        <span class="stageHeader__length__label">Length</span><br>
        19.6 km
      </p>
      <p class="stageHeader__length__text">
        <span class="stageHeader__length__label">Type</span><br>
        Team Time-Trial
      </p>
    </a>
    <div class="stageHeader__stage stageHeader__stage--main">
      <p class="stageHeader__length__text">
        <span class="stageHeader__length__label">Length</span><br>
        168.5 km
      </p>
      <p class="stageHeader__length__text">
        <span class="stageHeader__length__label">Type</span><br>
        Hilly
      </p>
    </div>
    """

    metrics = builder.parse_stage_metrics(html)

    assert metrics["distance_km"] == "168.5"
    assert metrics["race_type"] == "Hilly"


def test_build_for_stage_prefers_stage_ranking_table_over_banner(monkeypatch):
    stage_html = """
    <html>
      <head><title>Stage 9 - Malemort > Ussel - Tour de France 2026</title></head>
      <body>
        <h3>Stage Winner Continental</h3>
        <div>
          <a href="/en/rider/121/uno-x-mobility/tobias-halland-johannessen">T. JOHANNESSEN</a>
          <a href="/en/team/UXM/uno-x-mobility">UNO-X MOBILITY</a>
        </div>
      </body>
    </html>
    """
    rankings_html = """
    <html>
      <head><title>Official classifications of Tour de France 2026 - Stage 9</title></head>
      <body>
        <div>Stage ranking</div>
        <table>
          <thead>
            <tr>
              <th>Rank</th><th>Rider</th><th>Rider No.</th><th>Team</th><th>Times</th><th>Gap</th><th>B</th><th>P</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>1</td>
              <td><a href="/en/rider/101/alpecin-premier-tech/mathieu-van-der-poel">M. VAN DER POEL</a></td>
              <td>101</td>
              <td><a href="/en/team/APT/alpecin-premier-tech">ALPECIN-PREMIER TECH</a></td>
              <td>03h 27' 51''</td>
              <td>-</td>
              <td>B : 10''</td>
              <td>-</td>
            </tr>
          </tbody>
        </table>
      </body>
    </html>
    """

    def fake_fetch_html(path):
        if path == "/en/stage-9":
            return f"https://www.letour.fr{path}", stage_html
        if path == "/en/rankings/stage-9":
            return f"https://www.letour.fr{path}", rankings_html
        raise AssertionError(path)

    monkeypatch.setattr(builder, "fetch_html", fake_fetch_html)
    monkeypatch.setattr(builder, "validate_stage_page", lambda html, stage_number, year: f"Stage {stage_number} - Malemort > Ussel - Tour de France {year}")
    monkeypatch.setattr(builder, "page_text", lambda html: "Stage 9 - 07/12 - Malemort > Ussel")
    monkeypatch.setattr(builder, "parse_stage_metrics", lambda html: {"distance_km": "154.6", "race_type": "Hilly"})
    monkeypatch.setattr(builder, "parse_stage_schedule", lambda text: {"stage_start_local": "13:45", "stage_finish_expected_local": "17:22", "stage_first_start_local": None, "stage_last_arrival_local": None})
    monkeypatch.setattr(builder, "extract_links", lambda html: (builder.pd.DataFrame(), builder.pd.DataFrame()))

    stage_df, classifications, _, _ = builder.build_for_stage(9, 2026)

    assert stage_df.iloc[0]["winner"] == "M. VAN DER POEL"
    assert stage_df.iloc[0]["winner_url"].endswith("/mathieu-van-der-poel")
    assert not classifications.empty
    assert classifications.iloc[0]["classification_type"] == "stage"
    assert classifications.iloc[0]["rider_name"] == "M. VAN DER POEL"
