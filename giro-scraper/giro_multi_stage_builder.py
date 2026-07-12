#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from functools import lru_cache
from datetime import UTC, date, datetime
from io import StringIO
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://www.giroditalia.it"
HEADERS = {"User-Agent": "Mozilla/5.0"}
TEAM_RE = re.compile(r"/en/team/[^\"'#?]+")
RIDER_RE = re.compile(r"/en/rider/[^\"'#?]+")
RANKINGS_CLASSIFICATION_MAP = {
    "ORARR": "stage",
    "CLGEN": "gc",
}
COUNTRY_CODE_RE = re.compile(r"flag--([a-z]{2,3})", re.IGNORECASE)


def fetch_html(path: str):
    url = path if path.startswith("http") else urljoin(BASE, path)
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return url, response.text


def page_title(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    if soup.title and soup.title.string:
        return " ".join(soup.title.string.split())
    return ""


def page_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return " ".join(soup.get_text(" ", strip=True).split())


def _clean(value):
    return "" if value is None else " ".join(str(value).split()).strip()


def _safe_int(value):
    try:
        if value in (None, ""):
            return None
        return int(str(value).replace("#", "").strip())
    except (TypeError, ValueError):
        return None


def _first_anchor_href(node) -> str | None:
    if node is None:
        return None
    anchor = node.find("a", href=True)
    if not anchor:
        return None
    href = anchor.get("href") or ""
    return urljoin(BASE, href) if href else None


def parse_route_date(date_text: str | None, year: int):
    if not date_text:
        return None
    cleaned = _clean(date_text)
    for fmt in ("%a %m/%d/%Y", "%a %m/%d/%y", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            continue
    match = re.search(r"(\d{2})/(\d{2})/(\d{4})", cleaned)
    if match:
        month, day, parsed_year = map(int, match.groups())
        return date(parsed_year, month, day).isoformat()
    return None


def _split_start_finish(value: str | None):
    text = _clean(value)
    if not text:
        return None, None
    if ">" in text:
        left, right = text.split(">", 1)
        return left.strip() or None, right.strip() or None
    if " - " in text:
        left, right = text.split(" - ", 1)
        return left.strip() or None, right.strip() or None
    return text, text


def _normalise_stage_type(value: str | None):
    text = _clean(value)
    return text or None


def _country_code_from_value(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        value = " ".join(str(item) for item in value)
    text = _clean(value)
    if not text:
        return None
    match = re.search(r"(?:nationality|country)\s*[:\-]?\s*([a-z]{2,3})", text, re.IGNORECASE)
    if match:
        code = match.group(1).strip().lower()
        if code and code != "en":
            return code.upper()
    match = COUNTRY_CODE_RE.search(text)
    if match:
        code = match.group(1).strip().lower()
        if code and code != "en":
            return code.upper()
    match = re.search(r"\(([a-z]{2,3})\)", text, re.IGNORECASE)
    if match:
        code = match.group(1).strip().lower()
        if code and code != "en":
            return code.upper()
    if re.fullmatch(r"[a-z]{2,3}", text, re.IGNORECASE) and text.lower() != "en":
        return text.upper()
    return None


def _country_code_from_html(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for selector in (".riderInfos__country", ".athleteInfos__country", ".athlete__country"):
        for node in soup.select(selector):
            for candidate in [node, *node.find_all(True)]:
                for attr in ("data-class", "data-country", "data-country-code"):
                    code = _country_code_from_value(candidate.get(attr))
                    if code:
                        return code
                code = _country_code_from_value(candidate.get("class"))
                if code:
                    return code
                code = _country_code_from_value(candidate.get_text(" ", strip=True))
                if code:
                    return code
    for node in soup.find_all(True):
        code = _country_code_from_value(node.get("data-class"))
        if code:
            return code
    code = _country_code_from_value(soup.get_text(" ", strip=True))
    if code:
        return code
    return None


@lru_cache(maxsize=2048)
def _rider_country_fields(rider_url: str) -> dict[str, str | None]:
    try:
        _, html = fetch_html(rider_url)
    except Exception:
        return {"rider_country_code": None, "rider_country_flag": None}
    code = _country_code_from_html(html)
    if not code:
        return {"rider_country_code": None, "rider_country_flag": None}
    return {"rider_country_code": code, "rider_country_flag": code.lower()}


def _first_anchor_href(cell) -> str | None:
    if cell is None:
        return None
    anchor = cell.find("a", href=True)
    if not anchor:
        return None
    href = anchor.get("href") or ""
    if not href:
        return None
    return urljoin(BASE, href)


def _table_headers(table) -> list[str]:
    headers = []
    head = table.find("thead")
    if head:
        for cell in head.find_all(["th", "td"]):
            headers.append(_clean(cell.get_text(" ", strip=True)).lower())
    if not headers:
        first_row = table.find("tr")
        if first_row:
            headers = [_clean(cell.get_text(" ", strip=True)).lower() for cell in first_row.find_all(["th", "td"])]
    return headers


def _parse_archive_stage_items(html: str, year: int) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for item in soup.select(".stage-item[data-stage]"):
        stage_number = _safe_int(item.get("data-stage"))
        if stage_number is None:
            continue
        anchor = item.find_parent("a", href=True)
        href = anchor["href"] if anchor and anchor.has_attr("href") else None
        stage_name = _clean(item.get("data-nometappa"))
        if not stage_name:
            stage_name = _clean(item.get_text(" ", strip=True))
        stage_name = re.sub(rf"^Stage\s*{stage_number}\s*", "", stage_name, flags=re.I).strip()
        date_text = _clean(item.select_one(".stage-data .label-4").get_text(" ", strip=True) if item.select_one(".stage-data .label-4") else "")
        if not date_text:
            date_text = _clean(item.get_text(" ", strip=True))
        rows.append(
            {
                "race": "Giro d'Italia",
                "stage_number": stage_number,
                "stage_name": stage_name or None,
                "date": parse_route_date(date_text, year),
                "status": "scheduled",
                "winner": None,
                "winner_url": None,
                "team": None,
                "team_url": None,
                "distance_km": None,
                "race_type": None,
                "start_city": _split_start_finish(stage_name)[0],
                "finish_city": _split_start_finish(stage_name)[1],
                "cycling_event_label": f"Giro d'Italia {year} - Stage {stage_number}",
                "cycling_country": None,
                "cycling_url": href or f"{BASE}/en/classifiche/di-tappa/{stage_number}",
                "rankings_url": f"{BASE}/en/classifiche/di-tappa/{stage_number}",
                "stage_page_title": f"Stage {stage_number} - {stage_name} - Giro d'Italia {year}" if stage_name else None,
                "rankings_page_title": f"Rankings of the Giro d'Italia {year}",
            }
        )
    if rows:
        return pd.DataFrame(rows)

    return pd.DataFrame()


def _parse_route_table_html(html: str, year: int) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    tables = []
    try:
        tables = soup.find_all("table")
    except Exception:
        tables = []
    for table in tables:
        headers = [_clean(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]
        if not headers or "stage" not in headers[0]:
            continue
        rows = []
        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if not cells or any(cell.name == "th" for cell in cells):
                continue
            texts = [_clean(cell.get_text(" ", strip=True)) for cell in cells]
            if not texts or not texts[0].isdigit():
                continue
            stage_number = _safe_int(texts[0])
            if stage_number is None:
                continue
            stage_type = texts[1] if len(texts) > 1 else None
            date_text = texts[2] if len(texts) > 2 else None
            start_finish = texts[3] if len(texts) > 3 else None
            distance_text = texts[4] if len(texts) > 4 else None
            details_url = _first_anchor_href(cells[-1]) or f"{BASE}/en/archivio-{year}/"
            start_city, finish_city = _split_start_finish(start_finish)
            rows.append(
                {
                    "race": "Giro d'Italia",
                    "stage_number": stage_number,
                    "stage_name": _clean(start_finish) or None,
                    "date": parse_route_date(date_text, year),
                    "status": "scheduled",
                    "winner": None,
                    "winner_url": None,
                    "team": None,
                    "team_url": None,
                    "distance_km": _clean(distance_text).removesuffix("KM").removesuffix("km").strip() or None,
                    "race_type": _normalise_stage_type(stage_type),
                    "start_city": start_city,
                    "finish_city": finish_city,
                    "cycling_event_label": f"Giro d'Italia {year} - Stage {stage_number}",
                    "cycling_country": None,
                    "cycling_url": details_url,
                    "rankings_url": details_url,
                    "stage_page_title": f"Stage {stage_number} - {start_finish} - Giro d'Italia {year}" if start_finish else None,
                    "rankings_page_title": f"Official classifications of Giro d'Italia {year} - Stage {stage_number}",
                }
            )
        if rows:
            return pd.DataFrame(rows)

    return pd.DataFrame()


def parse_route_calendar(html: str, year: int):
    rows = _parse_archive_stage_items(html, year)
    if not rows.empty:
        return rows
    return _parse_route_table_html(html, year)


def _parse_ranking_rows(section, *, classification_type: str, stage_number: int | None, source_url: str) -> pd.DataFrame:
    if section is None:
        return pd.DataFrame()
    rows = []
    for row in section.select(".line-table"):
        cells = row.find_all(recursive=False)
        if not cells:
            continue
        rider_cell = next((cell for cell in cells if "corridore" in (cell.get("class") or [])), None)
        team_cell = next((cell for cell in cells if "team" in (cell.get("class") or [])), None)
        time_cell = next((cell for cell in cells if "tempo" in (cell.get("class") or [])), None)
        bonus_cell = next((cell for cell in cells if "abbuono" in (cell.get("class") or [])), None)
        gap_cell = next((cell for cell in cells if "distacco" in (cell.get("class") or [])), None)
        rank_text = _clean(rider_cell.get_text(" ", strip=True)) if rider_cell else ""
        m = re.match(r"(\d+)\s+(.*)", rank_text)
        rank = _safe_int(m.group(1)) if m else _safe_int(rank_text.split()[0] if rank_text.split() else None)
        rider_name = m.group(2).strip() if m else rank_text
        rider_url = _first_anchor_href(rider_cell)
        team_name = _clean(team_cell.get_text(" ", strip=True)) if team_cell else None
        team_url = _first_anchor_href(team_cell)
        rows.append(
            {
                "race": "Giro d'Italia",
                "stage_number": stage_number,
                "classification_type": classification_type,
                "rank": rank,
                "rider_name": rider_name or None,
                "rider_slug": None,
                "rider_url": rider_url,
                "bib": None,
                "team_name": team_name or None,
                "team_slug": None,
                "team_url": team_url,
                "time": _clean(time_cell.get_text(" ", strip=True)) if time_cell else None,
                "gap": _clean(gap_cell.get_text(" ", strip=True)) if gap_cell else None,
                "points": None,
                "bonus": _clean(bonus_cell.get_text(" ", strip=True)) if bonus_cell else None,
                "source_url": source_url,
            }
        )
    return pd.DataFrame(rows)


def extract_links(html: str):
    soup = BeautifulSoup(html, "html.parser")
    teams, riders = [], []
    seen_t, seen_r = set(), set()
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        label = _clean(anchor.get_text(" ", strip=True))
        full = urljoin(BASE, href)
        if TEAM_RE.search(href) and full not in seen_t:
            seen_t.add(full)
            slug = href.rstrip("/").split("/")[-1]
            teams.append({"team_name": label or slug.replace("-", " ").title(), "team_slug": slug, "team_url": full})
        if RIDER_RE.search(href) and full not in seen_r:
            seen_r.add(full)
            slug = href.rstrip("/").split("/")[-1]
            riders.append({
                "rider_name": label or slug.replace("-", " ").title(),
                "rider_slug": slug,
                "rider_url": full,
                **_rider_country_fields(full),
            })
    return pd.DataFrame(teams), pd.DataFrame(riders)


def infer_stage_status(stage_row: dict, today: date | None = None):
    today = today or datetime.now(UTC).date()
    stage_date_raw = stage_row.get("date")
    if not stage_date_raw:
        return "scheduled"
    try:
        stage_day = date.fromisoformat(stage_date_raw)
    except ValueError:
        return "scheduled"
    if stage_day < today:
        return "final"
    if stage_day == today:
        return "in_progress"
    return "scheduled"


def recommended_poll_minutes(stage_row: dict):
    return 15 if infer_stage_status(stage_row) == "in_progress" else 60


def write_versioned_csv(df: pd.DataFrame, outdir: Path, stem: str, year: int):
    df.to_csv(outdir / f"{stem}.csv", index=False)
    df.to_csv(outdir / f"{stem}_{year}.csv", index=False)


def write_versioned_text(outdir: Path, stem: str, year: int, text: str):
    (outdir / f"{stem}.txt").write_text(text, encoding="utf-8")
    (outdir / f"{stem}_{year}.txt").write_text(text, encoding="utf-8")


def write_schedule_artifacts(outdir: Path, year: int, stages: pd.DataFrame):
    cron_lines = [
        "# Hourly catch-all sync for Giro d'Italia",
        f"17 * * * * python giro_multi_stage_builder.py --year {year} --start-stage 1 --end-stage 21 --outdir output/giro-prod",
        "",
        "# During today's active stage window, poll every 15 minutes",
        "# Suggested cron ticks: */15 * * * *",
        "# Your app can inspect stages.csv -> poll_state and recommended_poll_minutes to decide whether to fan out a full stage refresh.",
    ]
    write_versioned_text(outdir, "suggested_cron", year, "\n".join(cron_lines))

    keep = [
        "stage_number",
        "stage_name",
        "stage_start_local",
        "stage_finish_expected_local",
        "stage_first_start_local",
        "stage_last_arrival_local",
        "poll_state",
        "recommended_poll_minutes",
        "cycling_url",
        "rankings_url",
    ]
    schedule = stages[[c for c in keep if c in stages.columns]]
    write_versioned_csv(schedule, outdir, "stage_schedule", year)

    payload = {
        "race": "Giro d'Italia",
        "year": year,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "notes": [
            "Use hourly polling outside active race windows.",
            "Use 15-minute polling from 30 minutes before start until 60 minutes after expected finish or last arrival.",
            "Treat a stage as effectively finished when the stage window has passed and two consecutive polls return unchanged results.",
        ],
    }
    text = json.dumps(payload, indent=2)
    (outdir / "polling_plan.json").write_text(text, encoding="utf-8")
    (outdir / f"polling_plan_{year}.json").write_text(text, encoding="utf-8")


def write_app_bundle(outdir: Path, year: int, stages: pd.DataFrame, classifications: pd.DataFrame, teams: pd.DataFrame, riders: pd.DataFrame):
    if "stage_number" not in classifications.columns:
        classifications = pd.DataFrame(columns=["stage_number"])
    schedule_columns = [
        "stage_number",
        "stage_name",
        "cycling_url",
        "rankings_url",
        "stage_start_local",
        "stage_finish_expected_local",
        "stage_first_start_local",
        "stage_last_arrival_local",
        "poll_state",
        "recommended_poll_minutes",
    ]
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    stage_payloads = []
    for stage_row in stages.to_dict(orient="records"):
        stage_number = stage_row["stage_number"]
        stage_payloads.append(
            {
                "stage": stage_row,
                "schedule": [{key: stage_row.get(key) for key in schedule_columns}],
                "classifications": classifications[classifications["stage_number"] == stage_number].to_dict(orient="records"),
            }
        )

    payload = {
        "race": "Giro d'Italia",
        "year": year,
        "source": "giroditalia.it",
        "generated_at": generated_at,
        "source_updated_at": generated_at,
        "teams": teams.to_dict(orient="records"),
        "riders": riders.to_dict(orient="records"),
        "stages": stage_payloads,
        "generated_files": [
            "stages.csv",
            "classifications.csv",
            "teams.csv",
            "riders.csv",
            "stage_schedule.csv",
            "suggested_cron.txt",
            "polling_plan.json",
        ],
    }
    (outdir / "giro_app_bundle.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    (outdir / f"giro_app_bundle_{year}.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--start-stage", type=int, default=1)
    parser.add_argument("--end-stage", type=int, default=21)
    parser.add_argument("--outdir", default="output/giro-multi-stage")
    parser.add_argument("--route-url", default=None, help="Optional override for the Giro route calendar page.")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    route_url = args.route_url or f"/en/archivio-{args.year}/"
    _route_url, route_html = fetch_html(route_url)
    route_table = parse_route_calendar(route_html, args.year)
    route_lookup = {int(row["stage_number"]): row for row in route_table.to_dict(orient="records") if row.get("stage_number") is not None}

    overall_url = f"{BASE}/en/classifiche/?classifica=CLGEN"
    overall_html = fetch_html(overall_url)[1]
    overall_soup = BeautifulSoup(overall_html, "html.parser")
    overall_section = overall_soup.select_one(".js-tab-classifica-CLGEN") or overall_soup

    stages_all, class_all, teams_all, riders_all = [], [], [], []
    stage_records = []
    for stage_number in range(args.start_stage, args.end_stage + 1):
        route_row = dict(route_lookup.get(stage_number, {}))
        stage_row = dict(route_row)
        stage_row["stage_number"] = stage_number
        stage_row["status"] = infer_stage_status(stage_row)
        stage_row["poll_state"] = "in_progress" if stage_row["status"] == "in_progress" else ("post_stage" if stage_row["status"] == "final" else "pre_stage")
        stage_row["recommended_poll_minutes"] = recommended_poll_minutes(stage_row)
        stage_row["cycling_event_label"] = stage_row.get("cycling_event_label") or f"Giro d'Italia {args.year} - Stage {stage_number}"
        stage_row["rankings_page_title"] = stage_row.get("rankings_page_title") or f"Rankings of the Giro d'Italia {args.year}"
        stage_row["rankings_url"] = stage_row.get("rankings_url") or f"{BASE}/en/classifiche/di-tappa/{stage_number}"
        stage_row["cycling_url"] = stage_row.get("cycling_url") or stage_row["rankings_url"]
        stage_row["stage_page_title"] = stage_row.get("stage_page_title") or f"Stage {stage_number} - Giro d'Italia {args.year}"
        stage_records.append(stage_row)

    latest_completed_stage = max(
        (row["stage_number"] for row in stage_records if row.get("status") in ("final", "in_progress")),
        default=None,
    )

    overall_gc_rows = _parse_ranking_rows(
        overall_section,
        classification_type="gc",
        stage_number=latest_completed_stage,
        source_url=overall_url,
    )
    overall_teams, overall_riders = extract_links(overall_html)

    for stage_row in stage_records:
        stage_number = stage_row["stage_number"]
        stage_df = pd.DataFrame([stage_row])
        stage_rows = []
        if stage_row.get("status") == "final":
            rankings_url = stage_row.get("rankings_url") or f"{BASE}/en/classifiche/di-tappa/{stage_number}"
            rankings_html = fetch_html(rankings_url)[1]
            rankings_soup = BeautifulSoup(rankings_html, "html.parser")
            stage_section = rankings_soup.select_one(".js-tab-classifica-ORARR") or rankings_soup
            stage_results = _parse_ranking_rows(
                stage_section,
                classification_type="stage",
                stage_number=stage_number,
                source_url=rankings_url,
            )
            stage_teams, stage_riders = extract_links(rankings_html)
            teams_all.append(stage_teams)
            riders_all.append(stage_riders)
            if not stage_results.empty:
                stage_rows.append(stage_results)
        if latest_completed_stage is not None and stage_number == latest_completed_stage and not overall_gc_rows.empty:
            stage_rows.append(overall_gc_rows)
        class_df = pd.concat(stage_rows, ignore_index=True) if stage_rows else pd.DataFrame(columns=["race", "stage_number", "classification_type", "rank", "rider_name", "rider_slug", "rider_url", "bib", "team_name", "team_slug", "team_url", "time", "gap", "points", "bonus", "source_url"])
        stages_all.append(stage_df)
        class_all.append(class_df)

    stages = pd.concat(stages_all, ignore_index=True) if stages_all else pd.DataFrame()
    classifications = pd.concat(class_all, ignore_index=True) if class_all else pd.DataFrame()
    teams_all.append(overall_teams)
    riders_all.append(overall_riders)
    teams = pd.concat(teams_all, ignore_index=True).drop_duplicates(subset=["team_url"]) if teams_all else pd.DataFrame()
    riders = pd.concat(riders_all, ignore_index=True).drop_duplicates(subset=["rider_url"]) if riders_all else pd.DataFrame()

    if not riders.empty:
        riders["norm_name"] = riders["rider_name"].map(lambda value: _clean(value).lower() if pd.notna(value) else "")
    if not teams.empty:
        teams["norm_team"] = teams["team_name"].map(lambda value: _clean(value).lower() if pd.notna(value) else "")
    if not classifications.empty:
        classifications["norm_name"] = classifications["rider_name"].map(lambda value: _clean(value).lower() if pd.notna(value) else "")
        classifications["norm_team"] = classifications["team_name"].map(lambda value: _clean(value).lower() if pd.notna(value) else "")
        if not riders.empty:
            classifications = classifications.merge(
                riders[[
                    "rider_name",
                    "rider_slug",
                    "rider_url",
                    "rider_country_code",
                    "rider_country_flag",
                    "norm_name",
                ]].drop_duplicates("norm_name"),
                on="norm_name",
                how="left",
                suffixes=("", "_lk"),
            )
        if not teams.empty:
            classifications = classifications.merge(
                teams[["team_name", "team_slug", "team_url", "norm_team"]].drop_duplicates("norm_team"),
                on="norm_team",
                how="left",
                suffixes=("", "_lk"),
            )
        classifications = classifications.rename(columns={"rider_name": "rider_name_scraped", "team_name": "team_name_scraped"})
        classifications["rider_name"] = classifications.get("rider_name_lk", classifications["rider_name_scraped"])
        classifications["team_name"] = classifications.get("team_name_lk", classifications["team_name_scraped"])
        for col in ["rider_slug", "rider_url", "rider_country_code", "rider_country_flag", "team_slug", "team_url"]:
            if col not in classifications.columns:
                classifications[col] = None
            lk_col = f"{col}_lk"
            if lk_col in classifications.columns:
                classifications[col] = classifications[lk_col].where(
                    classifications[lk_col].notna() & (classifications[lk_col].astype(str).str.strip() != ""),
                    classifications[col],
                )
        keep = [
            "race",
            "stage_number",
            "classification_type",
            "rank",
            "rider_name",
            "rider_slug",
            "rider_url",
            "rider_country_code",
            "rider_country_flag",
            "bib",
            "team_name",
            "team_slug",
            "team_url",
            "time",
            "gap",
            "points",
            "bonus",
            "source_url",
        ]
        classifications = classifications[[col for col in keep if col in classifications.columns]]

    if not stages.empty and not classifications.empty:
        stage_class = classifications[classifications["classification_type"] == "stage"]
        if not stage_class.empty:
            top = stage_class.head(1).iloc[0]
            mask = stages["stage_number"] == top.get("stage_number")
            if mask.any():
                stages.loc[mask, "winner"] = top.get("rider_name")
                stages.loc[mask, "winner_url"] = top.get("rider_url")
                stages.loc[mask, "team"] = top.get("team_name")
                stages.loc[mask, "team_url"] = top.get("team_url")
                stages.loc[mask, "winner_country_code"] = top.get("rider_country_code")
                stages.loc[mask, "winner_country_flag"] = str(top.get("rider_country_code")).lower() if top.get("rider_country_code") else None

    write_versioned_csv(stages, outdir, "stages", args.year)
    write_versioned_csv(classifications, outdir, "classifications", args.year)
    write_versioned_csv(teams, outdir, "teams", args.year)
    write_versioned_csv(riders, outdir, "riders", args.year)
    write_schedule_artifacts(outdir, args.year, stages)
    write_app_bundle(outdir, args.year, stages, classifications, teams, riders)

    manifest = pd.DataFrame(
        [
            ("stages.csv", "One row per stage with schedule windows, poll hints, and source URLs"),
            ("classifications.csv", "Ranking rows per stage with classification types and rider/team links"),
            ("teams.csv", "Unique teams with giroditalia.it links"),
            ("riders.csv", "Unique riders with giroditalia.it links for page rendering"),
            ("stage_schedule.csv", "Scheduling helper for your app"),
            ("polling_plan.json", "Machine-readable polling guidance"),
            ("suggested_cron.txt", "Suggested cron entries"),
            ("giro_app_bundle.json", "App-friendly JSON export"),
            ("stages_YYYY.csv", "Year-tagged stage calendar archive"),
            ("classifications_YYYY.csv", "Year-tagged classification archive"),
            ("teams_YYYY.csv", "Year-tagged team archive"),
            ("riders_YYYY.csv", "Year-tagged rider archive"),
            ("stage_schedule_YYYY.csv", "Year-tagged scheduling helper archive"),
            ("polling_plan_YYYY.json", "Year-tagged polling guidance archive"),
            ("suggested_cron_YYYY.txt", "Year-tagged cron archive"),
            ("giro_app_bundle_YYYY.json", "Year-tagged bundle archive"),
        ],
        columns=["file", "description"],
    )
    write_versioned_csv(manifest, outdir, "manifest", args.year)

    print(f"Wrote Giro outputs for stages {args.start_stage}..{args.end_stage} to {outdir}")


if __name__ == "__main__":
    main()
