#!/usr/bin/env python3
import argparse
import json
import re
from functools import lru_cache
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://www.letour.fr"
HEADERS = {"User-Agent": "Mozilla/5.0"}
TEAM_RE = re.compile(r"/en/team/[^\"'#?]+")
RIDER_RE = re.compile(r"/en/rider/[^\"'#?]+")
STAGE_DATE_RE = re.compile(r"Stage\s+\d+\s*-\s*(\d{2})/(\d{2})\s*-", re.IGNORECASE)
COUNTRY_CODE_RE = re.compile(r"flag--([a-z]{2,3})", re.IGNORECASE)

CLASSIFICATION_TYPE_BY_TAB = {
    "ite": "stage",
    "itg": "gc",
    "ipg": "points",
    "img": "kom",
    "ijg": "youth",
    "etg": "teams",
    "icg": "combative",
}


def _now_tour_local_naive():
    return datetime.now(ZoneInfo("Europe/Paris")).replace(tzinfo=None)


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


def main_stage_header(html: str):
    soup = BeautifulSoup(html, "html.parser")
    return soup.select_one(".stageHeader__stage--main") or soup


def validate_stage_page(html: str, stage_number: int, year: int):
    title = page_title(html)
    if str(year) not in title:
        raise ValueError(f"Expected year {year} in title: {title}")
    if f"Stage {stage_number}" not in title:
        raise ValueError(f"Expected Stage {stage_number} in title: {title}")
    return title


def norm(value):
    return " ".join(str(value).split()).strip().lower()


def _clean(value):
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()


def _safe_int(value):
    try:
        if value in (None, ""):
            return None
        return int(str(value).replace("#", "").strip())
    except (TypeError, ValueError):
        return None


def extract_links(html: str):
    soup = BeautifulSoup(html, "html.parser")
    teams, riders = [], []
    seen_t, seen_r = set(), set()
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        label = " ".join(anchor.get_text(" ", strip=True).split())
        full = urljoin(BASE, href)
        if TEAM_RE.search(href) and full not in seen_t:
            seen_t.add(full)
            slug = href.rstrip("/").split("/")[-1]
            teams.append({"team_name": label or slug.replace("-", " ").title(), "team_slug": slug, "team_url": full})
        if RIDER_RE.search(href) and full not in seen_r:
            seen_r.add(full)
            slug = href.rstrip("/").split("/")[-1]
            rider_name = label or slug.replace("-", " ").title()
            rider_country = _rider_country_fields(full)
            riders.append({
                "rider_name": rider_name,
                "rider_slug": slug,
                "rider_url": full,
                **rider_country,
            })
    return pd.DataFrame(teams), pd.DataFrame(riders)


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
    selectors = [
        ".riderInfos__country",
        ".athleteInfos__country",
        ".athlete__country",
    ]
    for selector in selectors:
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


def parse_stage_schedule(text: str):
    schedule = {
        "stage_start_local": None,
        "stage_finish_expected_local": None,
        "stage_first_start_local": None,
        "stage_last_arrival_local": None,
    }
    match = re.search(r"Neutralised start\s*:\s*(\d{1,2}:\d{2}).*?Expected arrival\s*:\s*(\d{1,2}:\d{2})", text)
    if match:
        schedule["stage_start_local"] = match.group(1)
        schedule["stage_finish_expected_local"] = match.group(2)
    match = re.search(r"First start\s*:\s*(\d{1,2}:\d{2}).*?Last arrival\s*:\s*(\d{1,2}:\d{2})", text)
    if match:
        schedule["stage_first_start_local"] = match.group(1)
        schedule["stage_last_arrival_local"] = match.group(2)
    return schedule


def parse_stage_date(text: str, year: int):
    match = STAGE_DATE_RE.search(text)
    if not match:
        return None
    month = int(match.group(1))
    day = int(match.group(2))
    return date(year, month, day).isoformat()


def parse_stage_metrics(html: str):
    header = main_stage_header(html)
    metrics = {"distance_km": None, "race_type": None}
    for block in header.select(".stageHeader__length__text"):
        parts = [_clean(part) for part in block.stripped_strings if _clean(part)]
        if len(parts) < 2:
            continue
        label, value = parts[0], parts[-1]
        label_norm = norm(label)
        if label_norm == "length":
            match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*km", value)
            if match:
                metrics["distance_km"] = match.group(1)
        elif label_norm == "type":
            metrics["race_type"] = value
    return metrics


def infer_stage_state(stage_row: dict, now_local: datetime | None = None):
    now_local = now_local or _now_tour_local_naive()
    stage_date_raw = stage_row.get("date")
    if not stage_date_raw:
        return "unknown"
    try:
        stage_day = date.fromisoformat(stage_date_raw)
    except ValueError:
        return "unknown"

    start_s = stage_row.get("stage_first_start_local") or stage_row.get("stage_start_local")
    end_s = stage_row.get("stage_last_arrival_local") or stage_row.get("stage_finish_expected_local")
    if not start_s or not end_s:
        if stage_day > now_local.date():
            return "pre_stage"
        if stage_day < now_local.date():
            return "post_stage"
        return "unknown"

    start_dt = datetime.combine(stage_day, datetime.strptime(start_s, "%H:%M").time())
    end_dt = datetime.combine(stage_day, datetime.strptime(end_s, "%H:%M").time())
    if now_local < start_dt - timedelta(minutes=30):
        return "pre_stage"
    if start_dt - timedelta(minutes=30) <= now_local <= end_dt + timedelta(minutes=60):
        return "active_window"
    if now_local > end_dt + timedelta(minutes=60):
        return "post_stage"
    return "unknown"


def recommended_poll_minutes(stage_row: dict, now_local: datetime | None = None):
    state = infer_stage_state(stage_row, now_local=now_local)
    return 15 if state == "active_window" else 60


def stage_status(stage_row: dict, has_results: bool, now_local: datetime | None = None):
    now_local = now_local or _now_tour_local_naive()
    stage_date_raw = stage_row.get("date")
    if stage_date_raw:
        try:
            stage_day = date.fromisoformat(stage_date_raw)
        except ValueError:
            stage_day = None
    else:
        stage_day = None

    poll_state = infer_stage_state(stage_row, now_local=now_local)
    if poll_state == "active_window":
        return "in_progress"
    if stage_day and stage_day > now_local.date():
        return "scheduled"
    if has_results and (poll_state == "post_stage" or (stage_day and stage_day < now_local.date())):
        return "final"
    if stage_day == now_local.date() and poll_state == "pre_stage":
        return "scheduled"
    if has_results:
        return "final"
    return "scheduled"


def extract_stage_winner(stage_html: str):
    soup = BeautifulSoup(stage_html, "html.parser")
    for heading in soup.find_all(["h2", "h3"]):
        if "stage winner" not in norm(heading.get_text(" ", strip=True)):
            continue
        container = heading.parent
        rider_anchor = container.find("a", href=RIDER_RE) if container else None
        team_anchor = container.find("a", href=TEAM_RE) if container else None
        return {
            "winner": _clean(rider_anchor.get_text(" ", strip=True)) if rider_anchor else None,
            "winner_url": urljoin(BASE, rider_anchor["href"]) if rider_anchor and rider_anchor.has_attr("href") else None,
            "team": _clean(team_anchor.get_text(" ", strip=True)) if team_anchor else None,
            "team_url": urljoin(BASE, team_anchor["href"]) if team_anchor and team_anchor.has_attr("href") else None,
        }
    return {"winner": None, "winner_url": None, "team": None, "team_url": None}


def extract_ranking_tab_urls(rankings_html: str):
    soup = BeautifulSoup(rankings_html, "html.parser")
    urls = {}
    for span in soup.select(".js-tabs-ranking[data-ajax-stack]"):
        raw = span.get("data-ajax-stack")
        if not raw:
            continue
        try:
            stack = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for tab_code, path in stack.items():
            tab_code = _clean(tab_code).lower()
            if tab_code in CLASSIFICATION_TYPE_BY_TAB and path:
                urls[tab_code] = urljoin(BASE, path)
    for span in soup.select(".js-tabs-ranking-nested[data-tabs-ajax][data-type]"):
        tab_code = _clean(span.get("data-type")).lower()
        if tab_code not in CLASSIFICATION_TYPE_BY_TAB:
            continue
        urls[tab_code] = urljoin(BASE, span.get("data-tabs-ajax"))
    return urls


def _header_key(header: str):
    text = norm(header)
    if text == "rank":
        return "rank"
    if text in {"rider", "team"}:
        return text
    if "rider no" in text:
        return "bib"
    if text.startswith("time"):
        return "time"
    if text == "gap":
        return "gap"
    if text == "points":
        return "points"
    if text == "b":
        return "bonus"
    if text == "p":
        return "points_secondary"
    return text.replace(" ", "_")


def _table_headers(table):
    headers = []
    for th in table.select("thead th"):
        header = _clean(th.get_text(" ", strip=True))
        if not header:
            header = f"column_{len(headers) + 1}"
        headers.append(_header_key(header))
    return headers


def parse_classification_rows(html: str, stage_number: int, source_url: str, classification_type: str):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table")
    if table is None:
        return []

    headers = _table_headers(table)
    rows = []
    for tr in table.select("tbody tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        values = {}
        for idx, cell in enumerate(cells):
            key = headers[idx] if idx < len(headers) else f"column_{idx + 1}"
            values[key] = _clean(cell.get_text(" ", strip=True))

        rider_anchor = tr.find("a", href=RIDER_RE)
        team_anchor = tr.find("a", href=TEAM_RE)
        row = {
            "race": "Tour de France",
            "stage_number": stage_number,
            "classification_type": classification_type,
            "rank": values.get("rank"),
            "rider_name": None,
            "rider_slug": None,
            "rider_url": None,
            "bib": values.get("bib"),
            "team_name": None,
            "team_slug": None,
            "team_url": None,
            "time": values.get("time"),
            "gap": values.get("gap"),
            "points": values.get("points") or values.get("points_secondary"),
            "bonus": values.get("bonus"),
            "source_url": source_url,
        }

        if rider_anchor is not None:
            rider_href = rider_anchor.get("href", "")
            row["rider_name"] = _clean(rider_anchor.get_text(" ", strip=True))
            row["rider_slug"] = rider_href.rstrip("/").split("/")[-1]
            row["rider_url"] = urljoin(BASE, rider_href)

        if team_anchor is not None:
            team_href = team_anchor.get("href", "")
            row["team_name"] = _clean(team_anchor.get_text(" ", strip=True))
            row["team_slug"] = team_href.rstrip("/").split("/")[-1]
            row["team_url"] = urljoin(BASE, team_href)

        if classification_type == "teams" and row["team_name"] is None:
            row["team_name"] = values.get("team")

        if row["rider_name"] is None and values.get("rider"):
            row["rider_name"] = values.get("rider")
        if row["team_name"] is None and values.get("team"):
            row["team_name"] = values.get("team")

        if row["classification_type"] == "combative" and row["points"] is None:
            row["points"] = values.get("column_5")

        rows.append(row)
    return rows


def build_stage_classifications(stage_number: int, rankings_html: str):
    ranking_tabs = extract_ranking_tab_urls(rankings_html)
    rows = []
    visible_stage_rows = parse_classification_rows(
        rankings_html,
        stage_number,
        f"{BASE}/en/rankings/stage-{stage_number}",
        "stage",
    )
    if visible_stage_rows:
        rows.extend(visible_stage_rows)
    elif ranking_tabs.get("ite"):
        source_url, html = fetch_html(ranking_tabs["ite"])
        rows.extend(parse_classification_rows(html, stage_number, source_url, "stage"))
    for tab_code, classification_type in CLASSIFICATION_TYPE_BY_TAB.items():
        if classification_type == "stage":
            continue
        ajax_url = ranking_tabs.get(tab_code)
        if not ajax_url:
            continue
        source_url, html = fetch_html(ajax_url)
        rows.extend(parse_classification_rows(html, stage_number, source_url, classification_type))
    return pd.DataFrame(rows)


def build_for_stage(stage_number: int, year: int):
    stage_path = f"/en/stage-{stage_number}"
    rankings_path = f"/en/rankings/stage-{stage_number}"

    stage_url, stage_html = fetch_html(stage_path)
    rankings_url, rankings_html = fetch_html(rankings_path)

    stage_title = validate_stage_page(stage_html, stage_number, year)
    rankings_title = validate_stage_page(rankings_html, stage_number, year)
    stage_text = page_text(stage_html)
    rankings_text = page_text(rankings_html)
    stage_metrics = parse_stage_metrics(stage_html)

    teams_stage, riders_stage = extract_links(stage_html)
    teams_rank, riders_rank = extract_links(rankings_html)

    classifications = build_stage_classifications(stage_number, rankings_html)

    teams = pd.concat([teams_stage, teams_rank], ignore_index=True).drop_duplicates(subset=["team_url"])
    riders = pd.concat([riders_stage, riders_rank], ignore_index=True).drop_duplicates(subset=["rider_url"])
    if "rider_country_code" not in riders.columns:
        riders["rider_country_code"] = None
    if "rider_country_flag" not in riders.columns:
        riders["rider_country_flag"] = None
    if not classifications.empty:
        rider_rows = classifications[["rider_name", "rider_slug", "rider_url"]].dropna(subset=["rider_url"])
        team_rows = classifications[["team_name", "team_slug", "team_url"]].dropna(subset=["team_url"])
        riders = pd.concat([riders, rider_rows], ignore_index=True).drop_duplicates(subset=["rider_url"])
        teams = pd.concat([teams, team_rows], ignore_index=True).drop_duplicates(subset=["team_url"])
        if "rider_country_code" not in riders.columns:
            riders["rider_country_code"] = None
        if "rider_country_flag" not in riders.columns:
            riders["rider_country_flag"] = None
        riders["norm_name"] = riders["rider_name"].map(norm)
        teams["norm_team"] = teams["team_name"].map(norm)
        classifications["norm_name"] = classifications["rider_name"].map(norm)
        classifications["norm_team"] = classifications["team_name"].map(norm)
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

    stage_name = stage_title.split(" - ")[1] if " - " in stage_title else ""
    if " - Tour de France" in stage_name:
        stage_name = stage_name.split(" - Tour de France")[0].strip()

    winner = {"winner": None, "winner_url": None, "team": None, "team_url": None}
    stage_class = classifications[classifications["classification_type"] == "stage"] if not classifications.empty else pd.DataFrame()
    if not stage_class.empty:
        top = stage_class[stage_class["rider_name"].fillna("").str.strip() != ""].head(1)
        if not top.empty:
            rider_country_code = top.iloc[0].get("rider_country_code")
            winner = {
                "winner": top.iloc[0].get("rider_name"),
                "winner_url": top.iloc[0].get("rider_url"),
                "team": top.iloc[0].get("team_name"),
                "team_url": top.iloc[0].get("team_url"),
                "winner_country_code": rider_country_code,
                "winner_country_flag": str(rider_country_code).lower() if rider_country_code else None,
            }
    stage_date = parse_stage_date(rankings_text or stage_text, year)

    stage_row = {
        "race": "Tour de France",
        "stage_number": stage_number,
        "stage_name": stage_name,
        "date": stage_date,
        "distance_km": stage_metrics.get("distance_km"),
        "race_type": stage_metrics.get("race_type"),
        "start_city": stage_name.split(">")[0].strip() if ">" in stage_name else None,
        "finish_city": stage_name.split(">")[-1].strip() if ">" in stage_name else None,
        "cycling_event_label": f"Tour de France {year} - Stage {stage_number}",
        "cycling_country": "France",
        "cycling_url": stage_url,
        "rankings_url": rankings_url,
        "stage_page_title": stage_title,
        "rankings_page_title": rankings_title,
        **parse_stage_schedule(stage_text),
        **winner,
    }
    stage_row["poll_state"] = infer_stage_state(stage_row)
    stage_row["recommended_poll_minutes"] = recommended_poll_minutes(stage_row)
    stage_row["status"] = stage_status(stage_row, has_results=not classifications.empty)
    if not stage_class.empty and stage_row["status"] == "final":
        stage_row.update(winner)
    elif not winner["winner"] and stage_row["status"] == "final":
        winner = extract_stage_winner(stage_html)
        stage_row.update(winner)

    rider_dim = (
        riders[["rider_name", "rider_slug", "rider_url", "rider_country_code", "rider_country_flag"]]
        .dropna(subset=["rider_url"])
        .drop_duplicates()
        .sort_values(["rider_name", "rider_url"])
        if not riders.empty
        else pd.DataFrame(columns=["rider_name", "rider_slug", "rider_url", "rider_country_code", "rider_country_flag"])
    )
    team_dim = (
        teams[["team_name", "team_slug", "team_url"]]
        .dropna(subset=["team_url"])
        .drop_duplicates()
        .sort_values(["team_name", "team_url"])
        if not teams.empty
        else pd.DataFrame(columns=["team_name", "team_slug", "team_url"])
    )

    if not classifications.empty:
        classifications["rank"] = classifications["rank"].map(_safe_int)
        classifications["bib"] = classifications["bib"].map(_safe_int)

    return pd.DataFrame([stage_row]), classifications, team_dim, rider_dim


def write_schedule_artifacts(outdir: Path, stages: pd.DataFrame):
    cron_lines = [
        "# Hourly catch-all sync",
        "7 * * * * python letour_multi_stage_builder.py --year 2026 --start-stage 1 --end-stage 21 --outdir output/letour-prod",
        "",
        "# Add a separate app-level guard: during today's active stage window, poll every 15 minutes",
        "# Suggested cron ticks: */15 * * * *",
        "# Your app can inspect stages.csv -> poll_state and recommended_poll_minutes to decide whether to fan out a full stage refresh.",
    ]
    (outdir / "suggested_cron.txt").write_text("\n".join(cron_lines), encoding="utf-8")

    stage_sched = stages[
        [
            c
            for c in stages.columns
            if c
            in [
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
        ]
    ].copy()
    stage_sched.to_csv(outdir / "stage_schedule.csv", index=False)

    payload = {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "notes": [
            "Use hourly polling outside active race windows.",
            "Use 15-minute polling from 30 minutes before start until 60 minutes after expected finish or last arrival.",
            "Treat a stage as effectively finished when the stage window has passed and two consecutive polls return unchanged results.",
        ],
    }
    (outdir / "polling_plan.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_app_bundle(outdir: Path, year: int, stages: pd.DataFrame, classifications: pd.DataFrame, teams: pd.DataFrame, riders: pd.DataFrame):
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
        "race": "Tour de France",
        "year": year,
        "source": "letour-scraper",
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
    (outdir / "letour_app_bundle.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    (outdir / f"letour_app_bundle_{year}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--start-stage", type=int, default=1)
    parser.add_argument("--end-stage", type=int, default=21)
    parser.add_argument("--outdir", default="output/letour-multi-stage")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    stages_all, class_all, teams_all, riders_all = [], [], [], []
    for stage_number in range(args.start_stage, args.end_stage + 1):
        stage_df, class_df, team_df, rider_df = build_for_stage(stage_number, args.year)
        stages_all.append(stage_df)
        class_all.append(class_df)
        teams_all.append(team_df)
        riders_all.append(rider_df)

    stages = pd.concat(stages_all, ignore_index=True) if stages_all else pd.DataFrame()
    classifications = pd.concat(class_all, ignore_index=True) if class_all else pd.DataFrame()
    teams = pd.concat(teams_all, ignore_index=True).drop_duplicates(subset=["team_url"]) if teams_all else pd.DataFrame()
    riders = pd.concat(riders_all, ignore_index=True).drop_duplicates(subset=["rider_url"]) if riders_all else pd.DataFrame()

    stages.to_csv(outdir / "stages.csv", index=False)
    classifications.to_csv(outdir / "classifications.csv", index=False)
    teams.to_csv(outdir / "teams.csv", index=False)
    riders.to_csv(outdir / "riders.csv", index=False)
    pd.DataFrame(
        [
            ("stages.csv", "One row per stage with schedule windows, poll hints, and source URLs"),
            ("classifications.csv", "Ranking rows per stage with classification types and rider/team links"),
            ("teams.csv", "Unique teams with letour.fr links"),
            ("riders.csv", "Unique riders with letour.fr links for page rendering"),
            ("stage_schedule.csv", "Scheduling helper for your app"),
            ("letour_app_bundle.json", "App-friendly JSON bundle for stage, classification, rider, and team rendering"),
            ("suggested_cron.txt", "Suggested cron entries"),
            ("polling_plan.json", "Machine-readable polling guidance"),
        ],
        columns=["file", "description"],
    ).to_csv(outdir / "manifest.csv", index=False)
    write_schedule_artifacts(outdir, stages)
    write_app_bundle(outdir, args.year, stages, classifications, teams, riders)

    print(f"Wrote normalized outputs for stages {args.start_stage}..{args.end_stage} to {outdir}")


if __name__ == "__main__":
    main()
