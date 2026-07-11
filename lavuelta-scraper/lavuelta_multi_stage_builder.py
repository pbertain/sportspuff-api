#!/usr/bin/env python3
import argparse
import json
import re
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = 'https://www.lavuelta.es'
HEADERS = {'User-Agent': 'Mozilla/5.0'}
TEAM_RE = re.compile(r"/en/team/[^\"'#?]+")
RIDER_RE = re.compile(r"/en/rider/[^\"'#?]+")


def fetch_html(path: str):
    url = path if path.startswith('http') else urljoin(BASE, path)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return url, r.text


def page_title(html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')
    if soup.title and soup.title.string:
        return ' '.join(soup.title.string.split())
    return ''


def page_text(html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')
    return ' '.join(soup.get_text(' ', strip=True).split())


def validate_stage_page(html: str, stage_number: int, year: int):
    title = page_title(html)
    body = page_text(html)
    combined = f'{title} {body}'
    if f'Stage {stage_number}' not in title and f'Stage {stage_number}' not in body:
        raise ValueError(f'Expected Stage {stage_number} in page title/text: {title}')
    return title


def extract_tables(html: str):
    tables = []
    try:
        for df in pd.read_html(StringIO(html)):
            if not df.empty:
                tables.append(df)
    except ValueError:
        pass
    soup = BeautifulSoup(html, 'html.parser')
    for table in soup.select('table'):
        try:
            df = pd.read_html(StringIO(str(table)))[0]
            if not df.empty:
                tables.append(df)
        except Exception:
            pass
    return tables


def parse_route_calendar(html: str, year: int):
    tables = extract_tables(html)
    if not tables:
        return pd.DataFrame()
    df = tables[0].copy()
    df.columns = [str(c).strip().replace('\n', ' ') for c in df.columns]
    rows = []
    for _, row in df.iterrows():
        stage_raw = str(row.get('Stage', '')).strip()
        if not stage_raw.isdigit():
            continue
        stage_number = int(stage_raw)
        stage_type = ' '.join(str(row.get('Type', '')).split()).strip() or None
        date_text = ' '.join(str(row.get('Date', '')).split()).strip() or None
        start_finish = ' '.join(str(row.get('Start and Finish', '')).split()).strip() or None
        distance_text = ' '.join(str(row.get('Distance', '')).split()).strip() or None
        rows.append({
            'stage_number': stage_number,
            'stage_name': start_finish,
            'date': parse_route_date(date_text),
            'status': 'scheduled',
            'race_type': stage_type,
            'start_city': start_finish.split('>')[0].strip() if start_finish and '>' in start_finish else (start_finish or None),
            'finish_city': start_finish.split('>')[-1].strip() if start_finish and '>' in start_finish else (start_finish or None),
            'distance_km': distance_text.removesuffix(' km') if distance_text else None,
            'cycling_country': 'Spain',
            'cycling_url': f'{BASE}/en/stage-{stage_number}',
            'rankings_url': f'{BASE}/en/rankings/stage-{stage_number}',
            'stage_page_title': f'Stage {stage_number} - {start_finish} - La Vuelta {year}' if start_finish else None,
            'rankings_page_title': f'Official classifications of La Vuelta - Stage {stage_number}',
            'cycling_event_label': f'La Vuelta {year} - Stage {stage_number}',
        })
    return pd.DataFrame(rows)


def parse_route_date(date_text: str | None):
    if not date_text:
        return None
    for fmt in ('%a %m/%d/%Y', '%a %m/%d/%y'):
        try:
            return datetime.strptime(date_text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def infer_stage_status(stage_date: str | None, today: datetime | None = None):
    if not stage_date:
        return 'scheduled'
    today = (today or datetime.now()).date()
    try:
        parsed_date = datetime.strptime(str(stage_date), '%Y-%m-%d').date()
    except ValueError:
        return 'scheduled'
    if parsed_date < today:
        return 'completed'
    if parsed_date == today:
        return 'in_progress'
    return 'scheduled'


def extract_links(html: str):
    soup = BeautifulSoup(html, 'html.parser')
    teams, riders = [], []
    seen_t, seen_r = set(), set()
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        label = ' '.join(a.get_text(' ', strip=True).split())
        full = urljoin(BASE, href)
        if TEAM_RE.search(href) and full not in seen_t:
            seen_t.add(full)
            slug = href.rstrip('/').split('/')[-1]
            teams.append({'team_name': label or slug.replace('-', ' ').title(), 'team_slug': slug, 'team_url': full})
        if RIDER_RE.search(href) and full not in seen_r:
            seen_r.add(full)
            slug = href.rstrip('/').split('/')[-1]
            riders.append({'rider_name': label or slug.replace('-', ' ').title(), 'rider_slug': slug, 'rider_url': full})
    return pd.DataFrame(teams), pd.DataFrame(riders)


def norm(s):
    return ' '.join(str(s).split()).strip().lower()


def looks_time_value(value) -> bool:
    text = ' '.join(str(value or '').split()).strip()
    return bool(re.search(r"\d{1,2}h\s+\d{2}'\s+\d{2}''", text))


def parse_stage_schedule(text: str):
    schedule = {
        'stage_start_local': None,
        'stage_finish_expected_local': None,
        'stage_first_start_local': None,
        'stage_last_arrival_local': None,
    }
    m = re.search(r'Neutralised start\s*:\s*(\d{1,2}:\d{2}).*?Expected arrival\s*:\s*(\d{1,2}:\d{2})', text)
    if m:
        schedule['stage_start_local'] = m.group(1)
        schedule['stage_finish_expected_local'] = m.group(2)
    m = re.search(r'First start\s*:\s*(\d{1,2}:\d{2}).*?Last arrival\s*:\s*(\d{1,2}:\d{2})', text)
    if m:
        schedule['stage_first_start_local'] = m.group(1)
        schedule['stage_last_arrival_local'] = m.group(2)
    return schedule


def detect_classification_type(df: pd.DataFrame, idx: int) -> str:
    cols = [str(c).strip().lower() for c in df.columns]
    joined = ' | '.join(cols)
    if 'teams' in joined or joined.strip() == 'team':
        return 'teams'
    if 'young' in joined or 'best young' in joined or 'white' in joined:
        return 'youth'
    if 'mountain' in joined or 'kom' in joined or 'climber' in joined or 'polka' in joined:
        return 'kom'
    if 'points' in joined or 'green' in joined:
        return 'points'
    if 'general' in joined or 'overall' in joined or 'gc' in joined or 'red' in joined:
        return 'gc'
    return 'stage' if idx == 0 else f'classification_{idx+1}'


def normalize_rider_table(df: pd.DataFrame, stage_number: int, source_url: str, classification_type: str):
    df = df.copy()
    df.columns = [str(c).strip().replace('\n', ' ') for c in df.columns]
    cols = list(df.columns)
    rows = []
    for _, row in df.iterrows():
        rank = row[cols[0]] if len(cols) > 0 else None
        rider_name = row[cols[1]] if len(cols) > 1 else None
        bib = row[cols[2]] if len(cols) > 2 else None
        team_name = row[cols[3]] if len(cols) > 3 else None
        time_value = row[cols[4]] if len(cols) > 4 else None
        gap = row[cols[5]] if len(cols) > 5 else None
        points = row[cols[6]] if len(cols) > 6 else None
        bonus = row[cols[7]] if len(cols) > 7 else None

        # The rankings table currently includes an extra parsed column after the
        # rider cell, which shifts the remaining values one position right.
        if (
            pd.notna(team_name)
            and str(team_name).strip().isdigit()
            and pd.notna(time_value)
            and not looks_time_value(time_value)
            and looks_time_value(gap)
        ):
            bib = team_name
            team_name = time_value
            time_value = gap
            gap = points
            bonus = row[cols[7]] if len(cols) > 7 else None
            points = row[cols[8]] if len(cols) > 8 else None

        rows.append({
            'race': 'La Vuelta',
            'stage_number': stage_number,
            'classification_type': classification_type,
            'rank': rank,
            'rider_name': rider_name,
            'bib': bib,
            'team_name': team_name,
            'time': time_value,
            'gap': gap,
            'points': points,
            'bonus': bonus,
            'source_url': source_url,
        })
    return pd.DataFrame(rows)


def infer_stage_state(stage_row: dict, now_local: datetime | None = None):
    now_local = now_local or datetime.now()
    start_s = stage_row.get('stage_first_start_local') or stage_row.get('stage_start_local')
    end_s = stage_row.get('stage_last_arrival_local') or stage_row.get('stage_finish_expected_local')
    if not start_s or not end_s:
        return 'unknown'
    start_dt = datetime.combine(now_local.date(), datetime.strptime(start_s, '%H:%M').time())
    end_dt = datetime.combine(now_local.date(), datetime.strptime(end_s, '%H:%M').time())
    if now_local < start_dt - timedelta(minutes=30):
        return 'pre_stage'
    if start_dt - timedelta(minutes=30) <= now_local <= end_dt + timedelta(minutes=60):
        return 'active_window'
    if now_local > end_dt + timedelta(minutes=60):
        return 'post_stage'
    return 'unknown'


def recommended_poll_minutes(stage_row: dict):
    return 15 if infer_stage_state(stage_row) == 'active_window' else 60


def write_versioned_csv(df: pd.DataFrame, outdir: Path, stem: str, year: int):
    df.to_csv(outdir / f'{stem}.csv', index=False)
    df.to_csv(outdir / f'{stem}_{year}.csv', index=False)


def write_versioned_text(outdir: Path, stem: str, year: int, content: str, suffix: str = '.txt'):
    (outdir / f'{stem}{suffix}').write_text(content, encoding='utf-8')
    (outdir / f'{stem}_{year}{suffix}').write_text(content, encoding='utf-8')


def build_for_stage(stage_number: int, year: int, route_row: dict | None = None):
    stage_path = f'/en/stage-{stage_number}'
    rankings_path = f'/en/rankings/stage-{stage_number}'

    stage_url, stage_html = fetch_html(stage_path)
    stage_title = validate_stage_page(stage_html, stage_number, year)
    stage_text = page_text(stage_html)
    teams_stage, riders_stage = extract_links(stage_html)

    route_row = route_row or {}
    stage_name = route_row.get('stage_name') or (stage_title.split(' - ')[1] if ' - ' in stage_title else '')
    if isinstance(stage_name, str) and ' - La Vuelta' in stage_name:
        stage_name = stage_name.split(' - La Vuelta')[0].strip()
    stage_status = infer_stage_status(route_row.get('date'))

    rankings_url = route_row.get('rankings_url') or f'{BASE}{rankings_path}'
    rankings_title = route_row.get('rankings_page_title') or f'Official classifications of La Vuelta - Stage {stage_number}'
    ranking_tables = []
    teams = teams_stage
    riders = riders_stage

    stage_row = {
        'race': 'La Vuelta',
        'stage_number': stage_number,
        'stage_name': stage_name,
        'date': route_row.get('date'),
        'status': stage_status,
        'winner': None,
        'winner_url': None,
        'team': None,
        'team_url': None,
        'distance_km': route_row.get('distance_km'),
        'race_type': route_row.get('race_type'),
        'start_city': route_row.get('start_city') or (stage_name.split('>')[0].strip() if '>' in stage_name else None),
        'finish_city': route_row.get('finish_city') or (stage_name.split('>')[-1].strip() if '>' in stage_name else None),
        'cycling_event_label': route_row.get('cycling_event_label') or f'La Vuelta {year} - Stage {stage_number}',
        'cycling_country': route_row.get('cycling_country') or 'Spain',
        'cycling_url': route_row.get('cycling_url') or stage_url,
        'rankings_url': rankings_url,
        'stage_page_title': route_row.get('stage_page_title') or stage_title,
        'rankings_page_title': rankings_title,
        **parse_stage_schedule(stage_text),
    }

    if stage_status != 'scheduled':
        rankings_url, rankings_html = fetch_html(rankings_path)
        rankings_title = validate_stage_page(rankings_html, stage_number, year)
        ranking_tables = extract_tables(rankings_html)
        teams_rank, riders_rank = extract_links(rankings_html)
        teams = pd.concat([teams_stage, teams_rank], ignore_index=True).drop_duplicates(subset=['team_url'])
        riders = pd.concat([riders_stage, riders_rank], ignore_index=True).drop_duplicates(subset=['rider_url'])
        stage_row['rankings_url'] = rankings_url
        stage_row['rankings_page_title'] = rankings_title

    class_frames = []
    for idx, df in enumerate(ranking_tables):
        ctype = detect_classification_type(df, idx)
        class_frames.append(normalize_rider_table(df, stage_number, rankings_url, ctype))
    classifications = pd.concat(class_frames, ignore_index=True) if class_frames else pd.DataFrame()

    if not riders.empty:
        riders['norm_name'] = riders['rider_name'].map(norm)
    if not teams.empty:
        teams['norm_team'] = teams['team_name'].map(norm)
    if not classifications.empty:
        classifications['norm_name'] = classifications['rider_name'].map(norm)
        classifications['norm_team'] = classifications['team_name'].map(norm)
        if not riders.empty:
            classifications = classifications.merge(
                riders[['rider_name','rider_slug','rider_url','norm_name']].drop_duplicates('norm_name'),
                on='norm_name', how='left', suffixes=('','_lk')
            )
        if not teams.empty:
            classifications = classifications.merge(
                teams[['team_name','team_slug','team_url','norm_team']].drop_duplicates('norm_team'),
                on='norm_team', how='left', suffixes=('','_lk')
            )
        classifications = classifications.rename(columns={'rider_name':'rider_name_scraped','team_name':'team_name_scraped'})
        classifications['rider_name'] = classifications.get('rider_name_lk', classifications['rider_name_scraped'])
        classifications['team_name'] = classifications.get('team_name_lk', classifications['team_name_scraped'])
        for col in ['rider_slug','rider_url','team_slug','team_url']:
            if col not in classifications.columns:
                classifications[col] = None
        keep = ['race','stage_number','classification_type','rank','rider_name','rider_slug','rider_url','bib','team_name','team_slug','team_url','time','gap','points','bonus','source_url']
        classifications = classifications[keep]

    rider_dim = riders[['rider_name','rider_slug','rider_url']].drop_duplicates().sort_values(['rider_name','rider_url']) if not riders.empty else pd.DataFrame(columns=['rider_name','rider_slug','rider_url'])
    team_dim = teams[['team_name','team_slug','team_url']].drop_duplicates().sort_values(['team_name','team_url']) if not teams.empty else pd.DataFrame(columns=['team_name','team_slug','team_url'])

    stage_class = classifications[classifications['classification_type'] == 'stage'] if not classifications.empty else pd.DataFrame()
    if not stage_class.empty:
        top = stage_class.iloc[0]
        stage_row['winner'] = top.get('rider_name')
        stage_row['winner_url'] = top.get('rider_url')
        stage_row['team'] = top.get('team_name')
        stage_row['team_url'] = top.get('team_url')

    stage_row['poll_state'] = infer_stage_state(stage_row)
    stage_row['recommended_poll_minutes'] = recommended_poll_minutes(stage_row)

    return pd.DataFrame([stage_row]), classifications, team_dim, rider_dim


def write_schedule_artifacts(outdir: Path, year: int, stages: pd.DataFrame):
    cron_lines = [
        '# Hourly catch-all sync for La Vuelta',
        f'7 * * * * python lavuelta_multi_stage_builder.py --year {year} --start-stage 1 --end-stage 21 --outdir output/lavuelta-prod',
        '',
        '# During today\'s active stage window, poll every 15 minutes',
        '# Suggested cron ticks: */15 * * * *',
        '# Your app can inspect stages.csv -> poll_state and recommended_poll_minutes to decide whether to fan out a full stage refresh.'
    ]
    write_versioned_text(outdir, 'suggested_cron', year, '\n'.join(cron_lines))
    keep = ['stage_number','stage_name','stage_start_local','stage_finish_expected_local','stage_first_start_local','stage_last_arrival_local','poll_state','recommended_poll_minutes','cycling_url','rankings_url']
    schedule = stages[[c for c in keep if c in stages.columns]]
    write_versioned_csv(schedule, outdir, 'stage_schedule', year)
    payload = {
        'race': 'La Vuelta',
        'year': year,
        'generated_at': datetime.now().isoformat(),
        'notes': [
            'Use hourly polling outside active race windows.',
            'Use 15-minute polling from 30 minutes before start until 60 minutes after expected finish or last arrival.',
            'Treat a stage as effectively finished when the stage window has passed and two consecutive polls return unchanged results.'
        ]
    }
    text = json.dumps(payload, indent=2)
    (outdir / 'polling_plan.json').write_text(text, encoding='utf-8')
    (outdir / f'polling_plan_{year}.json').write_text(text, encoding='utf-8')


def write_app_bundle(outdir: Path, year: int, stages: pd.DataFrame, classifications: pd.DataFrame, teams: pd.DataFrame, riders: pd.DataFrame):
    if 'stage_number' not in classifications.columns:
        classifications = pd.DataFrame(columns=['stage_number'])
    records = []
    for _, s in stages.iterrows():
        stage_no = int(s['stage_number']) if pd.notna(s['stage_number']) else None
        stage_classes = classifications[classifications['stage_number'] == stage_no].fillna('').to_dict(orient='records')
        records.append({
            'stage': {k: (None if pd.isna(v) else v) for k, v in s.to_dict().items()},
            'classifications': stage_classes,
        })
    bundle = {
        'race': 'La Vuelta',
        'year': year,
        'source': 'lavuelta.es',
        'generated_at': datetime.now().isoformat(),
        'teams': teams.fillna('').to_dict(orient='records'),
        'riders': riders.fillna('').to_dict(orient='records'),
        'stages': records,
    }
    text = json.dumps(bundle, indent=2, ensure_ascii=False)
    (outdir / 'lavuelta_app_bundle.json').write_text(text, encoding='utf-8')
    (outdir / f'lavuelta_app_bundle_{year}.json').write_text(text, encoding='utf-8')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--year', type=int, default=2026)
    ap.add_argument('--start-stage', type=int, default=1)
    ap.add_argument('--end-stage', type=int, default=21)
    ap.add_argument('--outdir', default='output/lavuelta-multi-stage')
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    _route_url, route_html = fetch_html('/en/overall-route')
    route_table = parse_route_calendar(route_html, args.year)
    route_lookup = {
        int(row['stage_number']): row
        for row in route_table.to_dict(orient='records')
        if row.get('stage_number') is not None
    }

    stages_all, class_all, teams_all, riders_all = [], [], [], []
    for stage_number in range(args.start_stage, args.end_stage + 1):
        stage_df, class_df, team_df, rider_df = build_for_stage(stage_number, args.year, route_lookup.get(stage_number))
        stages_all.append(stage_df)
        class_all.append(class_df)
        teams_all.append(team_df)
        riders_all.append(rider_df)

    stages = pd.concat(stages_all, ignore_index=True) if stages_all else pd.DataFrame()
    classifications = pd.concat(class_all, ignore_index=True) if class_all else pd.DataFrame()
    teams = pd.concat(teams_all, ignore_index=True).drop_duplicates(subset=['team_url']) if teams_all else pd.DataFrame()
    riders = pd.concat(riders_all, ignore_index=True).drop_duplicates(subset=['rider_url']) if riders_all else pd.DataFrame()

    if not stages.empty:
        stages['status'] = stages['status'].fillna('scheduled')
        stages['date'] = stages['date'].fillna(pd.NA)
        stages['distance_km'] = stages['distance_km'].fillna(pd.NA)

    write_versioned_csv(stages, outdir, 'stages', args.year)
    write_versioned_csv(classifications, outdir, 'classifications', args.year)
    write_versioned_csv(teams, outdir, 'teams', args.year)
    write_versioned_csv(riders, outdir, 'riders', args.year)
    write_schedule_artifacts(outdir, args.year, stages)
    write_app_bundle(outdir, args.year, stages, classifications, teams, riders)
    manifest = pd.DataFrame([
        ('stages.csv', 'One row per stage with schedule windows, poll hints, and source URLs'),
        ('classifications.csv', 'Ranking rows per stage with classification types and rider/team links'),
        ('teams.csv', 'Unique teams with lavuelta.es links'),
        ('riders.csv', 'Unique riders with lavuelta.es links for page rendering'),
        ('stage_schedule.csv', 'Scheduling helper for your app'),
        ('polling_plan.json', 'Machine-readable polling guidance'),
        ('suggested_cron.txt', 'Suggested cron entries'),
        ('lavuelta_app_bundle.json', 'App-friendly JSON export'),
        ('stages_YYYY.csv', 'Year-tagged stage calendar archive'),
        ('classifications_YYYY.csv', 'Year-tagged classification archive'),
        ('teams_YYYY.csv', 'Year-tagged team archive'),
        ('riders_YYYY.csv', 'Year-tagged rider archive'),
        ('stage_schedule_YYYY.csv', 'Year-tagged scheduling helper archive'),
        ('polling_plan_YYYY.json', 'Year-tagged polling guidance archive'),
        ('suggested_cron_YYYY.txt', 'Year-tagged cron archive'),
        ('lavuelta_app_bundle_YYYY.json', 'Year-tagged bundle archive'),
    ], columns=['file','description'])
    write_versioned_csv(manifest, outdir, 'manifest', args.year)

    print(f'Wrote La Vuelta outputs for stages {args.start_stage}..{args.end_stage} to {outdir}')

if __name__ == '__main__':
    main()
