#!/usr/bin/env python3
"""Test NBA API with proxy."""
import sys
sys.path.insert(0, '/app/dependencies/nba_api/src')
from nba_api.stats.endpoints import scoreboardv2
import requests
from datetime import date

# Test proxy directly
proxy = "http://sp4rff95tp:ooed85IM5+ezHlo7Tn@dc.decodo.com:10001"
print("Testing proxy connection...")
try:
    result = requests.get("https://ip.decodo.com/json", proxies={"http": proxy, "https": proxy}, timeout=10)
    print(f"Proxy test: {result.status_code}")
    print(f"Response: {result.text[:200]}")
except Exception as e:
    print(f"Proxy test failed: {e}")

# Test NBA API with proxy
print("\nTesting NBA API with proxy...")
date_str = date.today().strftime("%Y-%m-%d")
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.nba.com/",
    "Accept": "application/json, text/plain, */*",
}

# Patch requests to use proxy
import requests
original_get = requests.Session.get
original_request = requests.Session.request

def patched_get(self, url, **kwargs):
    kwargs['proxies'] = {"http": proxy, "https": proxy}
    return original_get(self, url, **kwargs)

def patched_request(self, method, url, **kwargs):
    kwargs['proxies'] = {"http": proxy, "https": proxy}
    return original_request(self, method, url, **kwargs)

requests.Session.get = patched_get
requests.Session.request = patched_request

try:
    scoreboard = scoreboardv2.ScoreboardV2(game_date=date_str, headers=headers, timeout=60)
    data = scoreboard.get_dict()
    result_sets = data.get("resultSets", [])
    print(f"NBA API success! Got {len(result_sets)} result sets")
    if result_sets:
        games = result_sets[0].get("rowSet", [])
        print(f"Found {len(games)} games")
except Exception as e:
    print(f"NBA API failed: {e}")
    import traceback
    traceback.print_exc()

