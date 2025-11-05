#!/usr/bin/env python3
"""Test NBA API with HTTPS proxy protocol."""
import requests
import time
import json

username = "sp4rff95tp"
password = "ooed85IM5+ezHlo7Tn"
host = "dc.decodo.com"
port = 10001

# Use HTTPS protocol for proxy connection
proxy_https = f"https://{username}:{password}@{host}:{port}"
print(f"Testing with HTTPS proxy: {proxy_https[:50]}...")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.nba.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nba.com"
}

# Test NBA API
url = "https://stats.nba.com/stats/scoreboardv2?GameDate=2025-11-05&LeagueID=00&DayOffset=0"
print(f"\nTesting NBA API...")

try:
    start = time.time()
    result = requests.get(url, 
                         proxies={"https": proxy_https}, 
                         headers=headers,
                         timeout=90,
                         verify=True)
    elapsed = time.time() - start
    print(f"✅ Success! Status: {result.status_code} in {elapsed:.1f}s")
    print(f"   Response size: {len(result.text)} bytes")
    if result.status_code == 200:
        data = result.json()
        result_sets = data.get("resultSets", [])
        print(f"   Result sets: {len(result_sets)}")
        if result_sets:
            games = result_sets[0].get("rowSet", [])
            print(f"   Games found: {len(games)}")
            if games:
                print(f"\n   Sample game: {games[0][:3]}")
except Exception as e:
    elapsed = time.time() - start if "start" in locals() else 0
    print(f"❌ Failed after {elapsed:.1f}s: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

