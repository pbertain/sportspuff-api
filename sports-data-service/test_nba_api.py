#!/usr/bin/env python3
"""
Test script for NBA API connectivity.
Run this from a server to test if stats.nba.com is reachable.
"""

import requests
import json
import sys
from datetime import date, timedelta

def test_nba_api(game_date=None):
    """Test NBA API connectivity and fetch scoreboard data."""
    
    if game_date is None:
        game_date = date.today()
    
    date_str = game_date.strftime('%Y-%m-%d')
    
    # Headers that NBA.com expects
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.nba.com/',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://www.nba.com'
    }
    
    # NBA ScoreboardV2 endpoint
    url = f"https://stats.nba.com/stats/scoreboardv2?GameDate={date_str}&LeagueID=00&DayOffset=0"
    
    print(f"Testing NBA API for date: {date_str}")
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    print("-" * 60)
    
    try:
        # Test 1: Basic connectivity
        print("\n1. Testing basic connectivity...")
        response = requests.get("https://stats.nba.com", headers=headers, timeout=10)
        print(f"   Status: {response.status_code}")
        
        # Test 2: Scoreboard endpoint
        print(f"\n2. Fetching scoreboard for {date_str}...")
        response = requests.get(url, headers=headers, timeout=30)
        
        print(f"   Status Code: {response.status_code}")
        print(f"   Response Size: {len(response.content)} bytes")
        
        if response.status_code == 200:
            try:
                data = response.json()
                
                # Extract games from resultSets
                if 'resultSets' in data and len(data['resultSets']) > 0:
                    game_header = data['resultSets'][0]
                    games = game_header.get('rowSet', [])
                    
                    print(f"\n   ✅ Success! Found {len(games)} games")
                    
                    if games:
                        print("\n   Games:")
                        for i, game in enumerate(games[:6], 1):
                            if len(game) >= 8:
                                # GameHeader format: [GAME_ID, GAME_DATE_EST, GAME_SEQUENCE, GAME_STATUS_ID, 
                                #                     GAME_STATUS_TEXT, GAME_STATUS, HOME_TEAM_ID, VISITOR_TEAM_ID, ...]
                                game_id = game[0]
                                status = game[5] if len(game) > 5 else "N/A"
                                print(f"      Game {i}: ID={game_id}, Status={status}")
                        
                        # Show full first game for debugging
                        if len(games) > 0:
                            print(f"\n   First game data (first 10 fields): {games[0][:10]}")
                    else:
                        print("   ⚠️  No games found for this date")
                else:
                    print("   ⚠️  Response structure unexpected")
                    print(f"   Response keys: {list(data.keys())[:10]}")
                    
            except json.JSONDecodeError as e:
                print(f"   ❌ Failed to parse JSON: {e}")
                print(f"   Response preview: {response.text[:200]}")
        else:
            print(f"   ❌ HTTP Error: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            
    except requests.exceptions.Timeout:
        print(f"   ❌ Request timed out after 30 seconds")
        return False
    except requests.exceptions.ConnectionError as e:
        print(f"   ❌ Connection error: {e}")
        return False
    except Exception as e:
        print(f"   ❌ Error: {type(e).__name__}: {e}")
        return False
    
    return True


def test_multiple_dates(days_ahead=7):
    """Test NBA API for multiple dates."""
    print("=" * 60)
    print("Testing NBA API for multiple dates")
    print("=" * 60)
    
    today = date.today()
    results = {}
    
    for day_offset in range(days_ahead + 1):
        test_date = today + timedelta(days=day_offset)
        print(f"\n{'='*60}")
        success = test_nba_api(test_date)
        results[test_date.isoformat()] = success
        
        if day_offset < days_ahead:
            import time
            time.sleep(1)  # Small delay between requests
    
    print(f"\n{'='*60}")
    print("Summary:")
    for date_str, success in results.items():
        status = "✅ Success" if success else "❌ Failed"
        print(f"  {date_str}: {status}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--multiple":
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
            test_multiple_dates(days)
        else:
            # Test specific date
            test_date = date.fromisoformat(sys.argv[1])
            test_nba_api(test_date)
    else:
        # Test today
        test_nba_api()

