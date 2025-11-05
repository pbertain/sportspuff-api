#!/usr/bin/env python3
"""
Test script for LeagueGameFinder to fetch NBA and WNBA season schedules.
This can be run locally or on the server to test connectivity.
"""

import sys
sys.path.insert(0, '/app/dependencies/nba_api/src')

from nba_api.stats.endpoints import leaguegamefinder
import time

# Custom headers
nba_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://www.nba.com/',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Origin': 'https://www.nba.com'
}

def test_nba_season(season="2025-26"):
    """Test fetching NBA season schedule."""
    print(f"\n{'='*60}")
    print(f"Testing NBA Season: {season}")
    print(f"{'='*60}")
    
    try:
        start = time.time()
        print(f"Calling LeagueGameFinder...")
        gf = leaguegamefinder.LeagueGameFinder(
            season_nullable=season,
            league_id_nullable='00',  # NBA league ID
            headers=nba_headers,
            timeout=90
        )
        data = gf.get_dict()
        elapsed = time.time() - start
        
        if 'resultSets' in data and len(data['resultSets']) > 0:
            game_results = data['resultSets'][0]
            game_rows = game_results.get('rowSet', [])
            headers = game_results.get('headers', [])
            
            print(f"✅ Success! Got {len(game_rows)} rows in {elapsed:.1f}s")
            print(f"   Headers: {headers[:5]}...")
            if len(game_rows) > 0:
                print(f"   Sample row: {game_rows[0][:7]}")
            return True
        else:
            print(f"⚠️  Got response but no resultSets")
            print(f"   Response keys: {list(data.keys())}")
            return False
            
    except Exception as e:
        elapsed = time.time() - start
        print(f"❌ Failed after {elapsed:.1f}s: {e}")
        return False

def test_wnba_season(season="2025"):
    """Test fetching WNBA season schedule."""
    print(f"\n{'='*60}")
    print(f"Testing WNBA Season: {season}")
    print(f"{'='*60}")
    
    try:
        start = time.time()
        print(f"Calling LeagueGameFinder...")
        gf = leaguegamefinder.LeagueGameFinder(
            season_nullable=season,
            league_id_nullable='10',  # WNBA league ID
            headers=nba_headers,
            timeout=90
        )
        data = gf.get_dict()
        elapsed = time.time() - start
        
        if 'resultSets' in data and len(data['resultSets']) > 0:
            game_results = data['resultSets'][0]
            game_rows = game_results.get('rowSet', [])
            headers = game_results.get('headers', [])
            
            print(f"✅ Success! Got {len(game_rows)} rows in {elapsed:.1f}s")
            print(f"   Headers: {headers[:5]}...")
            if len(game_rows) > 0:
                print(f"   Sample row: {game_rows[0][:7]}")
            return True
        else:
            print(f"⚠️  Got response but no resultSets")
            print(f"   Response keys: {list(data.keys())}")
            return False
            
    except Exception as e:
        elapsed = time.time() - start
        print(f"❌ Failed after {elapsed:.1f}s: {e}")
        return False

if __name__ == "__main__":
    print("\n" + "="*60)
    print("LeagueGameFinder Test Script")
    print("="*60)
    
    # Test NBA 2025-26 season
    nba_success = test_nba_season("2025-26")
    
    # Test WNBA 2025 season
    wnba_success = test_wnba_season("2025")
    
    print(f"\n{'='*60}")
    print("Summary:")
    print(f"  NBA 2025-26:  {'✅ Success' if nba_success else '❌ Failed'}")
    print(f"  WNBA 2025:    {'✅ Success' if wnba_success else '❌ Failed'}")
    print(f"{'='*60}\n")

