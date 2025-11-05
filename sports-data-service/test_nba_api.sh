#!/bin/bash
# Test script for NBA API connectivity
# Run this from a server to test if stats.nba.com is reachable

echo "=== Testing NBA API Connectivity ==="
echo ""

# Test 1: Basic connectivity
echo "1. Testing basic connectivity to stats.nba.com..."
curl -I -s --max-time 10 https://stats.nba.com | head -5
echo ""

# Test 2: Scoreboard endpoint for today
echo "2. Testing ScoreboardV2 endpoint for today (2025-11-04)..."
curl -s --max-time 30 \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" \
  -H "Referer: https://www.nba.com/" \
  -H "Accept: application/json, text/plain, */*" \
  -H "Accept-Language: en-US,en;q=0.9" \
  -H "Origin: https://www.nba.com" \
  "https://stats.nba.com/stats/scoreboardv2?GameDate=2025-11-04&LeagueID=00&DayOffset=0" | \
  python3 -m json.tool 2>/dev/null | head -50 || echo "Failed or timeout"
echo ""

# Test 3: Scoreboard endpoint for tomorrow
echo "3. Testing ScoreboardV2 endpoint for tomorrow (2025-11-05)..."
curl -s --max-time 30 \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" \
  -H "Referer: https://www.nba.com/" \
  -H "Accept: application/json, text/plain, */*" \
  "https://stats.nba.com/stats/scoreboardv2?GameDate=2025-11-05&LeagueID=00&DayOffset=0" | \
  python3 -m json.tool 2>/dev/null | head -30 || echo "Failed or timeout"
echo ""

# Test 4: Check if we can parse the response
echo "4. Checking game count in response..."
RESPONSE=$(curl -s --max-time 30 \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
  -H "Referer: https://www.nba.com/" \
  "https://stats.nba.com/stats/scoreboardv2?GameDate=2025-11-04&LeagueID=00&DayOffset=0")

if [ $? -eq 0 ] && [ -n "$RESPONSE" ]; then
    GAMES=$(echo "$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); games=data.get('resultSets',[{}])[0].get('rowSet',[]); print(len(games))" 2>/dev/null)
    if [ -n "$GAMES" ]; then
        echo "   Found $GAMES games in response"
    else
        echo "   Response received but couldn't parse games"
    fi
else
    echo "   Request failed or timed out"
fi

echo ""
echo "=== Test Complete ==="

