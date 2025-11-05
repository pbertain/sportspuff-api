#!/bin/bash
# Validation script for Sports Data Service API
# Tests service health and retrieves schedules for NBA, NHL, and NFL

set -e

API_PORT=${API_PORT:-34180}
API_URL="http://localhost:${API_PORT}"
TODAY=$(date +%Y%m%d)

echo "=========================================="
echo "Sports Data Service API Validation"
echo "=========================================="
echo ""
echo "API URL: ${API_URL}"
echo "Date: $(date '+%Y-%m-%d')"
echo ""

# Check if service is running
echo "1. Checking service health..."
if curl -s -f "${API_URL}/health" > /dev/null 2>&1; then
    echo "   ✓ Service is running and healthy"
    HEALTH_RESPONSE=$(curl -s "${API_URL}/health")
    echo "   Response: ${HEALTH_RESPONSE}"
else
    echo "   ✗ Service is not accessible"
    echo ""
    echo "   To start the service:"
    echo "   cd $(dirname "$0")"
    echo "   docker-compose up -d"
    echo ""
    exit 1
fi

echo ""
echo "2. Testing NBA schedule for today..."
NBA_RESPONSE=$(curl -s "${API_URL}/api/v1/schedule/nba/today")
if echo "$NBA_RESPONSE" | grep -q "games"; then
    NBA_COUNT=$(echo "$NBA_RESPONSE" | grep -o '"game_id"' | wc -l | tr -d ' ')
    echo "   ✓ NBA schedule retrieved successfully"
    echo "   Games found: ${NBA_COUNT}"
    echo "   Response preview:"
    echo "$NBA_RESPONSE" | python3 -m json.tool 2>/dev/null | head -20 || echo "$NBA_RESPONSE" | head -10
else
    echo "   ⚠ NBA schedule endpoint responded but format may be unexpected"
    echo "   Response: $NBA_RESPONSE"
fi

echo ""
echo "3. Testing NHL schedule for today..."
NHL_RESPONSE=$(curl -s "${API_URL}/api/v1/schedule/nhl/today")
if echo "$NHL_RESPONSE" | grep -q "games"; then
    NHL_COUNT=$(echo "$NHL_RESPONSE" | grep -o '"game_id"' | wc -l | tr -d ' ')
    echo "   ✓ NHL schedule retrieved successfully"
    echo "   Games found: ${NHL_COUNT}"
    echo "   Response preview:"
    echo "$NHL_RESPONSE" | python3 -m json.tool 2>/dev/null | head -20 || echo "$NHL_RESPONSE" | head -10
else
    echo "   ⚠ NHL schedule endpoint responded but format may be unexpected"
    echo "   Response: $NHL_RESPONSE"
fi

echo ""
echo "4. Testing NFL schedule for today..."
NFL_RESPONSE=$(curl -s "${API_URL}/api/v1/schedule/nfl/today")
if echo "$NFL_RESPONSE" | grep -q "games"; then
    NFL_COUNT=$(echo "$NFL_RESPONSE" | grep -o '"game_id"' | wc -l | tr -d ' ')
    echo "   ✓ NFL schedule retrieved successfully"
    echo "   Games found: ${NFL_COUNT}"
    echo "   Response preview:"
    echo "$NFL_RESPONSE" | python3 -m json.tool 2>/dev/null | head -20 || echo "$NFL_RESPONSE" | head -10
else
    echo "   ⚠ NFL schedule endpoint responded but format may be unexpected"
    echo "   Response: $NFL_RESPONSE"
fi

echo ""
echo "5. Testing combined schedules endpoint..."
ALL_RESPONSE=$(curl -s "${API_URL}/api/v1/schedules/today")
if echo "$ALL_RESPONSE" | grep -q "sports"; then
    echo "   ✓ Combined schedules endpoint working"
    echo "   Response preview:"
    echo "$ALL_RESPONSE" | python3 -m json.tool 2>/dev/null | head -30 || echo "$ALL_RESPONSE" | head -15
else
    echo "   ⚠ Combined schedules endpoint responded but format may be unexpected"
    echo "   Response: $ALL_RESPONSE"
fi

echo ""
echo "=========================================="
echo "Validation Complete"
echo "=========================================="
echo ""
echo "API Endpoints for sportspuff-v6:"
echo "  - NBA: ${API_URL}/api/v1/schedule/nba/today"
echo "  - NHL: ${API_URL}/api/v1/schedule/nhl/today"
echo "  - NFL: ${API_URL}/api/v1/schedule/nfl/today"
echo "  - All: ${API_URL}/api/v1/schedules/today"
echo ""

