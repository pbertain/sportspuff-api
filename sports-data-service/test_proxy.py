#!/usr/bin/env python3
"""
Test script to verify Decodo proxy connectivity.
"""

import requests
import sys

# Decodo proxy configuration
username = 'sp4rff95tp'
password = 'ooed85IM5+ezHlo7Tn'
proxy_host = 'dc.decodo.com'
port = 10001  # Test with first port

proxy = f"http://{username}:{password}@{proxy_host}:{port}"

print("="*60)
print("Testing Decodo Proxy Connectivity")
print("="*60)
print(f"Proxy: {proxy_host}:{port}")
print(f"Username: {username}")
print("-"*60)

# Test 1: Check proxy IP
print("\n1. Testing proxy IP detection...")
try:
    url = 'https://ip.decodo.com/json'
    result = requests.get(url, proxies={
        'http': proxy,
        'https': proxy
    }, timeout=30)
    print(f"✅ Success! Status: {result.status_code}")
    print(f"   Response: {result.text}")
except Exception as e:
    print(f"❌ Failed: {e}")
    sys.exit(1)

# Test 2: Test NBA API through proxy
print("\n2. Testing NBA API through proxy...")
try:
    nba_url = "https://stats.nba.com/stats/scoreboardv2?GameDate=2025-11-04&LeagueID=00&DayOffset=0"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.nba.com/',
        'Accept': 'application/json, text/plain, */*',
    }
    result = requests.get(nba_url, proxies={
        'http': proxy,
        'https': proxy
    }, headers=headers, timeout=60)
    print(f"✅ Success! Status: {result.status_code}")
    if result.status_code == 200:
        data = result.json()
        print(f"   Response size: {len(str(data))} bytes")
        if 'resultSets' in data:
            print(f"   Found {len(data['resultSets'])} result sets")
    else:
        print(f"   Response: {result.text[:200]}")
except Exception as e:
    print(f"❌ Failed: {e}")

# Test 3: Test multiple ports
print("\n3. Testing multiple proxy ports...")
ports = [10001, 10002, 10003, 10004, 10005]
success_count = 0
for port in ports:
    try:
        test_proxy = f"http://{username}:{password}@{proxy_host}:{port}"
        result = requests.get('https://ip.decodo.com/json', proxies={
            'http': test_proxy,
            'https': test_proxy
        }, timeout=10)
        print(f"   Port {port}: ✅ Success")
        success_count += 1
    except Exception as e:
        print(f"   Port {port}: ❌ Failed ({e})")

print(f"\n{'='*60}")
print(f"Summary: {success_count}/{len(ports)} ports working")
print(f"{'='*60}\n")

