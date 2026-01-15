"""
CoinDesk API Test Script (Updated with Correct Endpoints)
Testing API availability and authentication
"""

import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variable
API_KEY = os.getenv('COINDESK_API_KEY')

# CoinDesk API base URL
BASE_URL = "https://data-api.coindesk.com"

print("="*80)
print(" "*20 + "COINDESK API - TESTING SCRIPT")
print("="*80)

# Check if API key is loaded
print(f"\n🔑 API Key Status: ", end="")
if API_KEY:
    print(f"✅ Loaded (ends with: ...{API_KEY[-8:]})")
else:
    print("❌ NOT FOUND!")
    print("\n⚠️  Please make sure COINDESK_API_KEY is set in your .env file")
    print("   Example in .env file: COINDESK_API_KEY=your_actual_key_here")
    exit(1)

print(f"🌐 Base URL: {BASE_URL}\n")

# ============================================================================
# Test 1: API Version (No auth required)
# ============================================================================
print("\n" + "="*80)
print("TEST 1: API Version Check (No authentication required)")
print("="*80)

try:
    url = f"{BASE_URL}/info/v1/version"
    
    print(f"📡 Endpoint: {url}")
    response = requests.get(url, timeout=10)
    
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        print("✅ SUCCESS! CoinDesk API is reachable\n")
        data = response.json()
        print(f"Response:\n{json.dumps(data, indent=2)}")
    else:
        print(f"❌ Failed: {response.status_code}")
        print(f"Response: {response.text[:300]}")
        
except Exception as e:
    print(f"❌ Error: {e}")

# ============================================================================
# Test 2: Rate Limit Check (With Authentication)
# ============================================================================
print("\n\n" + "="*80)
print("TEST 2: Rate Limit Check (With API Key Authentication)")
print("="*80)

try:
    url = f"{BASE_URL}/admin/v2/rate/limit"
    
    # CoinDesk accepts API key as query parameter or in header
    params = {'api_key': API_KEY}
    headers = {'Authorization': f'Bearer {API_KEY}'}
    
    print(f"📡 Endpoint: {url}")
    print("🔐 Using API key for authentication...")
    
    # Try with query parameter first
    response = requests.get(url, params=params, timeout=10)
    
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        print("✅ SUCCESS! API key is valid\n")
        data = response.json()
        print(f"Rate Limit Info:\n{json.dumps(data, indent=2)}")
    elif response.status_code == 401:
        print("❌ UNAUTHORIZED - API key invalid or expired")
        print(f"Response: {response.text[:300]}")
    elif response.status_code == 403:
        print("❌ FORBIDDEN - API key doesn't have permission")
        print(f"Response: {response.text[:300]}")
    else:
        print(f"❌ Failed: {response.status_code}")
        print(f"Response: {response.text[:300]}")
        
except Exception as e:
    print(f"❌ Error: {e}")

# ============================================================================
# Test 3: Market Data - Index Latest Tick (BTC-USD)
# ============================================================================
print("\n\n" + "="*80)
print("TEST 3: Bitcoin Price Data (Index Latest Tick)")
print("="*80)

try:
    url = f"{BASE_URL}/index/cc/v1/latest/tick"
    params = {
        'market': 'ccix',
        'instruments': 'BTC-USD',
        'api_key': API_KEY
    }
    
    print(f"📡 Endpoint: {url}")
    print(f"📊 Market: CCIX (CoinDesk Index)")
    print(f"💰 Instrument: BTC-USD")
    
    response = requests.get(url, params=params, timeout=10)
    
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        print("✅ SUCCESS! Market data retrieved\n")
        data = response.json()
        print(f"Bitcoin Price Data:\n{json.dumps(data, indent=2)}")
    else:
        print(f"Response: {response.text[:500]}")
        
except Exception as e:
    print(f"❌ Error: {e}")

# ============================================================================
# Test 4: Overview Endpoint (Market Snapshot)
# ============================================================================
print("\n\n" + "="*80)
print("TEST 4: Market Overview (Market Snapshot)")
print("="*80)

try:
    # Try different potential overview endpoints
    overview_endpoints = [
        f"{BASE_URL}/overview/v1/latest",
        f"{BASE_URL}/overview/v1/snapshot",
    ]
    
    for endpoint_url in overview_endpoints:
        print(f"\n📡 Trying: {endpoint_url}")
        
        params = {'api_key': API_KEY}
        response = requests.get(endpoint_url, params=params, timeout=10)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ SUCCESS!\n")
            data = response.json()
            json_str = json.dumps(data, indent=2)
            if len(json_str) > 1000:
                print(f"Response (first 1000 chars):\n{json_str[:1000]}...")
            else:
                print(f"Response:\n{json_str}")
            break
        elif response.status_code == 404:
            print("❌ Endpoint not found, trying next...")
        else:
            print(f"Response: {response.text[:300]}")
        
except Exception as e:
    print(f"❌ Error: {e}")

# ============================================================================
# Summary and Recommendations
# ============================================================================
print("\n\n" + "="*80)
print(" "*25 + "SUMMARY & NEXT STEPS")
print("="*80)

print("""
📊 WHAT THIS TEST CHECKED:
   1. API connectivity and version
   2. API key authentication
   3. Rate limit status
   4. Market data access (BTC price)
   5. Overview/snapshot endpoints

✅ IF YOU SAW SUCCESS MESSAGES:
   → Your CoinDesk API is working correctly!
   → Your API key is valid and authenticated
   → You can proceed with building your ETL pipeline

❌ IF YOU SAW ERRORS:
   → 401/403: Check your API key validity on CoinDesk dashboard
   → 404: Some endpoints might require different subscription tiers
   → Connection errors: Check your network/DNS
   → Rate limit: You've exceeded your API call limits

📚 USEFUL RESOURCES:
   → Documentation: https://www.coindesk.com/indices/cryptocurrency-data-api
   → OpenAPI Spec: https://data-api.coindesk.com/info/v1/openapi

🔑 API KEY BEST PRACTICES FOR AIRFLOW:
   1. Never hardcode API keys in DAG files
   2. Use Airflow Variables: Variable.get("COINDESK_API_KEY")
   3. Or use Airflow Connections for better security
   4. Monitor rate limits in your DAG logic
   5. Implement retry logic for failed API calls

🚀 NEXT STEPS FOR YOUR ETL PROJECT:
   1. Identify which endpoints you need (price, volume, market cap, etc.)
   2. Design your database schema to store the data
   3. Create Airflow DAG to:
      • Extract: Call CoinDesk API
      • Transform: Process and clean data
      • Load: Insert into your database
   4. Set up Metabase connection to your database
   5. Build dashboards and visualizations!

💡 TIP: Start with simple endpoints that work, then expand gradually!
""")

print("="*80)
print(" "*20 + "Test completed at:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("="*80)
