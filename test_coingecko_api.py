"""
CoinGecko API Test Script
Learn about the API and understand the data structure

CoinGecko API is FREE and doesn't require an API key for basic usage!
Rate Limits (Free tier): 10-50 calls/minute
"""

import requests
import json
from datetime import datetime

print("="*80)
print(" "*25 + "COINGECKO API TEST")
print("="*80)

# CoinGecko API base URL
BASE_URL = "https://api.coingecko.com/api/v3"

# ============================================================================
# Test 1: API Health Check (Ping)
# ============================================================================
print("\n" + "="*80)
print("TEST 1: API Health Check")
print("="*80)

try:
    url = f"{BASE_URL}/ping"
    print(f"📡 Endpoint: {url}")
    
    response = requests.get(url, timeout=10)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        print("✅ SUCCESS! CoinGecko API is reachable\n")
        data = response.json()
        print(f"Response: {json.dumps(data, indent=2)}")
    else:
        print(f"❌ Failed: {response.status_code}")
        print(f"Response: {response.text}")
        
except Exception as e:
    print(f"❌ Error: {e}")

# ============================================================================
# Test 2: Get Top 3 Cryptocurrencies by Market Cap
# ============================================================================
print("\n\n" + "="*80)
print("TEST 2: Fetch Top 3 Cryptocurrencies")
print("="*80)

try:
    url = f"{BASE_URL}/coins/markets"
    
    # Parameters for the API call
    params = {
        'vs_currency': 'usd',           # Prices in USD
        'order': 'market_cap_desc',     # Order by market cap (highest first)
        'per_page': 3,                  # Get top 3 coins
        'page': 1,                      # First page
        'sparkline': False,             # Don't include sparkline data
        'price_change_percentage': '24h' # Include 24h price change
    }
    
    print(f"📡 Endpoint: {url}")
    print(f"📊 Parameters: {params}\n")
    
    response = requests.get(url, params=params, timeout=10)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        print("✅ SUCCESS! Got top 3 cryptocurrencies\n")
        data = response.json()
        
        # Display in a nice format
        print("-" * 80)
        for coin in data:
            print(f"\n🪙 {coin['name']} ({coin['symbol'].upper()})")
            print(f"   Rank: #{coin['market_cap_rank']}")
            print(f"   Current Price: ${coin['current_price']:,.2f}")
            print(f"   Market Cap: ${coin['market_cap']:,.0f}")
            print(f"   24h Volume: ${coin['total_volume']:,.0f}")
            print(f"   24h Change: {coin['price_change_percentage_24h']:.2f}%")
            print(f"   Last Updated: {coin['last_updated']}")
        print("-" * 80)
        
        # Show full JSON for one coin (so you can see all available fields)
        print("\n📋 Full data structure for first coin:")
        print(json.dumps(data[0], indent=2))
        
    else:
        print(f"❌ Failed: {response.status_code}")
        print(f"Response: {response.text[:500]}")
        
except Exception as e:
    print(f"❌ Error: {e}")

# ============================================================================
# Test 3: Get Historical Price Data (Last 7 days for Bitcoin)
# ============================================================================
print("\n\n" + "="*80)
print("TEST 3: Historical Price Data (Bitcoin - Last 7 days)")
print("="*80)

try:
    # For historical data, we use the market_chart endpoint
    coin_id = "bitcoin"  # CoinGecko uses coin IDs, not symbols
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    
    params = {
        'vs_currency': 'usd',
        'days': 7,              # Last 7 days
        'interval': 'daily'     # Daily data points
    }
    
    print(f"📡 Endpoint: {url}")
    print(f"📊 Parameters: {params}\n")
    
    response = requests.get(url, params=params, timeout=10)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        print("✅ SUCCESS! Got historical data\n")
        data = response.json()
        
        print(f"Available data types: {list(data.keys())}")
        print(f"Number of price points: {len(data['prices'])}")
        print(f"Number of market cap points: {len(data['market_caps'])}")
        print(f"Number of volume points: {len(data['total_volumes'])}")
        
        # Show first few price points
        print("\n📈 Sample price data (first 3 points):")
        for i, (timestamp, price) in enumerate(data['prices'][:3]):
            dt = datetime.fromtimestamp(timestamp / 1000)  # Convert from milliseconds
            print(f"   {dt.strftime('%Y-%m-%d %H:%M:%S')}: ${price:,.2f}")
        
        print("\n📊 Sample volume data (first 3 points):")
        for i, (timestamp, volume) in enumerate(data['total_volumes'][:3]):
            dt = datetime.fromtimestamp(timestamp / 1000)
            print(f"   {dt.strftime('%Y-%m-%d %H:%M:%S')}: ${volume:,.0f}")
            
    else:
        print(f"❌ Failed: {response.status_code}")
        print(f"Response: {response.text[:500]}")
        
except Exception as e:
    print(f"❌ Error: {e}")

# ============================================================================
# Summary and Key Learnings
# ============================================================================
print("\n\n" + "="*80)
print(" "*30 + "KEY LEARNINGS")
print("="*80)

print("""
📊 WHAT WE LEARNED:

1. CoinGecko API is FREE and EASY to use
   ✅ No API key required for basic usage
   ✅ Good rate limits for learning projects
   
2. Important Endpoints for Our ETL:
   
   📌 /coins/markets
      → Get current price, volume, market cap for top coins
      → Perfect for our 15-minute updates
      → Returns: price, volume, market_cap, 24h changes, rank, etc.
   
   📌 /coins/{id}/market_chart
      → Get historical price data
      → Can specify: days=1,7,14,30,365, or 'max'
      → Returns: prices, market_caps, total_volumes (as timestamps)
      → Perfect for backfilling historical data

3. Data Structure Understanding:
   
   For CURRENT data (/coins/markets):
   {
     "id": "bitcoin",
     "symbol": "btc",
     "name": "Bitcoin",
     "current_price": 96500.00,
     "market_cap": 1900000000000,
     "total_volume": 45000000000,
     "price_change_percentage_24h": 2.5,
     "market_cap_rank": 1,
     "last_updated": "2024-01-01T12:00:00.000Z"
   }
   
   For HISTORICAL data (/coins/{id}/market_chart):
   {
     "prices": [[timestamp_ms, price], ...],
     "market_caps": [[timestamp_ms, market_cap], ...],
     "total_volumes": [[timestamp_ms, volume], ...]
   }

4. What We'll Build:
   
   🔄 ETL Pipeline (Every 15 minutes):
      → Extract: Call /coins/markets for top 3 coins
      → Transform: Parse JSON, extract fields we need
      → Load: Insert into PostgreSQL database
   
   📊 Historical Data Pipeline (One-time or daily):
      → Extract: Call /coins/{id}/market_chart for each coin
      → Transform: Parse timestamp arrays
      → Load: Bulk insert historical data

🚀 NEXT STEP:
   Now that we understand the API, let's design our database schema!
   We need tables to store:
   - Current crypto prices (updated every 15 min)
   - Historical prices (for charts and trends)
   
""")

print("="*80)
print(f"Test completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)
