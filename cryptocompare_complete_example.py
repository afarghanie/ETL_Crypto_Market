"""
CryptoCompare API - Perfect for Your ETL Project!
No API key required for basic use
"""

import requests
import json
from datetime import datetime

print("="*70)
print("CRYPTOCOMPARE API - YOUR BEST OPTION")
print("="*70)

# ============================================================================
# Test 1: Get Current Prices for Multiple Coins
# ============================================================================
print("\n📊 Test 1: Current Prices")
print("-"*70)

url = "https://min-api.cryptocompare.com/data/pricemultifull"
params = {
    'fsyms': 'BTC,ETH,BNB,SOL,XRP,ADA,DOGE,MATIC,DOT,AVAX',
    'tsyms': 'USD,EUR,IDR'  # Multiple currencies!
}

response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    
    if 'RAW' in data:
        print("\n✅ Successfully retrieved data!\n")
        print(f"{'Coin':<8} {'USD Price':>15} {'24h Change':>12} {'Market Cap':>20}")
        print("-"*70)
        
        for coin, values in data['RAW'].items():
            usd_data = values['USD']
            price = usd_data['PRICE']
            change = usd_data['CHANGEPCT24HOUR']
            mktcap = usd_data['MKTCAP']
            
            print(f"{coin:<8} ${price:>14,.2f} {change:>11.2f}% ${mktcap:>18,.0f}")

# ============================================================================
# Test 2: Get Historical Data (for charts)
# ============================================================================
print("\n\n📈 Test 2: Historical Data (Last 7 days)")
print("-"*70)

url = "https://min-api.cryptocompare.com/data/v2/histoday"
params = {
    'fsym': 'BTC',
    'tsym': 'USD',
    'limit': 7  # Last 7 days
}

response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    
    if 'Data' in data and 'Data' in data['Data']:
        print("\n✅ Bitcoin Price History (Last 7 Days):\n")
        print(f"{'Date':<12} {'Open':>12} {'High':>12} {'Low':>12} {'Close':>12}")
        print("-"*70)
        
        for day in data['Data']['Data']:
            date = datetime.fromtimestamp(day['time']).strftime('%Y-%m-%d')
            print(f"{date:<12} ${day['open']:>11,.2f} ${day['high']:>11,.2f} "
                  f"${day['low']:>11,.2f} ${day['close']:>11,.2f}")

# ============================================================================
# Test 3: Get Top Coins by Market Cap
# ============================================================================
print("\n\n🏆 Test 3: Top Coins by Market Cap")
print("-"*70)

url = "https://min-api.cryptocompare.com/data/top/mktcapfull"
params = {
    'limit': 10,
    'tsym': 'USD'
}

response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    
    if 'Data' in data:
        print("\n✅ Top 10 Cryptocurrencies:\n")
        
        for i, coin_data in enumerate(data['Data'], 1):
            coin_info = coin_data['CoinInfo']
            raw_data = coin_data.get('RAW', {}).get('USD', {})
            
            if raw_data:
                print(f"{i}. {coin_info['FullName']} ({coin_info['Name']})")
                print(f"   Price: ${raw_data.get('PRICE', 0):,.2f}")
                print(f"   24h Volume: ${raw_data.get('TOTALVOLUME24HTO', 0):,.0f}")
                print()

# ============================================================================
# Summary
# ============================================================================
print("="*70)
print("WHY CRYPTOCOMPARE IS PERFECT FOR YOUR ETL:")
print("="*70)
print("""
✅ Pros:
   • No API key required for basic use
   • Multiple endpoints (current prices, historical, top coins)
   • Multiple currencies (USD, EUR, IDR, etc.)
   • Historical data available
   • Great for Metabase dashboards
   • Good rate limits for learning

⚠️  Cons:
   • Free tier has limits (around 100,000 calls/month)
   • For production, you might need an API key

📚 Documentation: https://min-api.cryptocompare.com/documentation

🎯 Perfect for your Airflow ETL learning project!
""")

print("\n" + "="*70)
print("NEXT STEPS FOR YOUR ETL PROJECT:")
print("="*70)
print("""
1. Choose which data you want to collect:
   • Current prices? → Use pricemultifull
   • Historical data? → Use histoday/histohour
   • Top coins? → Use top/mktcapfull

2. Design your data model:
   • What fields do you need?
   • How often to collect? (every hour, daily?)
   • Where to store? (PostgreSQL, SQLite?)

3. Create Airflow DAG:
   • Extract: Call CryptoCompare API
   • Transform: Clean and structure data
   • Load: Insert into database

4. Connect Metabase:
   • Create dashboards
   • Build visualizations

Would you like help with any of these steps?
""")
