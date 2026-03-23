import requests
import json
import os
import time
from datetime import datetime, timezone, timedelta

def load_api_key():
    key = os.environ.get("POLYGON_API_KEY")
    if key:
        return key
    try:
        with open(".env") as f:
            for line in f:
                if line.startswith("POLYGON_API_KEY"):
                    return line.strip().split("=")[1]
    except FileNotFoundError:
        pass
    raise Exception("API key not found")

API_KEY = load_api_key()
BASE_URL = "https://api.polygon.io"

today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
month_ago = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

def fetch(endpoint, params=None):
    params = params or {}
    params["apiKey"] = API_KEY
    response = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=10)
    response.raise_for_status()
    time.sleep(13)
    return response.json()

print("Fetching US Stocks...")
stocks = {}
for ticker in ["AAPL", "MSFT", "GOOGL", "NVDA", "AMZN"]:
    data = fetch(f"/v2/aggs/ticker/{ticker}/range/1/day/{month_ago}/{today}",
                 {"adjusted": "true", "sort": "asc"})
    stocks[ticker] = data.get("results", [])
    print(f"  OK {ticker}: {len(stocks[ticker])} days of data")

print("Fetching Market Indices...")
indices = {}
for name, ticker in {"SP500": "SPY", "NASDAQ": "QQQ", "DOW": "DIA"}.items():
    data = fetch(f"/v2/aggs/ticker/{ticker}/range/1/day/{month_ago}/{today}",
                 {"adjusted": "true", "sort": "asc"})
    indices[name] = data.get("results", [])
    print(f"  OK {name} ({ticker}): {len(indices[name])} days of data")

print("Fetching Crypto...")
crypto = {}
for pair in ["BTCUSD", "ETHUSD", "SOLUSD"]:
    data = fetch(f"/v2/aggs/ticker/X:{pair}/range/1/day/{month_ago}/{today}",
                 {"sort": "asc"})
    crypto[pair] = data.get("results", [])
    print(f"  OK {pair}: {len(crypto[pair])} days of data")

output = {
    "last_updated": today,
    "stocks": stocks,
    "indices": indices,
    "crypto": crypto
}

os.makedirs("data", exist_ok=True)
with open("data/market_data.json", "w") as f:
    json.dump(output, f, indent=2)

print("Done! Data saved to data/market_data.json")
