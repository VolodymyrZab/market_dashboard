import requests
import json
import os
import time
from datetime import datetime, timezone, timedelta

# --- Load API key from .env file ---
def load_api_key():
    # First try environment variable (GitHub Actions)
    key = os.environ.get("POLYGON_API_KEY")
    if key:
        return key
    # Fall back to .env file (local development)
    try:
        with open(".env") as f:
            for line in f:
                if line.startswith("POLYGON_API_KEY"):
                    return line.strip().split("=")[1]
    except FileNotFoundError:
        pass
    raise Exception("API key not found")
```

---

### Step 4 — Push the new files to GitHub
```
git add .
```
```
git commit -m "add GitHub Actions workflow"
```
```
git push

API_KEY = load_api_key()
BASE_URL = "https://api.polygon.io"

# --- Date helpers ---
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
month_ago = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

# --- Fetch function (with rate limit pause) ---
def fetch(endpoint, params=None):
    params = params or {}
    params["apiKey"] = API_KEY
    response = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=10)
    response.raise_for_status()
    time.sleep(13)  # stay safely under 5 requests/minute
    return response.json()

# --- 1. US Stocks (last 30 days) ---
print("\n📈 Fetching US Stocks...")
stocks = {}
for ticker in ["AAPL", "MSFT", "GOOGL", "NVDA", "AMZN"]:
    data = fetch(f"/v2/aggs/ticker/{ticker}/range/1/day/{month_ago}/{today}",
                 {"adjusted": "true", "sort": "asc"})
    stocks[ticker] = data.get("results", [])
    print(f"  ✓ {ticker}: {len(stocks[ticker])} days of data")

# --- 2. Market Indices (via ETF proxies) ---
print("\n📊 Fetching Market Indices...")
indices = {}
for name, ticker in {"SP500": "SPY", "NASDAQ": "QQQ", "DOW": "DIA"}.items():
    data = fetch(f"/v2/aggs/ticker/{ticker}/range/1/day/{month_ago}/{today}",
                 {"adjusted": "true", "sort": "asc"})
    indices[name] = data.get("results", [])
    print(f"  ✓ {name} ({ticker}): {len(indices[name])} days of data")

# --- 3. Crypto (last 30 days) ---
print("\n🪙 Fetching Crypto...")
crypto = {}
for pair in ["BTCUSD", "ETHUSD", "SOLUSD"]:
    data = fetch(f"/v2/aggs/ticker/X:{pair}/range/1/day/{month_ago}/{today}",
                 {"sort": "asc"})
    crypto[pair] = data.get("results", [])
    print(f"  ✓ {pair}: {len(crypto[pair])} days of data")

# --- Save everything to a JSON file ---
output = {
    "last_updated": today,
    "stocks": stocks,
    "indices": indices,
    "crypto": crypto
}

os.makedirs("data", exist_ok=True)
with open("data/market_data.json", "w") as f:
    json.dump(output, f, indent=2)

print("\n✅ Done! Data saved to data/market_data.json")