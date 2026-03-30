import requests
import json
import os
import time
from datetime import datetime, timezone, timedelta

# ─── Load API keys ────────────────────────────────────────────────
def load_env():
    env = {}
    # First try environment variables (GitHub Actions)
    for key in ["POLYGON_API_KEY", "FINNHUB_API_KEY"]:
        val = os.environ.get(key)
        if val:
            env[key] = val
    # Fall back to .env file
    try:
        with open(".env") as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    if k not in env:
                        env[k] = v
    except FileNotFoundError:
        pass
    return env

ENV = load_env()
POLYGON_KEY = ENV.get("POLYGON_API_KEY")
FINNHUB_KEY = ENV.get("FINNHUB_API_KEY")

if not POLYGON_KEY:
    raise Exception("POLYGON_API_KEY not found")
if not FINNHUB_KEY:
    raise Exception("FINNHUB_API_KEY not found")

POLYGON_BASE = "https://api.polygon.io"
FINNHUB_BASE = "https://finnhub.io/api/v1"

# ─── Date helpers ─────────────────────────────────────────────────
now_utc = datetime.now(timezone.utc)
today = now_utc.strftime("%Y-%m-%d")
yesterday = (now_utc - timedelta(days=1)).strftime("%Y-%m-%d")
month_ago = (now_utc - timedelta(days=30)).strftime("%Y-%m-%d")
year_ago = (now_utc - timedelta(days=365)).strftime("%Y-%m-%d")

# ─── Fetch helpers ────────────────────────────────────────────────
def polygon_get(endpoint, params=None):
    params = params or {}
    params["apiKey"] = POLYGON_KEY
    r = requests.get(f"{POLYGON_BASE}{endpoint}", params=params, timeout=10)
    r.raise_for_status()
    time.sleep(13)  # free tier: 5 req/min
    return r.json()

def finnhub_get(endpoint, params=None):
    params = params or {}
    params["token"] = FINNHUB_KEY
    r = requests.get(f"{FINNHUB_BASE}{endpoint}", params=params, timeout=10)
    r.raise_for_status()
    time.sleep(1)  # free tier: 60 req/min
    return r.json()

# ─── 1. EARNINGS CALENDAR ─────────────────────────────────────────
print("\nFetching earnings calendar...")
earnings_raw = []

# Today + yesterday window to catch BMO today and AMC yesterday
for date in [today, yesterday]:
    try:
        data = finnhub_get("/calendar/earnings", {
            "from": date,
            "to": date
        })
        items = data.get("earningsCalendar", [])
        for item in items:
            # Only keep BMO today and AMC yesterday
            hour = item.get("hour", "")
            report_date = item.get("date", "")
            if report_date == today and hour in ["bmo", "dmh"]:  # before market open
                item["timing_label"] = "Today BMO"
                earnings_raw.append(item)
            elif report_date == yesterday and hour == "amc":  # after market close
                item["timing_label"] = "Yesterday AMC"
                earnings_raw.append(item)
        print(f"  OK {date}: {len(items)} earnings found")
    except Exception as e:
        print(f"  WARN earnings {date}: {e}")

# ─── 2. EARNINGS SURPRISES (last 4 quarters per ticker) ──────────
earnings_tickers = [e["symbol"] for e in earnings_raw if e.get("symbol")]
earnings_tickers = list(dict.fromkeys(earnings_tickers))[:20]  # dedupe, max 20

print(f"\nFetching earnings details for {len(earnings_tickers)} tickers...")
earnings_details = {}
for ticker in earnings_tickers:
    try:
        data = finnhub_get("/stock/earnings", {"symbol": ticker, "limit": 4})
        earnings_details[ticker] = data if isinstance(data, list) else []
        print(f"  OK {ticker}: {len(earnings_details[ticker])} quarters")
    except Exception as e:
        print(f"  WARN {ticker}: {e}")
        earnings_details[ticker] = []

# ─── 3. PRICE + VOLUME + RVOL via Polygon ────────────────────────
print(f"\nFetching price/volume data for earnings tickers...")
earnings_prices = {}
for ticker in earnings_tickers:
    try:
        data = polygon_get(
            f"/v2/aggs/ticker/{ticker}/range/1/day/{month_ago}/{today}",
            {"adjusted": "true", "sort": "asc", "limit": 30}
        )
        bars = data.get("results", [])
        if len(bars) >= 2:
            # Calculate 20-day average volume for RVOL
            recent_bars = bars[-21:-1] if len(bars) >= 21 else bars[:-1]
            avg_vol = sum(b["v"] for b in recent_bars) / len(recent_bars) if recent_bars else 0
            last = bars[-1]
            rvol = round(last["v"] / avg_vol, 2) if avg_vol > 0 else None
            earnings_prices[ticker] = {
                "bars": bars,
                "last_close": last["c"],
                "last_volume": last["v"],
                "avg_volume": round(avg_vol),
                "rvol": rvol,
                "prev_close": bars[-2]["c"] if len(bars) >= 2 else None
            }
            print(f"  OK {ticker}: close={last['c']}, RVOL={rvol}")
        else:
            print(f"  WARN {ticker}: not enough bars")
    except Exception as e:
        print(f"  WARN {ticker} price: {e}")

# ─── 4. REGULAR STOCKS (existing dashboard) ──────────────────────
print("\nFetching regular stocks...")
STOCKS = ["AAPL", "MSFT", "GOOGL", "NVDA", "AMZN"]
stocks = {}
for ticker in STOCKS:
    try:
        data = polygon_get(
            f"/v2/aggs/ticker/{ticker}/range/1/day/{month_ago}/{today}",
            {"adjusted": "true", "sort": "asc"}
        )
        bars = data.get("results", [])
        recent = bars[-21:-1] if len(bars) >= 21 else bars[:-1]
        avg_vol = sum(b["v"] for b in recent) / len(recent) if recent else 0
        stocks[ticker] = {
            "bars": bars,
            "rvol": round(bars[-1]["v"] / avg_vol, 2) if avg_vol > 0 and bars else None
        }
        print(f"  OK {ticker}: {len(bars)} days")
    except Exception as e:
        print(f"  WARN {ticker}: {e}")

# ─── 5. INDICES ───────────────────────────────────────────────────
print("\nFetching indices...")
indices = {}
for name, ticker in {"SP500": "SPY", "NASDAQ": "QQQ", "DOW": "DIA"}.items():
    try:
        data = polygon_get(
            f"/v2/aggs/ticker/{ticker}/range/1/day/{month_ago}/{today}",
            {"adjusted": "true", "sort": "asc"}
        )
        indices[name] = {"bars": data.get("results", [])}
        print(f"  OK {name}")
    except Exception as e:
        print(f"  WARN {name}: {e}")

# ─── 6. CRYPTO ────────────────────────────────────────────────────
print("\nFetching crypto...")
crypto = {}
for pair in ["BTCUSD", "ETHUSD", "SOLUSD"]:
    try:
        data = polygon_get(
            f"/v2/aggs/ticker/X:{pair}/range/1/day/{month_ago}/{today}",
            {"sort": "asc"}
        )
        crypto[pair] = {"bars": data.get("results", [])}
        print(f"  OK {pair}")
    except Exception as e:
        print(f"  WARN {pair}: {e}")

# ─── 7. MARKET NEWS ───────────────────────────────────────────────
print("\nFetching market news...")
news = []
try:
    data = finnhub_get("/news", {"category": "general", "minId": 0})
    for item in (data or [])[:15]:
        news.append({
            "headline": item.get("headline", ""),
            "source": item.get("source", ""),
            "url": item.get("url", ""),
            "datetime": item.get("datetime", 0),
            "summary": item.get("summary", "")[:200]
        })
    print(f"  OK {len(news)} news items")
except Exception as e:
    print(f"  WARN news: {e}")

# ─── 8. FEAR & GREED (calculated from VIX proxy + momentum) ──────
print("\nCalculating Fear & Greed sentiment...")
sentiment_score = 50
sentiment_label = "Neutral"
try:
    # Use SPY 30-day momentum as sentiment proxy
    spy_bars = indices.get("SP500", {}).get("bars", [])
    if len(spy_bars) >= 20:
        prices = [b["c"] for b in spy_bars]
        # 20-day momentum
        momentum = (prices[-1] - prices[-20]) / prices[-20] * 100
        # 5-day vs 20-day trend
        ma5 = sum(prices[-5:]) / 5
        ma20 = sum(prices[-20:]) / 20
        trend = (ma5 - ma20) / ma20 * 100
        # Volume trend (high volume on up days = greed)
        recent_bars = spy_bars[-10:]
        up_vol = sum(b["v"] for b in recent_bars if b["c"] > b["o"])
        down_vol = sum(b["v"] for b in recent_bars if b["c"] <= b["o"])
        vol_ratio = up_vol / (up_vol + down_vol) * 100 if (up_vol + down_vol) > 0 else 50

        # Weighted score
        raw = (momentum * 3) + (trend * 5) + (vol_ratio - 50)
        sentiment_score = max(0, min(100, 50 + raw))
        sentiment_score = round(sentiment_score)

        if sentiment_score >= 75:
            sentiment_label = "Extreme Greed"
        elif sentiment_score >= 60:
            sentiment_label = "Greed"
        elif sentiment_score >= 45:
            sentiment_label = "Neutral"
        elif sentiment_score >= 25:
            sentiment_label = "Fear"
        else:
            sentiment_label = "Extreme Fear"

    print(f"  OK score={sentiment_score} ({sentiment_label})")
except Exception as e:
    print(f"  WARN sentiment: {e}")

# ─── 9. BUILD FULL EARNINGS LIST ─────────────────────────────────
earnings_output = []
for item in earnings_raw:
    ticker = item.get("symbol", "")
    if not ticker:
        continue
    price_data = earnings_prices.get(ticker, {})
    surprise_data = earnings_details.get(ticker, [])
    latest = surprise_data[0] if surprise_data else {}

    earnings_output.append({
        "symbol": ticker,
        "timing": item.get("timing_label", ""),
        "eps_estimate": item.get("epsEstimate"),
        "eps_actual": item.get("epsActual"),
        "revenue_estimate": item.get("revenueEstimate"),
        "revenue_actual": item.get("revenueActual"),
        "surprise_pct": latest.get("surprisePercent"),
        "last_close": price_data.get("last_close"),
        "prev_close": price_data.get("prev_close"),
        "volume": price_data.get("last_volume"),
        "avg_volume": price_data.get("avg_volume"),
        "rvol": price_data.get("rvol"),
        "bars": price_data.get("bars", [])
    })

# ─── 10. SAVE OUTPUT ─────────────────────────────────────────────
output = {
    "last_updated": today,
    "earnings": earnings_output,
    "stocks": stocks,
    "indices": indices,
    "crypto": crypto,
    "news": news,
    "sentiment": {
        "score": sentiment_score,
        "label": sentiment_label
    }
}

os.makedirs("data", exist_ok=True)
with open("data/market_data.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\nDone! Saved {len(earnings_output)} earnings, {len(news)} news, sentiment={sentiment_label}")
print("Data saved to data/market_data.json")
