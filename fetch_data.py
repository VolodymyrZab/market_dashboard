import requests
import json
import os
import time
from datetime import datetime, timezone, timedelta

# ─── Load API keys ────────────────────────────────────────────────
def load_env():
    env = {}
    for key in ["POLYGON_API_KEY", "FINNHUB_API_KEY"]:
        val = os.environ.get(key)
        if val:
            env[key] = val
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
POLYGON_KEY  = ENV.get("POLYGON_API_KEY")
FINNHUB_KEY  = ENV.get("FINNHUB_API_KEY")

if not POLYGON_KEY:  raise Exception("POLYGON_API_KEY not found")
if not FINNHUB_KEY:  raise Exception("FINNHUB_API_KEY not found")

POLYGON_BASE = "https://api.polygon.io"
FINNHUB_BASE = "https://finnhub.io/api/v1"

# ─── Dates ───────────────────────────────────────────────────────
now_utc   = datetime.now(timezone.utc)
today     = now_utc.strftime("%Y-%m-%d")
yesterday = (now_utc - timedelta(days=1)).strftime("%Y-%m-%d")
month_ago = (now_utc - timedelta(days=40)).strftime("%Y-%m-%d")  # 40d buffer for weekends

# ─── Fetch helpers ────────────────────────────────────────────────
def polygon(endpoint, params=None):
    params = params or {}
    params["apiKey"] = POLYGON_KEY
    r = requests.get(f"{POLYGON_BASE}{endpoint}", params=params, timeout=10)
    r.raise_for_status()
    time.sleep(13)   # free tier: 5 req/min
    return r.json()

def finnhub(endpoint, params=None):
    params = params or {}
    params["token"] = FINNHUB_KEY
    r = requests.get(f"{FINNHUB_BASE}{endpoint}", params=params, timeout=10)
    r.raise_for_status()
    time.sleep(1)    # free tier: 60 req/min
    return r.json()

# ─── RVOL helper ─────────────────────────────────────────────────
def calc_rvol(bars):
    if len(bars) < 2:
        return None
    window = bars[-21:-1] if len(bars) >= 21 else bars[:-1]
    if not window:
        return None
    avg = sum(b["v"] for b in window) / len(window)
    return round(bars[-1]["v"] / avg, 2) if avg > 0 else None

# ─── Polygon bars helper ──────────────────────────────────────────
def get_bars(ticker, asset="stock"):
    if asset == "crypto":
        endpoint = f"/v2/aggs/ticker/X:{ticker}/range/1/day/{month_ago}/{today}"
    else:
        endpoint = f"/v2/aggs/ticker/{ticker}/range/1/day/{month_ago}/{today}"
    params = {"sort": "asc", "limit": 50}
    if asset == "stock":
        params["adjusted"] = "true"
    data = polygon(endpoint, params)
    return data.get("results", [])

# ════════════════════════════════════════════════════════════════
# 1. EARNINGS CALENDAR  (Finnhub)
# ════════════════════════════════════════════════════════════════
print("\nFetching earnings calendar...")
earnings_raw = []
for date in [today, yesterday]:
    try:
        data = finnhub("/calendar/earnings", {"from": date, "to": date})
        items = data.get("earningsCalendar", [])
        for item in items:
            hour = item.get("hour", "")
            report_date = item.get("date", "")
            if report_date == today and hour in ["bmo", "dmh"]:
                item["timing_label"] = "Today BMO"
                earnings_raw.append(item)
            elif report_date == yesterday and hour == "amc":
                item["timing_label"] = "Yesterday AMC"
                earnings_raw.append(item)
        print(f"  OK {date}: {len(items)} total earnings")
    except Exception as e:
        print(f"  WARN {date}: {e}")

earnings_tickers = list(dict.fromkeys(
    [e["symbol"] for e in earnings_raw if e.get("symbol")]
))[:20]

# ════════════════════════════════════════════════════════════════
# 2. EARNINGS HISTORY — last 4 quarters  (Finnhub)
# ════════════════════════════════════════════════════════════════
print(f"\nFetching earnings history for {len(earnings_tickers)} tickers...")
earnings_history = {}
for ticker in earnings_tickers:
    try:
        data = finnhub("/stock/earnings", {"symbol": ticker, "limit": 4})
        earnings_history[ticker] = data if isinstance(data, list) else []
        print(f"  OK {ticker}: {len(earnings_history[ticker])} quarters")
    except Exception as e:
        print(f"  WARN {ticker}: {e}")
        earnings_history[ticker] = []

# ════════════════════════════════════════════════════════════════
# 3. MARKET CAP  (Finnhub)
# ════════════════════════════════════════════════════════════════
print(f"\nFetching market cap for earnings tickers...")
market_caps = {}
for ticker in earnings_tickers:
    try:
        data = finnhub("/stock/profile2", {"symbol": ticker})
        mc = data.get("marketCapitalization")
        market_caps[ticker] = mc * 1e6 if mc else None
        print(f"  OK {ticker}: {mc}M")
    except Exception as e:
        print(f"  WARN {ticker}: {e}")
        market_caps[ticker] = None

# ════════════════════════════════════════════════════════════════
# 4. PRICE + VOLUME for earnings tickers  (Polygon)
# ════════════════════════════════════════════════════════════════
print(f"\nFetching price/volume for earnings tickers...")
earnings_prices = {}
for ticker in earnings_tickers:
    try:
        bars = get_bars(ticker, asset="stock")
        if len(bars) >= 2:
            rvol = calc_rvol(bars)
            last = bars[-1]
            earnings_prices[ticker] = {
                "bars":       bars,
                "last_close": last["c"],
                "prev_close": bars[-2]["c"],
                "last_volume": last["v"],
                "rvol":       rvol
            }
            print(f"  OK {ticker}: close={last['c']}, RVOL={rvol}")
        else:
            print(f"  WARN {ticker}: not enough bars ({len(bars)})")
    except Exception as e:
        print(f"  WARN {ticker}: {e}")

# ════════════════════════════════════════════════════════════════
# 5. REGULAR STOCKS  (Polygon)
# ════════════════════════════════════════════════════════════════
print("\nFetching regular stocks...")
STOCKS = ["AAPL", "MSFT", "GOOGL", "NVDA", "AMZN"]
stocks = {}
for ticker in STOCKS:
    try:
        bars = get_bars(ticker, asset="stock")
        stocks[ticker] = {
            "bars": bars,
            "rvol": calc_rvol(bars)
        }
        print(f"  OK {ticker}: {len(bars)} bars")
    except Exception as e:
        print(f"  WARN {ticker}: {e}")

# ════════════════════════════════════════════════════════════════
# 6. INDICES via ETF proxies  (Polygon)
# ════════════════════════════════════════════════════════════════
print("\nFetching indices...")
indices = {}
for name, ticker in {"SP500": "SPY", "NASDAQ": "QQQ", "DOW": "DIA"}.items():
    try:
        bars = get_bars(ticker, asset="stock")
        indices[name] = {"bars": bars}
        print(f"  OK {name}: {len(bars)} bars")
    except Exception as e:
        print(f"  WARN {name}: {e}")

# ════════════════════════════════════════════════════════════════
# 7. CRYPTO  (Polygon)
# ════════════════════════════════════════════════════════════════
print("\nFetching crypto...")
crypto = {}
for pair in ["BTCUSD", "ETHUSD", "SOLUSD"]:
    try:
        bars = get_bars(pair, asset="crypto")
        crypto[pair] = {"bars": bars}
        print(f"  OK {pair}: {len(bars)} bars")
    except Exception as e:
        print(f"  WARN {pair}: {e}")

# ════════════════════════════════════════════════════════════════
# 8. NEWS  (Finnhub)
# ════════════════════════════════════════════════════════════════
print("\nFetching news...")
news = []
try:
    data = finnhub("/news", {"category": "general", "minId": 0})
    for item in (data or [])[:15]:
        news.append({
            "headline": item.get("headline", ""),
            "source":   item.get("source", ""),
            "url":      item.get("url", ""),
            "datetime": item.get("datetime", 0),
            "summary":  item.get("summary", "")[:200]
        })
    print(f"  OK {len(news)} items")
except Exception as e:
    print(f"  WARN news: {e}")

# ════════════════════════════════════════════════════════════════
# 9. SENTIMENT from SPY momentum  (calculated)
# ════════════════════════════════════════════════════════════════
print("\nCalculating sentiment...")
sentiment_score = 50
sentiment_label = "Neutral"
try:
    spy_bars = indices.get("SP500", {}).get("bars", [])
    if len(spy_bars) >= 20:
        prices    = [b["c"] for b in spy_bars]
        momentum  = (prices[-1] - prices[-20]) / prices[-20] * 100
        ma5       = sum(prices[-5:])  / 5
        ma20      = sum(prices[-20:]) / 20
        trend     = (ma5 - ma20) / ma20 * 100
        recent    = spy_bars[-10:]
        up_vol    = sum(b["v"] for b in recent if b["c"] > b["o"])
        down_vol  = sum(b["v"] for b in recent if b["c"] <= b["o"])
        vol_ratio = up_vol / (up_vol + down_vol) * 100 if (up_vol + down_vol) > 0 else 50
        raw       = (momentum * 3) + (trend * 5) + (vol_ratio - 50)
        sentiment_score = max(0, min(100, round(50 + raw)))
        if   sentiment_score >= 75: sentiment_label = "Extreme Greed"
        elif sentiment_score >= 60: sentiment_label = "Greed"
        elif sentiment_score >= 45: sentiment_label = "Neutral"
        elif sentiment_score >= 25: sentiment_label = "Fear"
        else:                       sentiment_label = "Extreme Fear"
    print(f"  OK score={sentiment_score} ({sentiment_label})")
except Exception as e:
    print(f"  WARN sentiment: {e}")

# ════════════════════════════════════════════════════════════════
# 10. BUILD EARNINGS OUTPUT
# ════════════════════════════════════════════════════════════════
earnings_output = []
for item in earnings_raw:
    ticker = item.get("symbol", "")
    if not ticker:
        continue
    price_data = earnings_prices.get(ticker, {})
    history    = earnings_history.get(ticker, [])

    prev_quarters = []
    for q in history[:2]:
        prev_quarters.append({
            "period":       q.get("period", ""),
            "eps_actual":   q.get("actual"),
            "eps_estimate": q.get("estimate"),
            "surprise_pct": q.get("surprisePercent"),
        })

    lc = price_data.get("last_close")
    pc = price_data.get("prev_close")
    price_chg = round((lc - pc) / pc * 100, 2) if lc and pc else None

    earnings_output.append({
        "symbol":           ticker,
        "timing":           item.get("timing_label", ""),
        "eps_estimate":     item.get("epsEstimate"),
        "eps_actual":       item.get("epsActual"),
        "revenue_estimate": item.get("revenueEstimate"),
        "revenue_actual":   item.get("revenueActual"),
        "market_cap":       market_caps.get(ticker),
        "last_close":       lc,
        "prev_close":       pc,
        "price_change_pct": price_chg,
        "volume":           price_data.get("last_volume"),
        "rvol":             price_data.get("rvol"),
        "prev_quarters":    prev_quarters,
        "bars":             price_data.get("bars", [])
    })

# Sort by market cap descending by default
earnings_output.sort(key=lambda x: x.get("market_cap") or 0, reverse=True)

# ════════════════════════════════════════════════════════════════
# SAVE
# ════════════════════════════════════════════════════════════════
output = {
    "last_updated": today,
    "earnings":     earnings_output,
    "stocks":       stocks,
    "indices":      indices,
    "crypto":       crypto,
    "news":         news,
    "sentiment":    {"score": sentiment_score, "label": sentiment_label}
}

os.makedirs("data", exist_ok=True)
with open("data/market_data.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\nDone! {len(earnings_output)} earnings · {len(news)} news · sentiment={sentiment_label}")
print("Saved → data/market_data.json")