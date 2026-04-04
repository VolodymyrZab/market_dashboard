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
POLYGON_KEY = ENV.get("POLYGON_API_KEY")
FINNHUB_KEY = ENV.get("FINNHUB_API_KEY")

if not POLYGON_KEY: raise Exception("POLYGON_API_KEY not found")
if not FINNHUB_KEY: raise Exception("FINNHUB_API_KEY not found")

POLYGON_BASE = "https://api.polygon.io"
FINNHUB_BASE = "https://finnhub.io/api/v1"

# ─── Dates ───────────────────────────────────────────────────────
now_utc    = datetime.now(timezone.utc)
today      = now_utc.strftime("%Y-%m-%d")
yesterday  = (now_utc - timedelta(days=1)).strftime("%Y-%m-%d")
week_ago   = (now_utc - timedelta(days=7)).strftime("%Y-%m-%d")
month_ago  = (now_utc - timedelta(days=31)).strftime("%Y-%m-%d")
year_ago   = (now_utc - timedelta(days=370)).strftime("%Y-%m-%d")  # 52 weeks + buffer
fetch_from = (now_utc - timedelta(days=40)).strftime("%Y-%m-%d")   # buffer for weekends

# ─── Fetch helpers ────────────────────────────────────────────────
def polygon(endpoint, params=None):
    params = params or {}
    params["apiKey"] = POLYGON_KEY
    r = requests.get(f"{POLYGON_BASE}{endpoint}", params=params, timeout=10)
    r.raise_for_status()
    time.sleep(13)
    return r.json()

def finnhub(endpoint, params=None):
    params = params or {}
    params["token"] = FINNHUB_KEY
    r = requests.get(f"{FINNHUB_BASE}{endpoint}", params=params, timeout=10)
    r.raise_for_status()
    time.sleep(1)
    return r.json()

def calc_rvol(bars):
    if len(bars) < 2: return None
    window = bars[-21:-1] if len(bars) >= 21 else bars[:-1]
    if not window: return None
    avg = sum(b["v"] for b in window) / len(window)
    return round(bars[-1]["v"] / avg, 2) if avg > 0 else None

def get_bars(ticker, from_date, asset="stock"):
    if asset == "crypto":
        endpoint = f"/v2/aggs/ticker/X:{ticker}/range/1/day/{from_date}/{today}"
    else:
        endpoint = f"/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{today}"
    params = {"sort": "asc", "limit": 400, "adjusted": "true"}
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
                item["timing_label"] = "bmo"
                earnings_raw.append(item)
            elif report_date == yesterday and hour == "amc":
                item["timing_label"] = "amc"
                earnings_raw.append(item)
        print(f"  OK {date}: {len(items)} total earnings")
    except Exception as e:
        print(f"  WARN {date}: {e}")

earnings_tickers = list(dict.fromkeys(
    [e["symbol"] for e in earnings_raw if e.get("symbol")]
))[:20]

# ════════════════════════════════════════════════════════════════
# 2. EARNINGS HISTORY  (Finnhub)
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
# 3. COMPANY PROFILES for earnings tickers  (Finnhub)
# ════════════════════════════════════════════════════════════════
print(f"\nFetching company profiles...")
profiles = {}
for ticker in earnings_tickers:
    try:
        data = finnhub("/stock/profile2", {"symbol": ticker})
        mc = data.get("marketCapitalization")
        profiles[ticker] = {
            "name":       data.get("name", ticker),
            "market_cap": mc * 1e6 if mc else None,
            "industry":   data.get("finnhubIndustry", "")
        }
        print(f"  OK {ticker}: {profiles[ticker]['name']}")
    except Exception as e:
        print(f"  WARN {ticker}: {e}")
        profiles[ticker] = {"name": ticker, "market_cap": None, "industry": ""}

# ════════════════════════════════════════════════════════════════
# 4. 52-WEEK METRICS for earnings tickers  (Finnhub)
# ════════════════════════════════════════════════════════════════
print(f"\nFetching 52-week metrics for earnings tickers...")
e_metrics = {}
for ticker in earnings_tickers:
    try:
        data = finnhub("/stock/metric", {"symbol": ticker, "metric": "all"})
        m = data.get("metric", {})
        e_metrics[ticker] = {
            "week52_high": m.get("52WeekHigh"),
            "week52_low":  m.get("52WeekLow"),
            "pe_ratio":    m.get("peBasicExclExtraTTM"),
        }
        print(f"  OK {ticker}")
    except Exception as e:
        print(f"  WARN {ticker}: {e}")
        e_metrics[ticker] = {}

# ════════════════════════════════════════════════════════════════
# 5. PRICE + VOLUME for earnings tickers  (Polygon)
# ════════════════════════════════════════════════════════════════
print(f"\nFetching price/volume for earnings tickers...")
earnings_prices = {}
for ticker in earnings_tickers:
    try:
        bars = get_bars(ticker, fetch_from)
        if len(bars) >= 2:
            rvol = calc_rvol(bars)
            last = bars[-1]
            earnings_prices[ticker] = {
                "bars":        bars,
                "last_close":  last["c"],
                "prev_close":  bars[-2]["c"],
                "last_volume": last["v"],
                "rvol":        rvol
            }
            print(f"  OK {ticker}: close={last['c']}, RVOL={rvol}")
        else:
            print(f"  WARN {ticker}: not enough bars")
    except Exception as e:
        print(f"  WARN {ticker}: {e}")

# ════════════════════════════════════════════════════════════════
# 6. ETF DATA  (Polygon — 1 year of bars for all performance calcs)
# ════════════════════════════════════════════════════════════════
print("\nFetching ETF data...")

ETF_LIST = [
    ("XTL",  "SPDR S&P Telecom ETF"),
    ("ICLN", "iShares Clean Energy ETF"),
    ("MOO",  "VanEck Agribusiness ETF"),
    ("ERTH", "Invesco MSCI Sustainable Future ETF"),
    ("TAN",  "Invesco Solar ETF"),
    ("IGF",  "iShares Global Infrastructure ETF"),
    ("ITA",  "iShares U.S. Aerospace & Defense ETF"),
    ("USO",  "United States Oil Fund"),
    ("XLP",  "Consumer Staples Select Sector SPDR"),
    ("LIT",  "Global X Lithium & Battery Tech ETF"),
    ("ARKQ", "ARK Autonomous Technology ETF"),
    ("XLV",  "Health Care Select Sector SPDR"),
    ("FXI",  "iShares China Large-Cap ETF"),
    ("UFO",  "Procure Space ETF"),
    ("IBB",  "iShares Biotechnology ETF"),
    ("XLB",  "Materials Select Sector SPDR"),
    ("XLK",  "Technology Select Sector SPDR"),
    ("URA",  "Global X Uranium ETF"),
    ("IGV",  "iShares Expanded Tech-Software ETF"),
]

etfs = {}
for ticker, name in ETF_LIST:
    try:
        # Fetch 1 year of bars for week/month/52wk performance
        bars = get_bars(ticker, year_ago)
        if len(bars) < 2:
            print(f"  WARN {ticker}: not enough bars ({len(bars)})")
            continue

        last  = bars[-1]
        lc    = last["c"]

        # Find bar closest to 1 week ago
        def find_bar_near(target_date_str):
            for b in reversed(bars[:-1]):
                bar_date = datetime.fromtimestamp(b["t"]/1000, tz=timezone.utc).strftime("%Y-%m-%d")
                if bar_date <= target_date_str:
                    return b
            return bars[0]

        bar_1d  = bars[-2] if len(bars) >= 2 else None
        bar_1w  = find_bar_near(week_ago)
        bar_1m  = find_bar_near(month_ago)
        bar_52w = bars[0]  # oldest bar in 1yr window

        def perf(old_bar):
            if not old_bar or not old_bar.get("c"): return None
            return round((lc - old_bar["c"]) / old_bar["c"] * 100, 2)

        day_perf   = perf(bar_1d)
        week_perf  = perf(bar_1w)
        month_perf = perf(bar_1m)
        year_perf  = perf(bar_52w)

        # 52-week high/low from all bars in window
        highs  = [b["h"] for b in bars]
        lows   = [b["l"] for b in bars]
        w52h   = max(highs)
        w52l   = min(lows)
        # % above/below 52wk high (negative = below high)
        pct_vs_52h = round((lc - w52h) / w52h * 100, 2) if w52h else None
        # position in 52wk range (0% = at low, 100% = at high)
        w52_pos = round((lc - w52l) / (w52h - w52l) * 100, 1) if w52h != w52l else 50

        rvol = calc_rvol(bars[-30:] if len(bars) >= 30 else bars)

        etfs[ticker] = {
            "name":        name,
            "bars":        bars[-30:],   # last 30 days for chart
            "last_close":  lc,
            "volume":      last["v"],
            "rvol":        rvol,
            "day_perf":    day_perf,
            "week_perf":   week_perf,
            "month_perf":  month_perf,
            "year_perf":   year_perf,
            "week52_high": round(w52h, 2),
            "week52_low":  round(w52l, 2),
            "week52_pos":  w52_pos,
            "pct_vs_52h":  pct_vs_52h,
        }
        print(f"  OK {ticker}: {lc} | 1d={day_perf}% 1w={week_perf}% 1m={month_perf}%")
    except Exception as e:
        print(f"  WARN {ticker}: {e}")

# ════════════════════════════════════════════════════════════════
# 7. NEWS — top stories  (Finnhub)
# ════════════════════════════════════════════════════════════════
print("\nFetching top market news...")
news = []
try:
    data = finnhub("/news", {"category": "general", "minId": 0})
    # Filter for quality: prefer items with a summary and known source
    quality_sources = {"Reuters","Bloomberg","CNBC","MarketWatch","WSJ","Financial Times",
                       "Barron's","Seeking Alpha","Yahoo Finance","The Wall Street Journal",
                       "Forbes","Business Insider","AP","Associated Press","Investopedia"}
    scored = []
    for item in (data or []):
        score = 0
        src = item.get("source","")
        if any(qs.lower() in src.lower() for qs in quality_sources): score += 2
        if item.get("summary",""): score += 1
        if item.get("image",""): score += 1
        scored.append((score, item))
    scored.sort(key=lambda x: (-x[0], -x[1].get("datetime",0)))
    for _, item in scored[:12]:
        news.append({
            "headline": item.get("headline",""),
            "source":   item.get("source",""),
            "url":      item.get("url",""),
            "datetime": item.get("datetime",0),
            "summary":  item.get("summary","")[:220],
            "image":    item.get("image","")
        })
    print(f"  OK {len(news)} top stories")
except Exception as e:
    print(f"  WARN news: {e}")

# ════════════════════════════════════════════════════════════════
# 8. SENTIMENT  (SPY momentum from ETFs)
# ════════════════════════════════════════════════════════════════
print("\nCalculating sentiment...")
sentiment_score = 50
sentiment_label = "Neutral"
try:
    # Use XLK (tech) + XLV (health) + XLP (staples) as proxy
    proxy_bars = etfs.get("XLK", {}).get("bars", [])
    if len(proxy_bars) >= 20:
        prices    = [b["c"] for b in proxy_bars]
        momentum  = (prices[-1] - prices[-20]) / prices[-20] * 100
        ma5       = sum(prices[-5:])  / 5
        ma20      = sum(prices[-20:]) / 20
        trend     = (ma5 - ma20) / ma20 * 100
        recent    = proxy_bars[-10:]
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
# 9. BUILD EARNINGS OUTPUT
# ════════════════════════════════════════════════════════════════
earnings_output = []
for item in earnings_raw:
    ticker = item.get("symbol","")
    if not ticker: continue

    price_data = earnings_prices.get(ticker, {})
    history    = earnings_history.get(ticker, [])
    profile    = profiles.get(ticker, {})
    metric     = e_metrics.get(ticker, {})

    revenue_streak = []
    for q in history[:4]:
        act = q.get("actual")
        est = q.get("estimate")
        revenue_streak.append({
            "period":       q.get("period",""),
            "eps_actual":   act,
            "eps_estimate": est,
            "surprise_pct": q.get("surprisePercent"),
            "beat":         (act >= est) if (act is not None and est is not None) else None
        })

    rev_act  = item.get("revenueActual")
    rev_est  = item.get("revenueEstimate")
    rev_beat = (rev_act >= rev_est) if (rev_act is not None and rev_est is not None) else None
    if revenue_streak and rev_beat is not None:
        revenue_streak[0]["rev_beat"]     = rev_beat
        revenue_streak[0]["rev_actual"]   = rev_act
        revenue_streak[0]["rev_estimate"] = rev_est

    lc = price_data.get("last_close")
    pc = price_data.get("prev_close")
    gap_pct = round((lc - pc) / pc * 100, 2) if lc and pc else None

    w52h    = metric.get("week52_high")
    w52l    = metric.get("week52_low")
    w52_pos = None
    if lc and w52h and w52l and w52h != w52l:
        w52_pos = round((lc - w52l) / (w52h - w52l) * 100, 1)

    earnings_output.append({
        "symbol":           ticker,
        "name":             profile.get("name", ticker),
        "industry":         profile.get("industry",""),
        "timing":           item.get("timing_label",""),
        "eps_estimate":     item.get("epsEstimate"),
        "eps_actual":       item.get("epsActual"),
        "revenue_estimate": rev_est,
        "revenue_actual":   rev_act,
        "revenue_beat":     rev_beat,
        "market_cap":       profile.get("market_cap"),
        "last_close":       lc,
        "prev_close":       pc,
        "gap_pct":          gap_pct,
        "volume":           price_data.get("last_volume"),
        "rvol":             price_data.get("rvol"),
        "week52_high":      w52h,
        "week52_low":       w52l,
        "week52_pos":       w52_pos,
        "pe_ratio":         metric.get("pe_ratio"),
        "revenue_streak":   revenue_streak,
        "prev_quarters":    revenue_streak[:2],
        "bars":             price_data.get("bars",[])
    })

earnings_output.sort(key=lambda x: x.get("market_cap") or 0, reverse=True)

# ════════════════════════════════════════════════════════════════
# SAVE
# ════════════════════════════════════════════════════════════════
output = {
    "last_updated": today,
    "earnings":     earnings_output,
    "etfs":         etfs,
    "news":         news,
    "sentiment":    {"score": sentiment_score, "label": sentiment_label}
}

os.makedirs("data", exist_ok=True)
with open("data/market_data.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\nDone! {len(earnings_output)} earnings · {len(etfs)} ETFs · {len(news)} news · sentiment={sentiment_label}")
print("Saved → data/market_data.json")
