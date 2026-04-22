"""One-time initialization: backfill h250/l250 (250-day high/low) into History Gist.
Reads from pre_indicators.pkl on Windows. Run once, then daily_scan maintains it.

Usage (Windows PowerShell):
  cd C:\stock-evolution
  python init_h250.py

Also cleans up per-stock 'dates' arrays (moved to top-level, saves ~2 MB).
"""
import os, json, pickle, requests
import numpy as np

# Config
HISTORY_GIST = os.environ.get("HISTORY_GIST_ID", "572d4ca53b0bfbd37dd5485becdcce49")
TOKEN = os.environ.get("GITHUB_TOKEN_GIST", "")
HEADERS = {"Authorization": f"token {TOKEN}"}
PRE_PKL = os.path.join(os.path.expanduser("~"), "stock-evolution", "pre_indicators.pkl")

print("=" * 60)
print("  init_h250: Backfill 250-day high/low into History Gist")
print("=" * 60)

# 1. Load pre_indicators.pkl
if not os.path.exists(PRE_PKL):
    # Try alternative path
    PRE_PKL = r"C:\stock-evolution\pre_indicators.pkl"
if not os.path.exists(PRE_PKL):
    print(f"  ERROR: {PRE_PKL} not found!")
    exit(1)

pre = pickle.load(open(PRE_PKL, "rb"))
tickers = pre["tickers"]
n_stocks, n_days = pre["n_stocks"], pre["n_days"]
high_all = pre.get("high", pre.get("open", pre["close"]))  # full high array [ns, nd]
low_all = pre.get("low", pre.get("open", pre["close"]))    # full low array [ns, nd]
print(f"  pre_indicators: {n_stocks} stocks x {n_days} days")
print(f"  tickers sample: {tickers[:5]}")

# Build ticker -> (last 250 highs, last 250 lows) map
tk_map = {}
for si, tk in enumerate(tickers):
    h_arr = high_all[si, :]
    l_arr = low_all[si, :]
    # Find last valid index (non-zero, non-nan)
    valid = np.where(~np.isnan(h_arr) & (h_arr > 0))[0]
    if len(valid) < 20:
        continue
    last_idx = valid[-1]
    start = max(0, last_idx - 249)
    h250 = [round(float(x), 2) for x in h_arr[start:last_idx + 1] if not np.isnan(x) and x > 0]
    l250 = [round(float(x), 2) for x in l_arr[start:last_idx + 1] if not np.isnan(x) and x > 0]
    if h250 and l250:
        tk_map[tk] = {"h250": h250, "l250": l250}
print(f"  Prepared h250/l250 for {len(tk_map)} stocks")

# 2. Read current History Gist
print("  Reading History Gist...")
r = requests.get(f"https://api.github.com/gists/{HISTORY_GIST}", headers=HEADERS, timeout=15)
gist_data = r.json()
fdata = list(gist_data["files"].values())[0]
if fdata.get("truncated"):
    raw = requests.get(fdata["raw_url"], headers=HEADERS, timeout=120)
    history = json.loads(raw.text)
else:
    history = json.loads(fdata["content"])

stocks = history.get("stocks", {})
print(f"  History Gist: {len(stocks)} stocks, updated={history.get('updated', '?')}")

# 3. Backfill h250/l250 + cleanup per-stock dates
updated = 0
dates_cleaned = 0
top_level_dates = None

for tk, cs in stocks.items():
    # Backfill h250/l250 from pre_indicators
    if tk in tk_map:
        cs["h250"] = tk_map[tk]["h250"]
        cs["l250"] = tk_map[tk]["l250"]
        updated += 1
    else:
        # Stock not in pre_indicators → use existing h/l as h250/l250 (80 days, better than nothing)
        if cs.get("h") and not cs.get("h250"):
            cs["h250"] = list(cs["h"])
            cs["l250"] = list(cs["l"])

    # Cleanup per-stock dates → move to top-level
    if cs.get("dates") and top_level_dates is None:
        top_level_dates = cs["dates"]  # grab one copy
    if "dates" in cs:
        del cs["dates"]
        dates_cleaned += 1

# Set top-level dates
if top_level_dates:
    history["dates"] = top_level_dates
    print(f"  Moved dates to top-level ({len(top_level_dates)} entries)")

print(f"  Backfilled: {updated} stocks from pre_indicators")
print(f"  Cleaned dates: {dates_cleaned} stocks")

# 4. Check size before pushing
content = json.dumps(history, ensure_ascii=False)
size_mb = len(content) / 1024 / 1024
print(f"  New Gist size: {size_mb:.1f} MB (limit: 10 MB)")

if size_mb > 9.5:
    print(f"  WARNING: Size is close to limit! Consider reducing data.")
    print(f"  Aborting. Check the data and retry.")
    exit(1)

# 5. Push to Gist
print("  Pushing to History Gist...")
fname = list(gist_data["files"].keys())[0]
payload = {"files": {fname: {"content": content}}}
r = requests.patch(f"https://api.github.com/gists/{HISTORY_GIST}", headers=HEADERS,
                   json=payload, timeout=120)
if r.status_code == 200:
    print(f"  SUCCESS! History Gist updated.")
else:
    print(f"  FAILED: {r.status_code} {r.text[:200]}")
    exit(1)

# 6. Verify
print("\n  Verification:")
sample_tks = [tk for tk in ["3645.TW", "6213.TW", "2454.TW", "3031.TW", "2330.TW"] if tk in stocks]
for tk in sample_tks:
    cs = stocks[tk]
    h250 = cs.get("h250", [])
    l250 = cs.get("l250", [])
    if h250 and l250:
        w52_high = max(h250[-250:])
        w52_low = min(l250[-250:])
        last_c = cs["c"][-1] if cs.get("c") else 0
        w52_pos = (last_c - w52_low) / (w52_high - w52_low) if w52_high > w52_low else 0.5
        print(f"  {tk}: h250={len(h250)}d, 52w high={w52_high}, low={w52_low}, pos={w52_pos:.3f}")
    else:
        print(f"  {tk}: no h250 data")

print(f"\n  Has top-level dates: {'dates' in history} ({len(history.get('dates',[]))} entries)")
print("=" * 60)
