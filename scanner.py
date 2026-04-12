"""
Live scanner for 龍蝦選股系統 Web App
Fetches TWSE/TPEx market data + yfinance history → computes indicators → scores
"""

import requests
import numpy as np
import pandas as pd
from datetime import datetime
import warnings
import urllib3

urllib3.disable_warnings()
warnings.filterwarnings("ignore")


def fetch_market_data():
    """Fetch all stocks from TWSE + TPEx official APIs (2 calls)."""
    all_data = {}
    today = datetime.now()
    date_ad = today.strftime("%Y%m%d")
    date_roc = f"{today.year - 1911}/{today.month:02d}/{today.day:02d}"
    trading_date = today.strftime("%Y-%m-%d")

    # TWSE
    try:
        r = requests.get(
            f"https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json&date={date_ad}",
            timeout=15, verify=False, headers={"User-Agent": "Mozilla/5.0"},
        )
        data = r.json()
        for row in data.get("data", []):
            try:
                code = row[0].strip()
                if code.startswith("00"):
                    continue
                vol = int(row[2].replace(",", ""))
                c = float(row[7].replace(",", ""))
                if vol > 0 and c > 0:
                    all_data[f"{code}.TW"] = {
                        "close": c, "vol": vol, "name": row[1].strip(),
                    }
            except Exception:
                continue
        # Extract actual trading date
        resp_date = data.get("date", "")
        if resp_date and len(resp_date) == 8:
            trading_date = f"{resp_date[:4]}-{resp_date[4:6]}-{resp_date[6:8]}"
    except Exception:
        pass

    # TPEx
    try:
        r = requests.get(
            "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php",
            params={"l": "zh-tw", "d": date_roc},
            headers={"User-Agent": "Mozilla/5.0"}, timeout=15, verify=False,
        )
        for t in r.json().get("tables", []):
            for row in t.get("data", []):
                try:
                    code = row[0].strip()
                    if not code or code.startswith("00"):
                        continue
                    vol = int(row[8].replace(",", "")) if row[8].replace(",", "").isdigit() else 0
                    c = float(row[2].replace(",", "")) if row[2].replace(",", "").replace(".", "").isdigit() else 0
                    if vol > 0 and c > 0:
                        all_data[f"{code}.TWO"] = {
                            "close": c, "vol": vol, "name": row[1].strip(),
                        }
                except Exception:
                    continue
    except Exception:
        pass

    return all_data, trading_date


def compute_indicators(c, h, lo, vol):
    """Compute technical indicators (matching GPU algorithm)."""
    n = len(c)
    if n < 20:
        return None

    last = n - 1
    ind = {"price": float(c[last])}

    # RSI (Wilder)
    delta = np.diff(c)
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    if n >= 15:
        ag = np.mean(gain[:14])
        al = np.mean(loss[:14])
        for i in range(15, n):
            ag = (ag * 13 + gain[i - 1]) / 14
            al = (al * 13 + loss[i - 1]) / 14
        rs = ag / al if al > 0 else 100
        ind["rsi"] = float(100 - 100 / (1 + rs))
    else:
        ind["rsi"] = 50.0

    # MAs (excludes today)
    for w in [3, 5, 8, 10, 15, 20, 30, 60]:
        ind[f"ma{w}"] = float(np.mean(c[last - w : last])) if n > w else float(c[last])

    # Bollinger Band position
    bb_win = c[last - 20 : last] if n > 20 else (c[:last] if last > 0 else c)
    bb_mid = float(np.mean(bb_win))
    bb_std = float(np.std(bb_win))
    bb_range = 4 * bb_std
    if bb_range > 1e-6:
        ind["bb_pos"] = min(2.0, max(-0.5, float((c[last] - (bb_mid - 2 * bb_std)) / bb_range)))
    else:
        ind["bb_pos"] = 0.5

    # Volume ratio
    vol_avg = float(np.mean(vol[last - 20 : last])) if n > 20 else (float(np.mean(vol[:last])) if last > 0 else 1)
    ind["vol_ratio"] = float(vol[last] / vol_avg) if vol_avg > 0 else 1.0

    # Vol > yesterday
    if last >= 1 and n > 21:
        vap = float(np.mean(vol[last - 21 : last - 1]))
        vrp = float(vol[last - 1] / vap) if vap > 0 else 1
    else:
        vrp = 1.0
    ind["vol_gt_yesterday"] = 1 if ind["vol_ratio"] > vrp else 0

    # KD
    kv = np.zeros(n)
    dv = np.zeros(n)
    kv[0] = 50
    dv[0] = 50
    for i in range(1, n):
        lo9 = np.min(lo[max(0, i - 9) : i + 1])
        hi9 = np.max(h[max(0, i - 9) : i + 1])
        rsv = (c[i] - lo9) / (hi9 - lo9) * 100 if hi9 > lo9 else 50
        kv[i] = kv[i - 1] * 2 / 3 + rsv * 1 / 3
        dv[i] = dv[i - 1] * 2 / 3 + kv[i] * 1 / 3
    ind["k_val"] = float(kv[last])
    ind["kd_golden_cross"] = 1 if kv[last] > dv[last] and (last < 1 or kv[last - 1] <= dv[last - 1]) else 0

    # Williams %R
    if n >= 15:
        h14 = float(np.max(h[last - 14 : last + 1]))
        l14 = float(np.min(lo[last - 14 : last + 1]))
        ind["williams_r"] = float((h14 - c[last]) / (h14 - l14) * -100) if h14 > l14 else -50
    else:
        ind["williams_r"] = -50.0

    # Momentum
    for d in [3, 5, 10]:
        ind[f"momentum_{d}"] = float((c[last] / c[last - d] - 1) * 100) if last >= d else 0

    # Near high (21-day)
    h20 = float(np.max(h[last - 20 : last + 1])) if n >= 21 else float(np.max(h))
    ind["near_high"] = float((c[last] / h20 - 1) * 100) if h20 > 0 else 0

    # 60-day new high
    ind["new_high_60"] = 1 if n > 60 and c[last] > np.max(h[last - 60 : last]) else 0

    # Above MA60
    ind["above_ma60"] = 1 if c[last] >= ind.get("ma60", c[last]) else 0

    # ATR (Wilder)
    tr = np.zeros(n)
    for i in range(1, n):
        tr[i] = max(h[i] - lo[i], abs(h[i] - c[i - 1]), abs(lo[i] - c[i - 1]))
    atr = np.zeros(n)
    for i in range(1, n):
        atr[i] = np.mean(tr[1 : min(i + 1, 15)]) if i <= 14 else (atr[i - 1] * 13 + tr[i]) / 14
    ind["atr_pct"] = float(atr[last] / c[last] * 100) if c[last] > 0 else 0

    # Squeeze
    def _sq(idx):
        if idx < 20:
            return False
        w = c[idx - 20 : idx]
        m = np.mean(w)
        s = np.std(w)
        return (m + 2 * s) < (m + 1.5 * atr[idx]) and (m - 2 * s) > (m - 1.5 * atr[idx])

    sq_t = _sq(last) if n > 20 else False
    sq_y = _sq(last - 1) if last >= 1 and n > 21 else False
    # Need MACD hist for squeeze
    e12 = np.zeros(n)
    e26 = np.zeros(n)
    e12[0] = c[0]
    e26[0] = c[0]
    for i in range(1, n):
        e12[i] = e12[i - 1] * (1 - 2 / 13) + c[i] * 2 / 13
        e26[i] = e26[i - 1] * (1 - 2 / 27) + c[i] * 2 / 27
    ml = e12 - e26
    ms = np.zeros(n)
    ms[0] = ml[0]
    for i in range(1, n):
        ms[i] = ms[i - 1] * (1 - 2 / 10) + ml[i] * 2 / 10
    mh = ml - ms
    ind["squeeze_fire"] = 1 if sq_y and not sq_t and mh[last] > 0 else 0

    # ADX
    if n >= 29:
        pdm = np.zeros(n)
        mdm = np.zeros(n)
        for i in range(1, n):
            up = h[i] - h[i - 1]
            dn = lo[i - 1] - lo[i]
            pdm[i] = up if up > dn and up > 0 else 0
            mdm[i] = dn if dn > up and dn > 0 else 0
        a14 = np.mean(tr[1:15])
        sp = np.mean(pdm[1:15])
        sm = np.mean(mdm[1:15])
        dx = np.zeros(n)
        for i in range(14, n):
            if i > 14:
                a14 = (a14 * 13 + tr[i]) / 14
                sp = (sp * 13 + pdm[i]) / 14
                sm = (sm * 13 + mdm[i]) / 14
            pdi = sp / a14 * 100 if a14 > 0 else 0
            mdi = sm / a14 * 100 if a14 > 0 else 0
            dx[i] = abs(pdi - mdi) / (pdi + mdi) * 100 if pdi + mdi > 0 else 0
        adx_v = np.mean(dx[14:29])
        for i in range(29, n):
            adx_v = (adx_v * 13 + dx[i]) / 14
        ind["adx"] = float(adx_v)
    else:
        ind["adx"] = 0.0

    return ind


def score_stock(ind, params):
    """Score a stock using strategy parameters."""
    sc = 0
    p = params
    ma_fw = int(p.get("ma_fast_w", 5))
    mom_days = int(p.get("momentum_days", 5))
    mom_val = ind.get(f"momentum_{mom_days}", 0)

    checks = [
        ("w_rsi", ind["rsi"] >= p.get("rsi_th", 55)),
        ("w_bb", ind["bb_pos"] >= p.get("bb_th", 0.7)),
        ("w_ma", ind["price"] > ind.get(f"ma{ma_fw}", 0)),
        ("w_wr", ind.get("williams_r", -50) >= p.get("wr_th", -30)),
        ("w_mom", mom_val >= p.get("mom_th", 3)),
        ("w_near_high", abs(ind.get("near_high", 99)) <= p.get("near_high_pct", 10)),
        ("w_squeeze", ind.get("squeeze_fire", 0) == 1),
        ("w_new_high", ind.get("new_high_60", 0) == 1),
        ("w_adx", ind.get("adx", 0) >= p.get("adx_th", 25)),
        ("w_atr", ind.get("atr_pct", 0) >= p.get("atr_min", 2.0)),
    ]

    for key, cond in checks:
        w = int(p.get(key, 0))
        if w > 0 and cond:
            sc += w

    # KD (special: cross mode)
    w = int(p.get("w_kd", 0))
    if w > 0:
        ok = ind["k_val"] >= p.get("kd_th", 50)
        if ok and p.get("kd_cross", 0) == 1:
            ok = ind.get("kd_golden_cross", 0)
        if ok:
            sc += w

    # Auxiliary +1 signals
    if p.get("above_ma60", 0) == 1 and ind.get("above_ma60", 0):
        sc += 1
    if p.get("vol_gt_yesterday", 0) == 1 and ind.get("vol_gt_yesterday", 0):
        sc += 1

    return sc


def run_scan(params, held_tickers=None):
    """Run a live scan: fetch data → compute → score → rank."""
    import yfinance as yf

    if held_tickers is None:
        held_tickers = set()

    # 1. Market data from official APIs
    market_data, trading_date = fetch_market_data()
    if not market_data or len(market_data) < 50:
        return None

    # 2. Top 50 by volume (exclude held)
    top = sorted(market_data.keys(), key=lambda t: market_data[t]["vol"], reverse=True)[:50]

    # 3. Download 3-month history via yfinance
    try:
        hist = yf.download(top, period="3mo", progress=False, threads=True)
    except Exception:
        return None

    if hist.empty:
        return None

    # 4. Score each stock
    threshold = params.get("buy_threshold", 6)
    signals = []

    for ticker in top:
        if ticker in held_tickers:
            continue
        try:
            if len(top) > 1:
                df_c = hist["Close"][ticker].dropna()
                df_h = hist["High"][ticker].dropna()
                df_l = hist["Low"][ticker].dropna()
                df_v = hist["Volume"][ticker].dropna()
            else:
                df_c = hist["Close"].dropna()
                df_h = hist["High"].dropna()
                df_l = hist["Low"].dropna()
                df_v = hist["Volume"].dropna()

            c = df_c.values.astype(np.float64)
            h = df_h.values.astype(np.float64)
            lo = df_l.values.astype(np.float64)
            v = df_v.values.astype(np.float64)

            if len(c) < 20:
                continue

            ind = compute_indicators(c, h, lo, v)
            if ind is None:
                continue

            sc = score_stock(ind, params)
            if sc >= threshold:
                signals.append({
                    "ticker": ticker,
                    "name": market_data[ticker].get("name", ticker),
                    "score": sc,
                    "close": market_data[ticker]["close"],
                    "vol_ratio": round(ind["vol_ratio"], 1),
                })
        except Exception:
            continue

    signals.sort(key=lambda x: (x["score"], x.get("vol_ratio", 0)), reverse=True)

    twse_n = len([k for k in market_data if ".TW" in k and ".TWO" not in k])
    otc_n = len([k for k in market_data if ".TWO" in k])

    return {
        "date": trading_date,
        "timestamp": datetime.now().isoformat(),
        "buy_signals": [{"rank": i + 1, **s} for i, s in enumerate(signals[:20])],
        "market_summary": {"twse_count": twse_n, "otc_count": otc_n, "scan_count": 50},
    }
