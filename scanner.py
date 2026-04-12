"""
Live scanner for 龍蝦選股系統 Web App
Uses Gist history cache + TWSE/TPEx live data (zero yfinance)
"""

import requests
import numpy as np
from datetime import datetime, timedelta, timezone

TW_TZ = timezone(timedelta(hours=8))
import warnings
import urllib3

urllib3.disable_warnings()
warnings.filterwarnings("ignore")


def fetch_market_data():
    """Fetch today's OHLCV from TWSE + TPEx (2 API calls)."""
    all_data = {}
    today = datetime.now(TW_TZ)
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
                o = float(row[4].replace(",", "").replace("--", "0"))
                h = float(row[5].replace(",", "").replace("--", "0"))
                lo = float(row[6].replace(",", "").replace("--", "0"))
                c = float(row[7].replace(",", "").replace("--", "0"))
                if vol > 0 and c > 0:
                    all_data[f"{code}.TW"] = {
                        "open": o, "high": h, "low": lo,
                        "close": c, "vol": vol, "name": row[1].strip(),
                    }
            except Exception:
                continue
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
                    o = float(row[4].replace(",", "")) if len(row) > 4 and row[4].replace(",", "").replace(".", "").isdigit() else c
                    h = float(row[5].replace(",", "")) if len(row) > 5 and row[5].replace(",", "").replace(".", "").isdigit() else c
                    lo = float(row[6].replace(",", "")) if len(row) > 6 and row[6].replace(",", "").replace(".", "").isdigit() else c
                    if vol > 0 and c > 0:
                        all_data[f"{code}.TWO"] = {
                            "open": o, "high": h, "low": lo,
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

    for w in [3, 5, 8, 10, 15, 20, 30, 60]:
        ind[f"ma{w}"] = float(np.mean(c[last - w:last])) if n > w else float(c[last])

    bb_win = c[last - 20:last] if n > 20 else (c[:last] if last > 0 else c)
    bb_mid = float(np.mean(bb_win))
    bb_std = float(np.std(bb_win))
    bb_range = 4 * bb_std
    ind["bb_pos"] = min(2.0, max(-0.5, (c[last] - (bb_mid - 2 * bb_std)) / bb_range)) if bb_range > 1e-6 else 0.5

    vol_avg = float(np.mean(vol[last - 20:last])) if n > 20 else (float(np.mean(vol[:last])) if last > 0 else 1)
    ind["vol_ratio"] = float(vol[last] / vol_avg) if vol_avg > 0 else 1.0

    if last >= 1 and n > 21:
        vap = float(np.mean(vol[last - 21:last - 1]))
        vrp = float(vol[last - 1] / vap) if vap > 0 else 1
    else:
        vrp = 1.0
    ind["vol_gt_yesterday"] = 1 if ind["vol_ratio"] > vrp else 0

    kv = np.zeros(n); dv = np.zeros(n); kv[0] = 50; dv[0] = 50
    for i in range(1, n):
        lo9 = np.min(lo[max(0, i - 9):i + 1])
        hi9 = np.max(h[max(0, i - 9):i + 1])
        rsv = (c[i] - lo9) / (hi9 - lo9) * 100 if hi9 > lo9 else 50
        kv[i] = kv[i - 1] * 2 / 3 + rsv / 3
        dv[i] = dv[i - 1] * 2 / 3 + kv[i] / 3
    ind["k_val"] = float(kv[last])
    ind["kd_golden_cross"] = 1 if kv[last] > dv[last] and (last < 1 or kv[last - 1] <= dv[last - 1]) else 0

    if n >= 15:
        h14 = float(np.max(h[last - 14:last + 1]))
        l14 = float(np.min(lo[last - 14:last + 1]))
        ind["williams_r"] = float((h14 - c[last]) / (h14 - l14) * -100) if h14 > l14 else -50
    else:
        ind["williams_r"] = -50.0

    for d in [3, 5, 10]:
        ind[f"momentum_{d}"] = float((c[last] / c[last - d] - 1) * 100) if last >= d else 0

    h20 = float(np.max(h[last - 20:last + 1])) if n >= 21 else float(np.max(h))
    ind["near_high"] = float((c[last] / h20 - 1) * 100) if h20 > 0 else 0
    ind["new_high_60"] = 1 if n > 60 and c[last] > np.max(h[last - 60:last]) else 0
    ind["above_ma60"] = 1 if c[last] >= ind.get("ma60", c[last]) else 0

    tr = np.zeros(n)
    for i in range(1, n):
        tr[i] = max(h[i] - lo[i], abs(h[i] - c[i - 1]), abs(lo[i] - c[i - 1]))
    atr = np.zeros(n)
    for i in range(1, n):
        atr[i] = np.mean(tr[1:min(i + 1, 15)]) if i <= 14 else (atr[i - 1] * 13 + tr[i]) / 14
    ind["atr_pct"] = float(atr[last] / c[last] * 100) if c[last] > 0 else 0

    e12 = np.zeros(n); e26 = np.zeros(n); e12[0] = c[0]; e26[0] = c[0]
    for i in range(1, n):
        e12[i] = e12[i - 1] * (1 - 2 / 13) + c[i] * 2 / 13
        e26[i] = e26[i - 1] * (1 - 2 / 27) + c[i] * 2 / 27
    ml = e12 - e26
    ms = np.zeros(n); ms[0] = ml[0]
    for i in range(1, n):
        ms[i] = ms[i - 1] * (1 - 2 / 10) + ml[i] * 2 / 10
    mh = ml - ms

    def _sq(idx):
        if idx < 20: return False
        w = c[idx - 20:idx]; m = np.mean(w); s = np.std(w)
        return (m + 2 * s) < (m + 1.5 * atr[idx]) and (m - 2 * s) > (m - 1.5 * atr[idx])
    sq_y = _sq(last - 1) if last >= 1 and n > 21 else False
    sq_t = _sq(last) if n > 20 else False
    ind["squeeze_fire"] = 1 if sq_y and not sq_t and mh[last] > 0 else 0

    if n >= 29:
        pdm = np.zeros(n); mdm = np.zeros(n)
        for i in range(1, n):
            up = h[i] - h[i - 1]; dn = lo[i - 1] - lo[i]
            pdm[i] = up if up > dn and up > 0 else 0
            mdm[i] = dn if dn > up and dn > 0 else 0
        a14 = np.mean(tr[1:15]); sp = np.mean(pdm[1:15]); sm = np.mean(mdm[1:15])
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

    for key, cond in [
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
    ]:
        w = int(p.get(key, 0))
        if w > 0 and cond:
            sc += w

    w = int(p.get("w_kd", 0))
    if w > 0:
        ok = ind["k_val"] >= p.get("kd_th", 50)
        if ok and p.get("kd_cross", 0) == 1:
            ok = ind.get("kd_golden_cross", 0)
        if ok:
            sc += w

    if p.get("above_ma60", 0) == 1 and ind.get("above_ma60", 0):
        sc += 1
    if p.get("vol_gt_yesterday", 0) == 1 and ind.get("vol_gt_yesterday", 0):
        sc += 1

    return sc


def compute_indicators_with_state(c, h, lo, vol, state):
    """Use pre-computed running states for Wilder indicators (exact match with Mac)."""
    n = len(c)
    if n < 20:
        return None
    last = n - 1
    ind = {"price": float(c[last])}

    # RSI from running state (exact)
    ag = state["rsi_ag"]; al = state["rsi_al"]
    ind["rsi"] = float(100 - 100 / (1 + ag / al)) if al > 0 else 100.0

    # MAs from cache (only needs recent data)
    for w in [3, 5, 8, 10, 15, 20, 30, 60]:
        ind[f"ma{w}"] = float(np.mean(c[last-w:last])) if n > w else float(c[last])

    # BB from cache
    bb_win = c[last-20:last] if n > 20 else (c[:last] if last > 0 else c)
    bb_mid = float(np.mean(bb_win)); bb_std = float(np.std(bb_win))
    bb_range = 4 * bb_std
    ind["bb_pos"] = min(2.0, max(-0.5, (c[last]-(bb_mid-2*bb_std))/bb_range)) if bb_range > 1e-6 else 0.5

    # Volume from cache
    vol_avg = float(np.mean(vol[last-20:last])) if n > 20 else (float(np.mean(vol[:last])) if last > 0 else 1)
    ind["vol_ratio"] = float(vol[last] / vol_avg) if vol_avg > 0 else 1.0
    if last >= 1 and n > 21:
        vap = float(np.mean(vol[last-21:last-1]))
        vrp = float(vol[last-1] / vap) if vap > 0 else 1
    else:
        vrp = 1.0
    ind["vol_gt_yesterday"] = 1 if ind["vol_ratio"] > vrp else 0

    # KD from running state
    ind["k_val"] = state["kd_k"]
    ind["kd_golden_cross"] = 1 if state["kd_k"] > state["kd_d"] and state["kd_k_prev"] <= state["kd_d_prev"] else 0

    # Williams %R from cache (only needs 15 days)
    if n >= 15:
        h14 = float(np.max(h[last-14:last+1])); l14 = float(np.min(lo[last-14:last+1]))
        ind["williams_r"] = float((h14-c[last])/(h14-l14)*-100) if h14 > l14 else -50
    else:
        ind["williams_r"] = -50.0

    # Momentum from cache
    for d in [3, 5, 10]:
        ind[f"momentum_{d}"] = float((c[last]/c[last-d]-1)*100) if last >= d else 0

    # Near high / new high from cache
    h20 = float(np.max(h[last-20:last+1])) if n >= 21 else float(np.max(h))
    ind["near_high"] = float((c[last]/h20-1)*100) if h20 > 0 else 0
    ind["new_high_60"] = 1 if n > 60 and c[last] > np.max(h[last-60:last]) else 0
    ind["above_ma60"] = 1 if c[last] >= ind.get("ma60", c[last]) else 0

    # ATR from running state (exact)
    ind["atr_pct"] = float(state["atr14"] / c[last] * 100) if c[last] > 0 else 0

    # Squeeze: use running state ATR + cache BB
    atr_val = state["atr14"]
    def _sq_with_atr(idx, atr_v):
        if idx < 20: return False
        w = c[idx-20:idx]; m = np.mean(w); s = np.std(w)
        return (m+2*s) < (m+1.5*atr_v) and (m-2*s) > (m-1.5*atr_v)
    sq_t = _sq_with_atr(last, atr_val) if n > 20 else False
    sq_y = _sq_with_atr(last-1, atr_val) if last >= 1 and n > 21 else False
    ind["squeeze_fire"] = 1 if sq_y and not sq_t and state["mh"] > 0 else 0

    # ADX from running state (exact)
    ind["adx"] = float(state["adx_val"])

    return ind


def run_scan(params, held_tickers=None, history_cache=None, indicator_states=None):
    """
    Live scan: Gist history cache + TWSE/TPEx today → indicators → score.
    Zero yfinance. Pure TWSE/TPEx.
    """
    if held_tickers is None:
        held_tickers = set()
    if not history_cache or "stocks" not in history_cache:
        return None

    cache_stocks = history_cache["stocks"]
    states = indicator_states.get("states", {}) if indicator_states else {}

    # 1. Today's market data from official APIs
    market_data, trading_date = fetch_market_data()
    if not market_data or len(market_data) < 50:
        return None

    # 2. Top 100 by volume (matching Mac scan)
    top = sorted(market_data.keys(), key=lambda t: market_data[t]["vol"], reverse=True)[:100]

    # 3. Score each stock using cache + today
    threshold = params.get("buy_threshold", 6)
    signals = []

    for ticker in top:
        if ticker in held_tickers:
            continue
        if ticker not in cache_stocks:
            continue

        try:
            cs = cache_stocks[ticker]
            hist_c = cs["c"]
            hist_h = cs["h"]
            hist_l = cs["l"]
            hist_v = cs["v"]

            # Merge today's data if newer than cache
            cache_updated = history_cache.get("updated", "")
            today_info = market_data[ticker]
            new_day = trading_date > cache_updated

            if new_day:
                c = np.array(hist_c + [today_info["close"]], dtype=np.float64)
                h = np.array(hist_h + [today_info["high"]], dtype=np.float64)
                lo = np.array(hist_l + [today_info["low"]], dtype=np.float64)
                v = np.array(hist_v + [today_info["vol"]], dtype=np.float64)
            else:
                c = np.array(hist_c, dtype=np.float64)
                h = np.array(hist_h, dtype=np.float64)
                lo = np.array(hist_l, dtype=np.float64)
                v = np.array(hist_v, dtype=np.float64)

            if len(c) < 20:
                continue

            if ticker in states:
                st = dict(states[ticker])  # copy
                # Update running state with new day's data
                if new_day and len(hist_c) > 0:
                    prev_c = hist_c[-1]
                    new_c = today_info["close"]
                    change = new_c - prev_c
                    # RSI
                    st["rsi_ag"] = (st["rsi_ag"] * 13 + max(change, 0)) / 14
                    st["rsi_al"] = (st["rsi_al"] * 13 + max(-change, 0)) / 14
                    # MACD
                    st["ema12"] = st["ema12"] * (1 - 2/13) + new_c * 2/13
                    st["ema26"] = st["ema26"] * (1 - 2/27) + new_c * 2/27
                    new_ml = st["ema12"] - st["ema26"]
                    st["mh_prev"] = st["mh"]
                    st["macd_sig"] = st["macd_sig"] * (1 - 2/10) + new_ml * 2/10
                    st["mh"] = new_ml - st["macd_sig"]
                    # ATR
                    new_tr = max(today_info["high"] - today_info["low"],
                                abs(today_info["high"] - prev_c),
                                abs(today_info["low"] - prev_c))
                    st["atr14"] = (st["atr14"] * 13 + new_tr) / 14
                    # ADX
                    up = today_info["high"] - h[-2] if len(h) > 1 else 0
                    dn = lo[-2] - today_info["low"] if len(lo) > 1 else 0
                    pdm_v = up if up > dn and up > 0 else 0
                    mdm_v = dn if dn > up and dn > 0 else 0
                    st["adx_a14"] = (st["adx_a14"] * 13 + new_tr) / 14
                    st["adx_sp"] = (st["adx_sp"] * 13 + pdm_v) / 14
                    st["adx_sm"] = (st["adx_sm"] * 13 + mdm_v) / 14
                    pdi = st["adx_sp"] / st["adx_a14"] * 100 if st["adx_a14"] > 0 else 0
                    mdi = st["adx_sm"] / st["adx_a14"] * 100 if st["adx_a14"] > 0 else 0
                    dx = abs(pdi - mdi) / (pdi + mdi) * 100 if pdi + mdi > 0 else 0
                    st["adx_val"] = (st["adx_val"] * 13 + dx) / 14
                    # KD
                    lo9 = float(np.min(lo[-10:])) if len(lo) >= 10 else float(np.min(lo))
                    hi9 = float(np.max(h[-10:])) if len(h) >= 10 else float(np.max(h))
                    rsv = (new_c - lo9) / (hi9 - lo9) * 100 if hi9 > lo9 else 50
                    st["kd_k_prev"] = st["kd_k"]
                    st["kd_d_prev"] = st["kd_d"]
                    st["kd_k"] = st["kd_k"] * 2/3 + rsv / 3
                    st["kd_d"] = st["kd_d"] * 2/3 + st["kd_k"] / 3
                ind = compute_indicators_with_state(c, h, lo, v, st)
            else:
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
        "timestamp": datetime.now(TW_TZ).isoformat(),
        "buy_signals": [{"rank": i + 1, **s} for i, s in enumerate(signals[:20])],
        "market_summary": {"twse_count": twse_n, "otc_count": otc_n, "scan_count": 100},
    }


def check_sell_signals(holdings, params, market_data, history_cache):
    """Check sell conditions for user's holdings."""
    from datetime import date as _date
    signals = []
    sp = params
    cache_stocks = history_cache.get("stocks", {}) if history_cache else {}

    for h in holdings:
        ticker = h.get("ticker", "")
        buy_price = h.get("buy_price", 0)
        buy_date_str = h.get("buy_date", "")
        name = h.get("name", "")

        if not buy_price or not ticker:
            continue

        # Current price
        cur_price = None
        if market_data and ticker in market_data:
            cur_price = market_data[ticker]["close"]
        if not cur_price:
            continue

        ret = (cur_price / buy_price - 1) * 100

        # Calendar days → approximate trading days (GPU uses trading days)
        try:
            calendar_days = (datetime.now(TW_TZ).date() - _date.fromisoformat(buy_date_str)).days
        except (ValueError, TypeError):
            calendar_days = 0
        days_held = max(0, int(calendar_days * 5 / 7))  # approximate trading days

        # Skip sell checks on buy day (matching GPU: dh < 1 → skip)
        if days_held < 1:
            h["peak_price"] = round(max(h.get("peak_price", buy_price), cur_price), 2)
            continue

        stop_loss = sp.get("stop_loss", -20)
        take_profit = sp.get("take_profit", 80)
        hold_days = sp.get("hold_days", 30)
        trailing_stop = sp.get("trailing_stop", 0)
        reason = None

        # Peak price: only since buy (stored in holding, not from full cache)
        peak_price = max(h.get("peak_price", buy_price), cur_price)

        # 1. Stop loss
        if ret <= stop_loss:
            reason = f"停損！報酬 {ret:+.1f}%（停損線 {stop_loss}%）"

        # 2. Take profit
        if reason is None and sp.get("use_take_profit", 1) and ret >= take_profit:
            reason = f"停利！報酬 +{ret:.1f}%（目標 +{take_profit}%）"

        # 3. Trailing stop (移動停利) — peak must be meaningfully above buy
        if reason is None and trailing_stop > 0 and peak_price > buy_price * 1.01:
            dd = (cur_price / peak_price - 1) * 100
            if dd <= -trailing_stop:
                reason = f"移動停利！從高點 {peak_price:.1f} 回撤 {dd:.1f}%（門檻 -{trailing_stop}%），報酬 {ret:+.1f}%"

        # 4. Below MA60 — only if bought ABOVE MA60 (otherwise user chose to buy below)
        if reason is None and int(sp.get("sell_below_ma", 0)) > 0 and ticker in cache_stocks:
            cs = cache_stocks[ticker]
            closes = list(cs["c"])
            if ticker in market_data:
                closes = closes + [market_data[ticker]["close"]]
            if len(closes) > 60:
                ma60 = float(np.mean(closes[-61:-1]))
                if buy_price >= ma60 and cur_price < ma60:
                    reason = f"跌破 MA60！現價 {cur_price:.1f} < MA60 {ma60:.1f}，報酬 {ret:+.1f}%"

        # 5. Stagnation exit — use trading days
        if reason is None and sp.get("use_stagnation_exit", 0):
            stag_days = int(sp.get("stagnation_days", 10))
            stag_min = sp.get("stagnation_min_ret", 5)
            if days_held >= stag_days and ret < stag_min:
                reason = f"停滯出場！持有 {days_held} 交易日報酬僅 {ret:+.1f}%"

        # 6. Profit lock (鎖利) — peak must have actually reached trigger
        if reason is None and sp.get("use_profit_lock", 0):
            lt = sp.get("lock_trigger", 30)
            lf = sp.get("lock_floor", 10)
            peak_gain = (peak_price / buy_price - 1) * 100
            if peak_gain >= lt and ret < lf:
                reason = f"鎖利出場！曾漲 +{peak_gain:.1f}% 但跌回 +{ret:.1f}%（鎖利線 +{lf}%）"

        # 7. Time decay (gradual profit requirement) — use trading days
        if reason is None and sp.get("use_time_decay", 0):
            rpd = sp.get("ret_per_day", 0.5)
            hd_half = int(hold_days) // 2
            if days_held >= hd_half:
                min_req = (days_held - hd_half) * rpd
                if ret < min_req:
                    reason = f"漸進停利！持有 {days_held} 交易日報酬 {ret:+.1f}%，低於期望 +{min_req:.1f}%"

        # 8. Max hold days — use trading days
        if reason is None and days_held >= hold_days:
            reason = f"持有已達 {days_held} 交易日（上限 {hold_days}），報酬 {ret:+.1f}%"

        # Update peak_price in holding (for persistent tracking)
        h["peak_price"] = round(peak_price, 2)

        if reason:
            signals.append({
                "ticker": ticker,
                "name": name,
                "buy_price": buy_price,
                "current_price": cur_price,
                "return": round(ret, 1),
                "days_held": days_held,
                "reason": reason,
            })

    return signals
