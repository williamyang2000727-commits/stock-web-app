"""
Daily automated scan - runs via GitHub Actions at 16:35 Taiwan time
Updates: indicator states, history cache, backtest extension, scan results
Zero human intervention needed.
"""

import os
import json
import requests
import numpy as np
from datetime import datetime, date, timedelta, timezone

TW_TZ = timezone(timedelta(hours=8))
TOKEN = os.environ["GITHUB_TOKEN_GIST"]
DATA_GIST = os.environ["DATA_GIST_ID"]
HISTORY_GIST = os.environ["HISTORY_GIST_ID"]
STATE_GIST = os.environ["STATE_GIST_ID"]
GPU_GIST = os.environ.get("GPU_GIST_ID", "c1bef892d33589baef2142ce250d18c2")
HEADERS = {"Authorization": f"token {TOKEN}"}


def read_gist(gist_id):
    r = requests.get(f"https://api.github.com/gists/{gist_id}", headers=HEADERS, timeout=15)
    result = {}
    for fname, fdata in r.json().get("files", {}).items():
        if fdata.get("truncated"):
            raw = requests.get(fdata["raw_url"], headers=HEADERS, timeout=60)
            result[fname] = json.loads(raw.text)
        else:
            try:
                result[fname] = json.loads(fdata.get("content", "{}"))
            except:
                result[fname] = {}
    return result


def write_gist(gist_id, filename, data):
    payload = {"files": {filename: {"content": json.dumps(data, ensure_ascii=False)}}}
    r = requests.patch(f"https://api.github.com/gists/{gist_id}", headers=HEADERS, json=payload, timeout=60)
    return r.status_code == 200


def fetch_market_data():
    """Fetch all stocks from TWSE + TPEx"""
    import urllib3
    urllib3.disable_warnings()
    all_data = {}
    today = datetime.now(TW_TZ)
    date_ad = today.strftime("%Y%m%d")
    date_roc = f"{today.year - 1911}/{today.month:02d}/{today.day:02d}"
    trading_date = today.strftime("%Y-%m-%d")

    try:
        r = requests.get(f"https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json&date={date_ad}",
                         timeout=15, verify=False, headers={"User-Agent": "Mozilla/5.0"})
        data = r.json()
        for row in data.get("data", []):
            try:
                code = row[0].strip()
                if code.startswith("00"): continue
                vol = int(row[2].replace(",", ""))
                c = float(row[7].replace(",", ""))
                if vol > 0 and c > 0:
                    if "--" in row[4] or "--" in row[5] or "--" in row[6]:
                        o = h = lo = c
                    else:
                        o = float(row[4].replace(",", "")) if row[4].replace(",", "").replace(".", "").replace("-", "").isdigit() else c
                        h = float(row[5].replace(",", "")) if row[5].replace(",", "").replace(".", "").replace("-", "").isdigit() else c
                        lo = float(row[6].replace(",", "")) if row[6].replace(",", "").replace(".", "").replace("-", "").isdigit() else c
                    all_data[f"{code}.TW"] = {"open": o, "high": h, "low": lo, "close": c, "vol": vol, "name": row[1].strip()}
            except:
                continue
        rd = data.get("date", "")
        if rd and len(rd) == 8:
            trading_date = f"{rd[:4]}-{rd[4:6]}-{rd[6:8]}"
    except:
        pass

    try:
        r = requests.get("https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php",
                         params={"l": "zh-tw", "d": date_roc}, headers={"User-Agent": "Mozilla/5.0"},
                         timeout=15, verify=False)
        for t in r.json().get("tables", []):
            for row in t.get("data", []):
                try:
                    code = row[0].strip()
                    if not code or code.startswith("00"): continue
                    vol = int(row[8].replace(",", "")) if row[8].replace(",", "").isdigit() else 0
                    c = float(row[2].replace(",", "")) if row[2].replace(",", "").replace(".", "").isdigit() else 0
                    if vol > 0 and c > 0:
                        o = float(row[4].replace(",", "")) if len(row) > 4 and row[4].replace(",", "").replace(".", "").isdigit() else c
                        h = float(row[5].replace(",", "")) if len(row) > 5 and row[5].replace(",", "").replace(".", "").isdigit() else c
                        lo = float(row[6].replace(",", "")) if len(row) > 6 and row[6].replace(",", "").replace(".", "").isdigit() else c
                        all_data[f"{code}.TWO"] = {"open": o, "high": h, "low": lo, "close": c, "vol": vol, "name": row[1].strip()}
                except:
                    continue
    except:
        pass

    return all_data, trading_date


def compute_indicators_with_state(c, h, lo, vol, state):
    """Same as scanner.py"""
    n = len(c)
    if n < 20: return None
    last = n - 1
    ind = {"price": float(c[last])}

    ag = state["rsi_ag"]; al = state["rsi_al"]
    ind["rsi"] = float(100 - 100 / (1 + ag / al)) if al > 0 else 100.0

    for w in [3, 5, 8, 10, 15, 20, 30, 60]:
        ind[f"ma{w}"] = float(np.mean(c[last - w:last])) if n > w else float(c[last])

    bb_win = c[last - 20:last] if n > 20 else (c[:last] if last > 0 else c)
    bb_mid = float(np.mean(bb_win)); bb_std = float(np.std(bb_win))
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

    ind["k_val"] = state["kd_k"]
    ind["kd_golden_cross"] = 1 if state["kd_k"] > state["kd_d"] and state["kd_k_prev"] <= state["kd_d_prev"] else 0

    if n >= 15:
        h14 = float(np.max(h[last - 14:last + 1])); l14 = float(np.min(lo[last - 14:last + 1]))
        ind["williams_r"] = float((h14 - c[last]) / (h14 - l14) * -100) if h14 > l14 else -50
    else:
        ind["williams_r"] = -50.0

    for d in [3, 5, 10]:
        ind[f"momentum_{d}"] = float((c[last] / c[last - d] - 1) * 100) if last >= d else 0

    h20 = float(np.max(h[last - 20:last + 1])) if n >= 21 else float(np.max(h))
    ind["near_high"] = float((c[last] / h20 - 1) * 100) if h20 > 0 else 0
    ind["new_high_60"] = 1 if n > 60 and c[last] > np.max(h[last - 60:last]) else 0
    ind["above_ma60"] = 1 if c[last] >= ind.get("ma60", c[last]) else 0

    ind["atr_pct"] = float(state["atr14"] / c[last] * 100) if c[last] > 0 else 0

    atr_val = state["atr14"]
    def _sq(idx, av):
        if idx < 20: return False
        w = c[idx - 20:idx]; m = np.mean(w); s = np.std(w)
        return (m + 2 * s) < (m + 1.5 * av) and (m - 2 * s) > (m - 1.5 * av)
    sq_t = _sq(last, atr_val) if n > 20 else False
    sq_y = _sq(last - 1, atr_val) if last >= 1 and n > 21 else False
    ind["squeeze_fire"] = 1 if sq_y and not sq_t and state["mh"] > 0 else 0
    ind["adx"] = float(state["adx_val"])

    # 補齊：MACD / BIAS / OBV / up_days / vol_up_days / mom_accel / week52
    if "macd_hist" in state:
        ind["macd_hist"] = float(state["macd_hist"])
        ind["macd_line"] = float(state.get("macd_line", 0))
        ind["macd_hist_prev"] = float(state.get("macd_hist_prev", 0))
    else:
        e12 = np.zeros(n); e26 = np.zeros(n); e12[0] = c[0]; e26[0] = c[0]
        for i in range(1, n):
            e12[i] = e12[i-1] * (1 - 2/13) + c[i] * 2/13
            e26[i] = e26[i-1] * (1 - 2/27) + c[i] * 2/27
        ml_arr = e12 - e26
        ms_arr = np.zeros(n); ms_arr[0] = ml_arr[0]
        for i in range(1, n):
            ms_arr[i] = ms_arr[i-1] * (1 - 2/10) + ml_arr[i] * 2/10
        mh_arr = ml_arr - ms_arr
        ind["macd_line"] = float(ml_arr[last])
        ind["macd_hist"] = float(mh_arr[last])
        ind["macd_hist_prev"] = float(mh_arr[last - 1]) if last >= 1 else 0.0

    ma20_v = ind.get("ma20", c[last])
    ind["bias"] = float((c[last] - ma20_v) / ma20_v * 100) if ma20_v > 0 else 0.0

    obv = np.zeros(n)
    for i in range(1, n):
        if c[i] > c[i - 1]: obv[i] = obv[i - 1] + vol[i]
        elif c[i] < c[i - 1]: obv[i] = obv[i - 1] - vol[i]
        else: obv[i] = obv[i - 1]
    for d in [3, 5, 10]:
        ind[f"obv_rising_{d}"] = 1 if last >= d and obv[last] > obv[last - d] else 0

    up = 0
    for i in range(last, 0, -1):
        if c[i] > c[i - 1]: up += 1
        else: break
    ind["up_days"] = int(up)

    vup = 0
    for i in range(last, 0, -1):
        if vol[i] > vol[i - 1]: vup += 1
        else: break
    ind["vol_up_days"] = int(vup)

    if last >= 6:
        m_t = (c[last] / c[last - 5] - 1) * 100
        m_y = (c[last - 1] / c[last - 6] - 1) * 100
        ind["mom_accel"] = float(m_t - m_y)
    else:
        ind["mom_accel"] = 0.0

    w52_n = min(250, n)
    w52_start = last - w52_n + 1
    if w52_start >= 0 and w52_n >= 20:
        high_w = float(np.max(h[w52_start:last + 1]))
        low_w = float(np.min(lo[w52_start:last + 1]))
        ind["week52_pos"] = (c[last] - low_w) / (high_w - low_w) if high_w > low_w else 0.5
    else:
        ind["week52_pos"] = 0.5

    return ind


def score_stock(ind, params):
    """Must match GPU kernel 1:1 — mirrors scanner.py score_stock exactly."""
    sc = 0
    p = params
    ma_fw = int(p.get("ma_fast_w", 5))
    mom_days = int(p.get("momentum_days", 5))
    mom_val = ind.get(f"momentum_{mom_days}", 0)

    for key, cond in [
        ("w_rsi", ind["rsi"] >= p.get("rsi_th", 55)),
        ("w_bb", ind["bb_pos"] >= p.get("bb_th", 0.7)),
        ("w_vol", ind.get("vol_ratio", 0) >= p.get("vol_th", 3)),
        ("w_ma", ind["price"] > ind.get(f"ma{ma_fw}", 0)),
        ("w_wr", ind.get("williams_r", -50) >= p.get("wr_th", -30)),
        ("w_mom", mom_val >= p.get("mom_th", 3)),
        ("w_near_high", abs(ind.get("near_high", 99)) <= p.get("near_high_pct", 10)),
        ("w_squeeze", ind.get("squeeze_fire", 0) == 1),
        ("w_new_high", ind.get("new_high_60", 0) == 1),
        ("w_adx", ind.get("adx", 0) >= p.get("adx_th", 25)),
        ("w_atr", ind.get("atr_pct", 0) >= p.get("atr_min", 2.0)),
        ("w_bias", 0 <= ind.get("bias", -1) <= p.get("bias_max", 15)),
        ("w_up_days", ind.get("up_days", 0) >= p.get("up_days_min", 3)),
        ("w_week52", ind.get("week52_pos", 0) >= p.get("week52_min", 0.7)),
        ("w_vol_up_days", ind.get("vol_up_days", 0) >= p.get("vol_up_days_min", 3)),
        ("w_mom_accel", ind.get("mom_accel", -99) >= p.get("mom_accel_min", 2)),
    ]:
        w = int(p.get(key, 0))
        if w > 0 and cond: sc += w

    w = int(p.get("w_kd", 0))
    if w > 0:
        ok = ind["k_val"] >= p.get("kd_th", 50)
        if ok and p.get("kd_cross", 0) == 1: ok = ind.get("kd_golden_cross", 0)
        if ok: sc += w

    w = int(p.get("w_macd", 0))
    if w > 0:
        mm = int(p.get("macd_mode", 2))
        ok = False
        if mm == 0:
            ok = ind.get("macd_hist", 0) > 0 and ind.get("macd_hist_prev", 0) <= 0
        elif mm == 1:
            ok = ind.get("macd_line", 0) > 0
        elif mm == 2:
            ok = ind.get("macd_hist", 0) > 0
        if ok: sc += w

    w = int(p.get("w_obv", 0))
    if w > 0:
        od = int(p.get("obv_rising_days", 10))
        od_key = min([3, 5, 10], key=lambda x: abs(x - od))
        if ind.get(f"obv_rising_{od_key}", 0) == 1: sc += w

    cg = int(p.get("consecutive_green", 0))
    if cg >= 1 and ind.get("up_days", 0) >= cg: sc += 1
    if p.get("gap_up", 0) == 1 and ind.get("gap_pct", 0) >= 1.0: sc += 1
    if p.get("above_ma60", 0) == 1 and ind.get("above_ma60", 0): sc += 1
    if p.get("vol_gt_yesterday", 0) == 1 and ind.get("vol_gt_yesterday", 0): sc += 1
    return sc


def main():
    print(f"[{datetime.now(TW_TZ).strftime('%Y-%m-%d %H:%M')}] Daily scan starting...")

    # 1. Fetch market data
    market_data, trading_date = fetch_market_data()
    print(f"Market: {len(market_data)} stocks, date={trading_date}")
    if len(market_data) < 500:
        print("Market data insufficient, aborting")
        return

    # 2. Read all Gist data
    data_gist = read_gist(DATA_GIST)
    history_data = read_gist(HISTORY_GIST)
    state_data = read_gist(STATE_GIST)
    gpu_data = read_gist(GPU_GIST)

    history = list(history_data.values())[0] if history_data else {}
    states_all = list(state_data.values())[0] if state_data else {}
    states = states_all.get("states", {})
    cache = history.get("stocks", {})
    cache_updated = history.get("updated", "")
    state_updated = states_all.get("updated", "")

    params = list(gpu_data.values())[0] if gpu_data else {}
    if "params" in params:
        params = params["params"]
    if not params:
        params = data_gist.get("strategy_params.json", {})
    print(f"Cache: {len(cache)} stocks, States: {len(states)} stocks")

    # 3. Update indicator states
    if trading_date > state_updated:
        print("Updating indicator states...")
        updated = 0
        for tk, sv in states.items():
            if tk not in market_data or tk not in cache: continue
            cs = cache[tk]
            if not cs.get("c"): continue
            info = market_data[tk]
            prev_c = cs["c"][-1]
            new_c = info["close"]
            ch = new_c - prev_c
            sv["rsi_ag"] = round((sv["rsi_ag"] * 13 + max(ch, 0)) / 14, 6)
            sv["rsi_al"] = round((sv["rsi_al"] * 13 + max(-ch, 0)) / 14, 6)
            sv["ema12"] = round(sv["ema12"] * (1 - 2 / 13) + new_c * 2 / 13, 4)
            sv["ema26"] = round(sv["ema26"] * (1 - 2 / 27) + new_c * 2 / 27, 4)
            new_ml = sv["ema12"] - sv["ema26"]
            sv["mh_prev"] = sv["mh"]
            sv["macd_sig"] = round(sv["macd_sig"] * (1 - 2 / 10) + new_ml * 2 / 10, 4)
            sv["mh"] = round(new_ml - sv["macd_sig"], 4)
            new_tr = max(info.get("high", new_c) - info.get("low", new_c),
                         abs(info.get("high", new_c) - prev_c), abs(info.get("low", new_c) - prev_c))
            sv["atr14"] = round((sv["atr14"] * 13 + new_tr) / 14, 4)
            prev_h = cs["h"][-1] if cs.get("h") else new_c
            prev_l = cs["l"][-1] if cs.get("l") else new_c
            up = info.get("high", new_c) - prev_h
            dn = prev_l - info.get("low", new_c)
            pdm = up if up > dn and up > 0 else 0
            mdm = dn if dn > up and dn > 0 else 0
            sv["adx_a14"] = round((sv["adx_a14"] * 13 + new_tr) / 14, 4)
            sv["adx_sp"] = round((sv["adx_sp"] * 13 + pdm) / 14, 4)
            sv["adx_sm"] = round((sv["adx_sm"] * 13 + mdm) / 14, 4)
            pdi = sv["adx_sp"] / sv["adx_a14"] * 100 if sv["adx_a14"] > 0 else 0
            mdi = sv["adx_sm"] / sv["adx_a14"] * 100 if sv["adx_a14"] > 0 else 0
            dx = abs(pdi - mdi) / (pdi + mdi) * 100 if pdi + mdi > 0 else 0
            sv["adx_val"] = round((sv["adx_val"] * 13 + dx) / 14, 4)
            lo_arr = cs["l"][-9:] + [info.get("low", new_c)]
            hi_arr = cs["h"][-9:] + [info.get("high", new_c)]
            rsv = (new_c - min(lo_arr)) / (max(hi_arr) - min(lo_arr)) * 100 if max(hi_arr) > min(lo_arr) else 50
            sv["kd_k_prev"] = sv["kd_k"]; sv["kd_d_prev"] = sv["kd_d"]
            sv["kd_k"] = round(sv["kd_k"] * 2 / 3 + rsv / 3, 4)
            sv["kd_d"] = round(sv["kd_d"] * 2 / 3 + sv["kd_k"] / 3, 4)
            updated += 1
        states_all["updated"] = trading_date
        write_gist(STATE_GIST, "indicator_state.json", states_all)
        print(f"  Updated {updated} stocks")

    # 4. Update history cache
    if trading_date > cache_updated:
        print("Updating history cache...")
        for tk, hist in cache.items():
            if tk in market_data:
                info = market_data[tk]
                hist["c"] = hist["c"][-79:] + [info["close"]]
                hist["h"] = hist["h"][-79:] + [info.get("high", info["close"])]
                hist["l"] = hist["l"][-79:] + [info.get("low", info["close"])]
                hist["v"] = hist["v"][-79:] + [info["vol"]]
                hist["dates"] = (hist.get("dates") or [])[-79:] + [trading_date]
        history["updated"] = trading_date
        write_gist(HISTORY_GIST, "history_cache.json", history)
        print(f"  Updated {len(cache)} stocks")

    # 5. Buy ranking scan
    print("Scanning buy signals...")
    top100 = sorted(market_data.keys(), key=lambda t: market_data[t]["vol"], reverse=True)[:100]
    buy_th = params.get("buy_threshold", 10)
    signals = []
    for tk in top100:
        if tk not in cache or tk not in states: continue
        cs = cache[tk]
        c = np.array(cs["c"], dtype=np.float64)
        h = np.array(cs["h"], dtype=np.float64)
        lo = np.array(cs["l"], dtype=np.float64)
        v = np.array(cs["v"], dtype=np.float64)
        if len(c) < 20: continue
        ind = compute_indicators_with_state(c, h, lo, v, states[tk])
        if ind is None: continue
        # Gap % — 今天 open vs 快取中昨天 close
        _td_info = market_data.get(tk, {})
        if _td_info.get("open") and len(cs["c"]) > 0:
            _prev_c = cs["c"][-1]
            ind["gap_pct"] = float((_td_info["open"] / _prev_c - 1) * 100) if _prev_c > 0 else 0.0
        else:
            ind["gap_pct"] = 0.0
        sc = score_stock(ind, params)
        if sc >= buy_th:
            signals.append({"rank": 0, "ticker": tk, "name": market_data[tk].get("name", tk),
                            "score": sc, "close": market_data[tk]["close"],
                            "vol_ratio": round(ind["vol_ratio"], 1)})
    signals.sort(key=lambda x: (x["score"], x["vol_ratio"]), reverse=True)
    for i, s in enumerate(signals): s["rank"] = i + 1
    print(f"  {len(signals)} signals, #1: {signals[0]['name'] if signals else 'none'}")

    # 6. Update scan_results
    twse_n = len([k for k in market_data if ".TW" in k and ".TWO" not in k])
    otc_n = len([k for k in market_data if ".TWO" in k])
    scan_results = {
        "date": trading_date,
        "timestamp": datetime.now(TW_TZ).isoformat(),
        "strategy_version": "auto",
        "strategy_score": "auto",
        "buy_signals": signals[:20],
        "sell_signals": [],
        "holdings_status": [],
        "market_summary": {"twse_count": twse_n, "otc_count": otc_n, "scan_count": 100},
    }
    write_gist(DATA_GIST, "scan_results.json", scan_results)

    # 7. Backtest extension
    bt_data = data_gist.get("backtest_results.json", {})
    if bt_data and bt_data.get("trades"):
        bt_end = bt_data["stats"].get("end_date", "")
        if trading_date > bt_end:
            print(f"Extending backtest from {bt_end} to {trading_date}...")
            sp = params
            max_pos = int(sp.get("max_positions", 2))

            sim_holdings = [dict(t) for t in bt_data["trades"] if t.get("reason") == "持有中"]
            bt_trades = [t for t in bt_data["trades"] if t.get("reason") != "持有中"]

            # Sell check
            new_h = []
            for h_item in sim_holdings:
                tk = h_item.get("ticker", "")
                if tk not in market_data:
                    new_h.append(h_item); continue
                bp = h_item["buy_price"]; cur = market_data[tk]["close"]
                ret = (cur / bp - 1) * 100 if bp > 0 else 0
                # Approximate trading days
                try:
                    bd = date.fromisoformat(h_item["buy_date"])
                    dh = max(0, int((date.fromisoformat(trading_date) - bd).days * 5 / 7))
                except:
                    dh = 0
                pk = max(h_item.get("peak_price", bp), cur); h_item["peak_price"] = pk
                reason = None
                if dh < 1: new_h.append(h_item); continue
                # Bug fix: breakeven 保本出場（對齊 GPU kernel + scanner.py）
                eff_stop = sp.get("stop_loss", -20)
                peak_g = (pk / bp - 1) * 100 if bp > 0 else 0
                if sp.get("use_breakeven", 0) and peak_g >= sp.get("breakeven_trigger", 20):
                    eff_stop = 0
                if ret <= eff_stop:
                    reason = f"保本 {ret:+.1f}%" if eff_stop == 0 else f"停損 {ret:+.1f}%"
                if not reason and sp.get("use_take_profit", 1) and ret >= sp.get("take_profit", 80): reason = f"停利 +{ret:.1f}%"
                if not reason and sp.get("trailing_stop", 0) > 0 and pk > bp * 1.01:
                    if (cur / pk - 1) * 100 <= -sp["trailing_stop"]: reason = "移動停利"
                if not reason and int(sp.get("sell_below_ma", 0)) > 0 and tk in cache:
                    cs_c = cache[tk]["c"] if tk in cache else []
                    if len(cs_c) > 60:
                        ma60 = sum(cs_c[-61:-1]) / 60
                        if bp >= ma60 and cur < ma60: reason = "跌破MA60"
                if not reason and sp.get("use_stagnation_exit", 0):
                    stag_d = int(sp.get("stagnation_days", 10))
                    stag_min = sp.get("stagnation_min_ret", 5)
                    if dh >= stag_d and ret < stag_min: reason = "停滯出場"
                if not reason and sp.get("use_time_decay", 0):
                    hh = int(sp.get("hold_days", 30)) // 2
                    if dh >= hh and ret < (dh - hh) * sp.get("ret_per_day", 0.5): reason = "漸進停利"
                if not reason and sp.get("use_profit_lock", 0):
                    pg = (pk / bp - 1) * 100
                    if pg >= sp.get("lock_trigger", 30) and ret < sp.get("lock_floor", 10): reason = "鎖利"
                if not reason and dh >= int(sp.get("hold_days", 30)): reason = f"到期{dh}天 {ret:+.1f}%"
                if reason:
                    bt_trades.append({"ticker": tk, "name": h_item.get("name", ""), "buy_price": round(bp, 2),
                                      "sell_price": round(cur, 2), "hold_days": dh, "return_pct": round(ret, 1),
                                      "reason": reason, "buy_date": h_item["buy_date"], "sell_date": trading_date})
                    print(f"  SELL {h_item.get('name', '')} {reason}")
                else:
                    new_h.append(h_item)
            sim_holdings = new_h

            # Buy check (exclude stocks just sold today)
            _just_sold = {t["ticker"] for t in bt_trades if t.get("sell_date") == trading_date}
            if len(sim_holdings) < max_pos and signals:
                held_tks = {h_item["ticker"] for h_item in sim_holdings} | _just_sold
                for sig in signals:
                    if sig["ticker"] not in held_tks:
                        sim_holdings.append({
                            "ticker": sig["ticker"], "name": sig["name"],
                            "buy_price": sig["close"], "buy_date": trading_date,
                            "peak_price": sig["close"], "sell_price": sig["close"],
                            "hold_days": 0, "return_pct": 0, "reason": "持有中",
                        })
                        print(f"  BUY {sig['name']} {sig['score']}分")
                        break  # Only buy #1 per day (matching GPU)

            # Update holdings prices + hold_days
            for h_item in sim_holdings:
                tk = h_item["ticker"]
                if tk in market_data:
                    cur = market_data[tk]["close"]
                    h_item["sell_price"] = round(cur, 2)
                    h_item["return_pct"] = round((cur / h_item["buy_price"] - 1) * 100, 1) if h_item["buy_price"] > 0 else 0
                    try:
                        bd = date.fromisoformat(h_item["buy_date"])
                        h_item["hold_days"] = max(0, int((date.fromisoformat(trading_date) - bd).days * 5 / 7))
                    except:
                        pass

            all_trades = sorted(bt_trades + sim_holdings, key=lambda t: t.get("buy_date", ""))
            bt_data["trades"] = all_trades
            bt_data["stats"]["end_date"] = trading_date
            write_gist(DATA_GIST, "backtest_results.json", bt_data)
            print(f"  Backtest: {len(bt_trades)} completed + {len(sim_holdings)} holding")

    print(f"Done! [{datetime.now(TW_TZ).strftime('%H:%M')}]")


if __name__ == "__main__":
    main()
