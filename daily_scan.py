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


def write_gist(gist_id, filename, data, max_retry=3):
    """Write Gist with retry. Returns True on success, raises RuntimeError on persistent failure.

    重要：state Gist + history cache 必須原子性寫入，失敗會導致 state vs cache 失同步
    → 下次 daily_scan 把今天 K 重複加進 state（double-update），EMA/ATR/ADX 永久偏差。
    所以失敗時 raise，讓 GitHub Actions workflow 標記 fail，下次重跑會走完整流程。
    """
    import time as _t
    payload = {"files": {filename: {"content": json.dumps(data, ensure_ascii=False)}}}
    last_err = None
    for attempt in range(max_retry):
        try:
            r = requests.patch(f"https://api.github.com/gists/{gist_id}", headers=HEADERS, json=payload, timeout=60)
            if r.status_code == 200:
                return True
            last_err = f"status={r.status_code} body={r.text[:200]}"
        except Exception as e:
            last_err = str(e)
        if attempt < max_retry - 1:
            _t.sleep(2 ** attempt)  # 1s, 2s, 4s exponential backoff
    raise RuntimeError(f"write_gist({filename}) failed after {max_retry} attempts: {last_err}")


def fetch_market_data():
    """Fetch all stocks from TWSE + TPEx with retry (each up to 3 attempts)."""
    import urllib3, time as _time
    urllib3.disable_warnings()
    all_data = {}
    today = datetime.now(TW_TZ)
    date_ad = today.strftime("%Y%m%d")
    date_roc = f"{today.year - 1911}/{today.month:02d}/{today.day:02d}"
    trading_date = today.strftime("%Y-%m-%d")

    for _attempt in range(3):
        try:
            r = requests.get(f"https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json&date={date_ad}",
                             timeout=20, verify=False, headers={"User-Agent": "Mozilla/5.0"})
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
            if len([k for k in all_data if ".TW" in k and ".TWO" not in k]) >= 500:
                break
        except:
            pass
        if _attempt < 2:
            _time.sleep(3)
            print(f"  TWSE retry {_attempt+2}/3...")

    for _attempt in range(3):
        try:
            r = requests.get("https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php",
                             params={"l": "zh-tw", "d": date_roc}, headers={"User-Agent": "Mozilla/5.0"},
                             timeout=20, verify=False)
            _otc_count = 0
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
                            _otc_count += 1
                    except:
                        continue
            if _otc_count >= 200:
                break
        except:
            pass
        if _attempt < 2:
            _time.sleep(3)
            print(f"  TPEx retry {_attempt+2}/3...")

    return all_data, trading_date


def fetch_ex_dividend_window():
    """Fetch TWSE 上市 ex-dividend schedule via TWT48U (預告表).
    TWT48U returns ~30 upcoming ex-dividend events; each row's row[0] carries
    its own ROC date (e.g. "115年04月23日"), so one call gives us a window of
    ~2 months forward. GitHub Actions runner can reach TWSE (unlike Streamlit
    Cloud US egress) — that's why the cron writes the cache to Data Gist.
    TPEx (.TWO) openapi is broken as of 2026-04 and not covered.
    Returns dict {"YYYY-MM-DD": ["1217", ...]}; empty dict on failure."""
    import time as _time, re as _re
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.twse.com.tw/",
    }
    result = {}
    for _attempt in range(3):
        try:
            r = requests.get(
                "https://www.twse.com.tw/exchangeReport/TWT48U",
                params={"response": "json"}, headers=headers, timeout=15,
            )
            if r.status_code != 200:
                _time.sleep(2); continue
            j = r.json()
            if j.get("stat") != "OK":
                _time.sleep(2); continue
            for row in j.get("data") or []:
                if len(row) < 2:
                    continue
                m = _re.match(r"(\d+)年(\d+)月(\d+)日", row[0])
                if not m:
                    continue
                yr = int(m.group(1)) + 1911
                mo, dy = int(m.group(2)), int(m.group(3))
                date_str = f"{yr:04d}-{mo:02d}-{dy:02d}"
                result.setdefault(date_str, []).append(str(row[1]).strip())
            break
        except Exception:
            _time.sleep(2)
    return result


# Import shared indicator + scoring from scanner (single source of truth)
# NOTE: must be able to import scanner module; this file runs in same dir
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scanner import compute_indicators, compute_indicators_with_state, score_stock


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
                hist["o"] = (hist.get("o") or [])[-79:] + [info.get("open", info["close"])]
                # h250/l250: 250 天 high/low（精準 week52_pos）
                hist["h250"] = (hist.get("h250") or hist["h"])[-249:] + [info.get("high", info["close"])]
                hist["l250"] = (hist.get("l250") or hist["l"])[-249:] + [info.get("low", info["close"])]
                # 清理舊的 per-stock dates（改存頂層，省 2 MB）
                hist.pop("dates", None)
        # dates 存一份在頂層（所有股票共用同一組交易日）
        history["dates"] = (history.get("dates") or [])[-79:] + [trading_date]
        history["updated"] = trading_date
        write_gist(HISTORY_GIST, "history_cache.json", history)
        print(f"  Updated {len(cache)} stocks")

    # 5. Buy ranking scan
    print("Scanning buy signals...")
    top100 = sorted(market_data.keys(), key=lambda t: market_data[t]["vol"], reverse=True)[:100]
    buy_th = params.get("buy_threshold", 10)
    signals = []
    for tk in top100:
        if tk not in cache: continue  # FIX M7: don't require states (fallback to compute_indicators)
        cs = cache[tk]
        c = np.array(cs["c"], dtype=np.float64)
        h = np.array(cs["h"], dtype=np.float64)
        lo = np.array(cs["l"], dtype=np.float64)
        v = np.array(cs["v"], dtype=np.float64)
        if len(c) < 20: continue
        o_arr = np.array(cs["o"], dtype=np.float64) if cs.get("o") and len(cs["o"]) == len(cs["c"]) else None
        h250 = np.array(cs["h250"], dtype=np.float64) if cs.get("h250") else None
        l250 = np.array(cs["l250"], dtype=np.float64) if cs.get("l250") else None
        if tk in states:
            ind = compute_indicators_with_state(c, h, lo, v, states[tk], o=o_arr, h250=h250, l250=l250)
        else:
            ind = compute_indicators(c, h, lo, v, o=o_arr, h250=h250, l250=l250)
        if ind is None: continue
        # Gap % — 今天 open vs 昨天 close（cache 已 append 今天，所以昨天在 [-2]）
        _td_info = market_data.get(tk, {})
        if _td_info.get("open") and len(cs["c"]) > 1:
            _prev_c = cs["c"][-2]  # FIX C2: cache 已更新，[-1]=今天，[-2]=昨天
            ind["gap_pct"] = float((_td_info["open"] / _prev_c - 1) * 100) if _prev_c > 0 else 0.0
        else:
            ind["gap_pct"] = 0.0
        sc = score_stock(ind, params)
        if sc >= buy_th:
            signals.append({"rank": 0, "ticker": tk, "name": market_data[tk].get("name", tk),
                            "score": sc, "close": market_data[tk]["close"],
                            "vol_ratio": round(ind["vol_ratio"], 1)})
    # 三層排序：分數 > vol_ratio > ticker（ticker 正向=小代碼優先，確定性）
    signals.sort(key=lambda x: (-x["score"], -x["vol_ratio"], x.get("ticker", "")))
    for i, s in enumerate(signals): s["rank"] = i + 1
    print(f"  {len(signals)} signals, #1: {signals[0]['name'] if signals else 'none'}")

    # 6. Update scan_results
    twse_n = len([k for k in market_data if ".TW" in k and ".TWO" not in k])
    otc_n = len([k for k in market_data if ".TWO" in k])
    # Preserve previous pending if Phase A+B doesn't run (defensive: >= should always enter,
    # but if bt_data is missing/empty, pending from previous run must survive)
    _prev_scan = data_gist.get("scan_results.json", {})
    scan_results = {
        "date": trading_date,
        "timestamp": datetime.now(TW_TZ).isoformat(),
        "strategy_version": "auto",
        "strategy_score": "auto",
        "buy_signals": signals[:20],
        "sell_signals": [],
        "holdings_status": [],
        "market_summary": {"twse_count": twse_n, "otc_count": otc_n, "scan_count": 100},
        "pending_sells": _prev_scan.get("pending_sells", []),
        "pending_buy": _prev_scan.get("pending_buy"),
    }

    # 7. Backtest extension (2-phase pending mechanism)
    bt_data = data_gist.get("backtest_results.json", {})
    if bt_data and bt_data.get("trades"):
        bt_end = bt_data["stats"].get("end_date", "")
        if trading_date >= bt_end:  # >= not > : always run Phase A+B even if bt already current
            print(f"Extending backtest from {bt_end} to {trading_date}...")
            sp = params
            max_pos = int(sp.get("max_positions", 2))

            sim_holdings = [dict(t) for t in bt_data["trades"] if t.get("reason") == "持有中"]
            bt_trades = [t for t in bt_data["trades"] if t.get("reason") != "持有中"]

            from trading_days import count_between
            _fallback_cal = history.get("dates", [])

            def _clean_reason(r):
                for _pf, _cl in [("移動停利","移動停利"),("保本","保本出場"),("停損","停損"),
                                  ("停利","停利"),("跌破","跌破均線"),("停滯","停滯出場"),
                                  ("漸進","漸進停利"),("鎖利","鎖利出場"),("動量","動量反轉"),
                                  ("到期","到期"),("RSI","RSI超買"),("MACD","MACD死叉"),
                                  ("KD","KD死叉"),("量能","量縮")]:
                    if r.startswith(_pf):
                        return _cl
                return r

            # === Phase A: Execute YESTERDAY's pending (with TODAY's actual prices) ===
            prev_scan = data_gist.get("scan_results.json", {})
            _pending_sells = prev_scan.get("pending_sells", [])
            _pending_buy = prev_scan.get("pending_buy", None)

            # GUARD: If scan_results.date == today, a previous run already executed Phase A.
            # The pending in scan_results is for TOMORROW, not today. Skip to avoid duplicates.
            _prev_scan_date = prev_scan.get("date", "")
            if _prev_scan_date == trading_date:
                print(f"  Phase A SKIP: scan already from today ({trading_date}), pending is for tomorrow")
                _pending_sells = []
                _pending_buy = None

            if _pending_sells:
                for ps in _pending_sells:
                    tk = ps["ticker"]
                    for i, h in enumerate(sim_holdings):
                        if h["ticker"] == tk and tk in market_data:
                            cur = market_data[tk].get("open", market_data[tk]["close"])  # FIX C1: GPU sells at D+1 open
                            ret = (cur / h["buy_price"] - 1) * 100 - 0.585 if h["buy_price"] > 0 else 0  # match GPU: subtract transaction cost
                            dh = count_between(h.get("buy_date", ""), trading_date, fallback_calendar=_fallback_cal)
                            bt_trades.append({
                                "ticker": tk, "name": h.get("name", ""),
                                "buy_price": round(h["buy_price"], 2),
                                "sell_price": round(cur, 2), "hold_days": dh,
                                "return_pct": round(ret, 1),
                                "reason": ps["reason"],
                                "buy_date": h["buy_date"], "sell_date": trading_date})
                            sim_holdings.pop(i)
                            print(f"  EXEC SELL {h.get('name', '')} {ps['reason']} @{cur}")
                            break

            if _pending_buy and len(sim_holdings) < max_pos:
                tk = _pending_buy["ticker"]
                _sold_today = {t["ticker"] for t in bt_trades if t.get("sell_date") == trading_date}
                _held_tks = {h["ticker"] for h in sim_holdings}
                if tk not in _sold_today and tk not in _held_tks and tk in market_data:  # FIX M2: no duplicates
                    cur = market_data[tk]["close"]
                    sim_holdings.append({
                        "ticker": tk, "name": _pending_buy.get("name", ""),
                        "buy_price": cur, "buy_date": trading_date,
                        "peak_price": cur, "sell_price": cur,
                        "hold_days": 0, "return_pct": 0, "reason": "持有中"})
                    print(f"  EXEC BUY {_pending_buy.get('name', '')} @{cur}")

            # === Phase B: Generate TODAY's pending (for tomorrow) ===
            _new_pending_sells = []
            _new_pending_buy = None

            from sell_rules import should_sell
            for h in sim_holdings:
                tk = h.get("ticker", "")
                if tk not in market_data: continue
                bp = h["buy_price"]; cur = market_data[tk]["close"]
                dh = count_between(h.get("buy_date", ""), trading_date, fallback_calendar=_fallback_cal)
                pk = max(h.get("peak_price", bp), cur); h["peak_price"] = pk
                if dh < 1: continue
                cache_c = list(cache[tk]["c"]) if tk in cache else None
                if cache_c is not None and tk in market_data:
                    # NOTE: cache already has today (step 4). Append again so that
                    # should_sell's MA60 slice [-61:-1] includes today (last dup excluded).
                    cache_c = cache_c + [market_data[tk]["close"]]
                # Compute indicators if strategy uses indicator-based sell conditions
                _ind = None
                if tk in cache and any(sp.get(k, 0) for k in ("use_rsi_sell", "use_macd_sell", "use_kd_sell", "sell_vol_shrink", "use_mom_exit")):
                    try:
                        _cs = cache[tk]
                        _c = np.array(_cs["c"], dtype=np.float64)
                        _h = np.array(_cs["h"], dtype=np.float64)
                        _l = np.array(_cs["l"], dtype=np.float64)
                        _v = np.array(_cs["v"], dtype=np.float64)
                        _o = np.array(_cs["o"], dtype=np.float64) if _cs.get("o") and len(_cs["o"]) == len(_cs["c"]) else None
                        _h250_s = np.array(_cs["h250"], dtype=np.float64) if _cs.get("h250") else None
                        _l250_s = np.array(_cs["l250"], dtype=np.float64) if _cs.get("l250") else None
                        if len(_c) >= 20:
                            _ind = compute_indicators_with_state(_c, _h, _l, _v, states[tk], o=_o, h250=_h250_s, l250=_l250_s) if tk in states else compute_indicators(_c, _h, _l, _v, o=_o, h250=_h250_s, l250=_l250_s)
                    except Exception:
                        pass
                reason = should_sell(bp, cur, pk, dh, sp, cache_closes=cache_c, indicators=_ind)
                if reason:
                    _new_pending_sells.append({
                        "ticker": tk, "name": h.get("name", ""),
                        "reason": _clean_reason(reason)})

            # 有空位就找買入候選（不管是賣出騰出的還是本來就空的）
            _slots_freeing = len(_new_pending_sells)
            _holdings_after = len(sim_holdings) - _slots_freeing
            if _holdings_after < max_pos and signals:
                _sold_tks = {ps["ticker"] for ps in _new_pending_sells}
                _held_tks = {h["ticker"] for h in sim_holdings} - _sold_tks
                for sig in signals:
                    if sig["ticker"] not in _held_tks and sig["ticker"] not in _sold_tks:
                        _new_pending_buy = {
                            "ticker": sig["ticker"], "name": sig["name"],
                            "score": sig.get("score", 0), "close": sig["close"]}
                        break

            # Save pending in scan_results (for tomorrow)
            scan_results["pending_sells"] = _new_pending_sells
            scan_results["pending_buy"] = _new_pending_buy
            if _new_pending_sells:
                print(f"  PENDING SELL: {[ps['name'] for ps in _new_pending_sells]}")
            if _new_pending_buy:
                print(f"  PENDING BUY: {_new_pending_buy['name']} (tomorrow)")

            # Update holdings prices (no sell/buy on today, just price refresh)
            for h_item in sim_holdings:
                tk = h_item["ticker"]
                if tk in market_data:
                    cur = market_data[tk]["close"]
                    h_item["sell_price"] = round(cur, 2)
                    h_item["return_pct"] = round((cur / h_item["buy_price"] - 1) * 100, 1) if h_item["buy_price"] > 0 else 0
                    h_item["hold_days"] = count_between(h_item.get("buy_date", ""), trading_date, fallback_calendar=_fallback_cal)

            # ⚠️ 不再寫 backtest_results.json（17:00 Windows pipeline rebuild_tab3 會用
            # cpu_replay 1500 天真公式重建，蓋過去）
            # 這裡 Phase A/B 仍然要跑（為了算 scan_results 的 pending），但不寫 Gist
            # 避免 16:35-17:00 之間 Tab 3 看到 80 天版失真資料
            print(f"  Phase A/B 完成（{len(sim_holdings)} 檔持有中）— backtest_results.json 由 17:00 pipeline 重建，daily_scan 不寫")

    # Write scan_results AFTER step 7 (includes pending fields)
    write_gist(DATA_GIST, "scan_results.json", scan_results)

    # 8. Refresh ex-dividend cache for Web (Streamlit Cloud can't reach TWSE)
    try:
        _ex_new = fetch_ex_dividend_window()
        if _ex_new:
            # Merge with existing Gist content to preserve historical dates.
            # TWT48U only returns ~2 months forward, so past dates must survive
            # via the cumulative cache. Drop cache entries older than 60 days.
            _ex_old = (data_gist.get("ex_dividend.json") or {}).get("tickers_by_date", {})
            _cutoff = (datetime.now(TW_TZ).date() - timedelta(days=60)).isoformat()
            _merged = {d: tks for d, tks in _ex_old.items() if d >= _cutoff}
            _merged.update(_ex_new)  # new data overrides same-date old entries
            write_gist(DATA_GIST, "ex_dividend.json", {
                "updated": datetime.now(TW_TZ).isoformat(timespec="seconds"),
                "tickers_by_date": _merged,
            })
            _today_tk = _merged.get(trading_date, [])
            print(f"  Ex-dividend cache: {len(_merged)} days (new {len(_ex_new)}, today {trading_date}: {len(_today_tk)} stocks)")
    except Exception as _e:
        print(f"  Ex-dividend fetch failed (non-fatal): {_e}")

    print(f"Done! [{datetime.now(TW_TZ).strftime('%H:%M')}]")


if __name__ == "__main__":
    main()
