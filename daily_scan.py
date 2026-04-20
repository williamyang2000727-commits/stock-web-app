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
                # 新加：存 open 價（修 consecutive_green + gap_up 歷史限制）
                # 舊快取沒有 o 陣列，從今天開始每天累加（~80 天後全部填滿）
                hist["o"] = (hist.get("o") or [])[-79:] + [info.get("open", info["close"])]
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
        o_arr = np.array(cs["o"], dtype=np.float64) if cs.get("o") and len(cs["o"]) == len(cs["c"]) else None
        ind = compute_indicators_with_state(c, h, lo, v, states[tk], o=o_arr)
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

            # 統一到 trading_days 模組（禁止在這裡自己寫 *5/7 近似）
            from trading_days import count_between
            _any_stock = next(iter(cache.values()), {})
            _fallback_cal = _any_stock.get("dates", [])

            # D+1 執行日（GPU 的 sell/buy 都在 D+1，不是 D 當天）
            from datetime import date as _dt, timedelta as _td
            _td_parsed = _dt.fromisoformat(trading_date)
            _next_td = _td_parsed + _td(days=1)
            while _next_td.weekday() >= 5:
                _next_td += _td(days=1)
            _exec_date = str(_next_td)  # D+1 = 實際執行日

            # 理由格式統一（sell_rules 回傳帶 % 的詳細字串，GPU 用乾淨名稱）
            def _clean_reason(r):
                for _pf, _cl in [("移動停利","移動停利"),("保本","保本出場"),("停損","停損"),
                                  ("停利","停利"),("跌破","跌破均線"),("停滯","停滯出場"),
                                  ("漸進","漸進停利"),("鎖利","鎖利出場"),("動量","動量反轉"),
                                  ("到期","到期"),("RSI","RSI超買"),("MACD","MACD死叉"),
                                  ("KD","KD死叉"),("量能","量縮")]:
                    if r.startswith(_pf):
                        return _cl
                return r

            # Sell check（D 日收盤檢查，D+1 執行）
            new_h = []
            for h_item in sim_holdings:
                tk = h_item.get("ticker", "")
                if tk not in market_data:
                    new_h.append(h_item); continue
                bp = h_item["buy_price"]; cur = market_data[tk]["close"]
                ret = (cur / bp - 1) * 100 if bp > 0 else 0
                bd_str = h_item.get("buy_date", "")
                dh = count_between(bd_str, trading_date, fallback_calendar=_fallback_cal)
                pk = max(h_item.get("peak_price", bp), cur); h_item["peak_price"] = pk
                if dh < 1: new_h.append(h_item); continue
                from sell_rules import should_sell
                cache_c = list(cache[tk]["c"]) if tk in cache else None
                if cache_c is not None and tk in market_data:
                    cache_c = cache_c + [market_data[tk]["close"]]
                reason = should_sell(bp, cur, pk, dh, sp, cache_closes=cache_c, indicators=None)
                if reason:
                    reason = _clean_reason(reason)
                    bt_trades.append({"ticker": tk, "name": h_item.get("name", ""), "buy_price": round(bp, 2),
                                      "sell_price": round(cur, 2), "hold_days": dh, "return_pct": round(ret, 1),
                                      "reason": reason, "buy_date": h_item["buy_date"], "sell_date": _exec_date})
                    print(f"  SELL {h_item.get('name', '')} {reason}")
                else:
                    new_h.append(h_item)
            sim_holdings = new_h

            # Buy check（用「前一天」的掃描候選，不是今天的 — 跟 GPU 一致）
            # GPU: Day D 掃描 #1 → Day D+1 買入。daily_scan 在 D+1 跑，
            # 所以該用 D（昨天 = Gist 上還沒被覆蓋的 scan_results）的 #1。
            _prev_scan = data_gist.get("scan_results.json", {})
            _prev_signals = _prev_scan.get("buy_signals", [])
            _buy_candidates = _prev_signals if _prev_signals else signals  # fallback 到今天
            _buy_source = "prev" if _prev_signals else "today"
            _just_sold = {t["ticker"] for t in bt_trades if t.get("sell_date") == _exec_date}
            if len(sim_holdings) < max_pos and _buy_candidates:
                held_tks = {h_item["ticker"] for h_item in sim_holdings} | _just_sold
                for sig in _buy_candidates:
                    if sig["ticker"] not in held_tks:
                        # 買入價用今天的收盤（D+1 close，最接近實際執行價）
                        _buy_price = sig["close"]
                        if sig["ticker"] in market_data:
                            _buy_price = market_data[sig["ticker"]]["close"]
                        sim_holdings.append({
                            "ticker": sig["ticker"], "name": sig["name"],
                            "buy_price": _buy_price, "buy_date": _exec_date,
                            "peak_price": _buy_price, "sell_price": _buy_price,
                            "hold_days": 0, "return_pct": 0, "reason": "持有中",
                        })
                        print(f"  BUY {sig['name']} {sig.get('score','')}分 (src={_buy_source}, exec={_exec_date})")
                        break  # Only buy #1 per day (matching GPU)

            # Update holdings prices + hold_days（用精確交易日計數）
            for h_item in sim_holdings:
                tk = h_item["ticker"]
                if tk in market_data:
                    cur = market_data[tk]["close"]
                    h_item["sell_price"] = round(cur, 2)
                    h_item["return_pct"] = round((cur / h_item["buy_price"] - 1) * 100, 1) if h_item["buy_price"] > 0 else 0
                    bd_str2 = h_item.get("buy_date", "")
                    h_item["hold_days"] = count_between(bd_str2, trading_date, fallback_calendar=_fallback_cal)

            all_trades = sorted(bt_trades + sim_holdings, key=lambda t: t.get("buy_date", ""))
            bt_data["trades"] = all_trades
            # Bug fix: 重算 stats（之前只更新 end_date，total_return_pct 會 stale）
            _completed_refresh = [t for t in all_trades if t.get("reason") != "持有中"]
            _rets_refresh = [t.get("return_pct", 0) for t in _completed_refresh]
            _wins_refresh = [r for r in _rets_refresh if r > 0]
            _losses_refresh = [r for r in _rets_refresh if r <= 0]
            bt_data["stats"]["end_date"] = trading_date
            bt_data["stats"]["total_trades"] = len(_completed_refresh)
            bt_data["stats"]["total_return_pct"] = round(sum(_rets_refresh), 1)
            bt_data["stats"]["win_rate"] = round(len(_wins_refresh) / len(_rets_refresh) * 100, 1) if _rets_refresh else 0
            bt_data["stats"]["avg_return"] = round(sum(_rets_refresh) / len(_rets_refresh), 1) if _rets_refresh else 0
            bt_data["stats"]["avg_win"] = round(sum(_wins_refresh) / len(_wins_refresh), 1) if _wins_refresh else 0
            bt_data["stats"]["avg_loss"] = round(sum(_losses_refresh) / len(_losses_refresh), 1) if _losses_refresh else 0
            bt_data["stats"]["max_win"] = round(max(_rets_refresh), 1) if _rets_refresh else 0
            bt_data["stats"]["max_loss"] = round(min(_rets_refresh), 1) if _rets_refresh else 0
            bt_data["stats"]["avg_hold_days"] = round(sum(t.get("hold_days", 0) for t in _completed_refresh) / len(_completed_refresh), 1) if _completed_refresh else 0
            write_gist(DATA_GIST, "backtest_results.json", bt_data)
            print(f"  Backtest: {len(_completed_refresh)} completed + {len(sim_holdings)} holding | 總報酬 {bt_data['stats']['total_return_pct']}% 勝率 {bt_data['stats']['win_rate']}%")

    print(f"Done! [{datetime.now(TW_TZ).strftime('%H:%M')}]")


if __name__ == "__main__":
    main()
