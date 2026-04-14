"""
Yang's 選股系統 Web App
Taiwan stock selection system - Streamlit dashboard
"""

import streamlit as st
import requests
import json
import pandas as pd
from datetime import datetime, date, timedelta, timezone
import hashlib

# 台灣時區 (UTC+8)
TW_TZ = timezone(timedelta(hours=8))
def tw_now():
    return datetime.now(TW_TZ)
def tw_today():
    return tw_now().date()

# ── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="Yang's 選股系統",
    page_icon="📈",    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Secrets ──────────────────────────────────────────────────
GITHUB_TOKEN = st.secrets["github_token"]
DATA_GIST_ID = st.secrets["data_gist_id"]
HISTORY_GIST_ID = st.secrets.get("history_gist_id", DATA_GIST_ID)
GPU_GIST_ID = "c1bef892d33589baef2142ce250d18c2"  # GPU evolution pushes here


# ── Gist I/O ────────────────────────────────────────────────
@st.cache_data(ttl=300)
def _read_gist(gist_id):
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        r = requests.get(f"https://api.github.com/gists/{gist_id}", headers=headers, timeout=15)
        if r.status_code == 200:
            result = {}
            for fname, fdata in r.json().get("files", {}).items():
                # Large files (>1MB) are truncated by Gist API, fetch via raw_url
                if fdata.get("truncated"):
                    try:
                        raw = requests.get(fdata["raw_url"], headers=headers, timeout=30)
                        result[fname] = json.loads(raw.text)
                    except Exception:
                        result[fname] = {}
                else:
                    try:
                        result[fname] = json.loads(fdata["content"])
                    except (json.JSONDecodeError, KeyError):
                        result[fname] = {}
            return result
    except Exception:
        pass
    return {}


def read_gist_file(filename):
    return _read_gist(DATA_GIST_ID).get(filename, {})


def write_gist_file(filename, data_dict, clear_cache=False):
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    payload = {"files": {filename: {"content": json.dumps(data_dict, ensure_ascii=False, indent=2)}}}
    try:
        r = requests.patch(f"https://api.github.com/gists/{DATA_GIST_ID}",
                           headers=headers, json=payload, timeout=30)
        if r.status_code == 200:
            if clear_cache:
                st.cache_data.clear()
            return True
    except Exception:
        pass
    return False


# ── Authentication ───────────────────────────────────────────
def authenticate():
    if st.session_state.get("authenticated"):
        return True
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("# 📈 Yang's 選股系統")
        st.caption("Taiwan Stock Selection System")
        st.markdown("---")
        with st.form("login"):
            username = st.text_input("帳號").strip().lower()
            password = st.text_input("密碼", type="password")
            if st.form_submit_button("登入", use_container_width=True):
                if username and password:
                    users = dict(st.secrets.get("users", {}))
                    if username in users:
                        if hashlib.sha256(password.encode()).hexdigest() == users[username]:
                            st.session_state.authenticated = True
                            st.session_state.username = username
                            st.rerun()
                        else:
                            st.error("密碼錯誤")
                    else:
                        st.error("帳號不存在")
    return False


# ── Market Data (cached, no Mac dependency) ──────────────────
@st.cache_data(ttl=1800, show_spinner="正在抓取市場資料...")
def get_market_data():
    from scanner import fetch_market_data
    return fetch_market_data()


# ── Live Scan (每次登入都跑，session 內快取) ─────────────────


# ── Helper ───────────────────────────────────────────────────
def next_trading_day(scan_date_str, cal=None):
    try:
        d = date.fromisoformat(scan_date_str)
        # Use trading calendar if available (skips holidays)
        if cal:
            future = sorted(td for td in cal if td > d)
            if future:
                return future[0]
        # Fallback: skip weekends only
        nd = d + timedelta(days=1)
        while nd.weekday() >= 5:
            nd += timedelta(days=1)
        return nd
    except (ValueError, TypeError):
        return tw_today()


def save_user_holdings(username, holdings, clear_cache=True):
    portfolios = read_gist_file("portfolios.json")
    if not isinstance(portfolios, dict):
        portfolios = {}
    portfolios[username] = {"holdings": holdings, "updated": tw_now().isoformat()}
    return write_gist_file("portfolios.json", portfolios, clear_cache=clear_cache)


# ══════════════════════════════════════════════════════════════
# MAIN APP
# ══════════════════════════════════════════════════════════════
if not authenticate():
    st.stop()

username = st.session_state.username

# ── Sidebar ──
with st.sidebar:
    st.markdown(f"### 👤 {username}")
    st.markdown(f"📅 {tw_today().strftime('%Y/%m/%d')}")
    st.markdown("---")
    if st.button("🔄 重新整理", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    if st.button("🚪 登出", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()
    st.markdown("---")
    st.caption("📈 Yang's 選股系統 v1.0")

# ── Load Strategy (auto-sync from GPU Gist) ──
@st.cache_data(ttl=300)
def _read_gpu_strategy():
    """Read latest strategy directly from GPU evolution Gist."""
    try:
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        r = requests.get(f"https://api.github.com/gists/{GPU_GIST_ID}", headers=headers, timeout=15)
        if r.status_code == 200:
            fdata = list(r.json().get("files", {}).values())[0]
            content = fdata.get("content", "{}")
            strategy = json.loads(content)
            return strategy.get("params", {})
    except Exception:
        pass
    return {}

strategy_params = _read_gpu_strategy()
if not strategy_params:
    strategy_params = read_gist_file("strategy_params.json")  # fallback
portfolios = read_gist_file("portfolios.json")
user_holdings = portfolios.get(username, {}).get("holdings", []) if isinstance(portfolios, dict) else []
held_tickers = tuple(h.get("ticker", "") for h in user_holdings)

# ── Market Data (TWSE/TPEx, no Mac) ──
try:
    market_data, trading_date = get_market_data()
except Exception:
    market_data, trading_date = {}, ""

# ── History Cache (separate Gist for large file) ──
@st.cache_data(ttl=300)
def _read_history_gist():
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        r = requests.get(f"https://api.github.com/gists/{HISTORY_GIST_ID}", headers=headers, timeout=15)
        if r.status_code == 200:
            fdata = list(r.json().get("files", {}).values())[0]
            if fdata.get("truncated"):
                raw = requests.get(fdata["raw_url"], headers=headers, timeout=60)
                return json.loads(raw.text)
            return json.loads(fdata.get("content", "{}"))
    except Exception:
        pass
    return {}

history_cache = _read_history_gist()
cache_date = history_cache.get("updated", "") if history_cache else ""

# ── Indicator States ──
indicator_states = read_gist_file("indicator_state.json")
state_date = indicator_states.get("updated", "") if indicator_states else ""

# ── Update states + cache if new trading day (MUST update states BEFORE cache) ──
if indicator_states and market_data and trading_date and trading_date > state_date:
    import numpy as np
    _states = indicator_states.get("states", {})
    _pre_cache = history_cache.get("stocks", {}) if history_cache else {}  # PRE-mutation cache
    _updated = False
    for _tk, _sv in _states.items():  # FIX #5: renamed from 'st' to '_sv' (avoid shadowing streamlit)
        if _tk not in market_data or _tk not in _pre_cache:
            continue
        _info = market_data[_tk]
        _cs = _pre_cache[_tk]
        if not _cs.get("c"):
            continue
        _prev_c = _cs["c"][-1]  # FIX #14: read BEFORE cache mutation (below)
        _new_c = _info["close"]
        _ch = _new_c - _prev_c
        # RSI
        _sv["rsi_ag"] = round((_sv["rsi_ag"] * 13 + max(_ch, 0)) / 14, 6)
        _sv["rsi_al"] = round((_sv["rsi_al"] * 13 + max(-_ch, 0)) / 14, 6)
        # MACD
        _sv["ema12"] = round(_sv["ema12"] * (1 - 2/13) + _new_c * 2/13, 4)
        _sv["ema26"] = round(_sv["ema26"] * (1 - 2/27) + _new_c * 2/27, 4)
        _new_ml = _sv["ema12"] - _sv["ema26"]
        _sv["mh_prev"] = _sv["mh"]
        _sv["macd_sig"] = round(_sv["macd_sig"] * (1 - 2/10) + _new_ml * 2/10, 4)
        _sv["mh"] = round(_new_ml - _sv["macd_sig"], 4)
        # ATR
        _new_tr = max(_info.get("high", _new_c) - _info.get("low", _new_c),
                      abs(_info.get("high", _new_c) - _prev_c),
                      abs(_info.get("low", _new_c) - _prev_c))
        _sv["atr14"] = round((_sv["atr14"] * 13 + _new_tr) / 14, 4)
        # ADX
        _prev_h = _cs["h"][-1] if _cs.get("h") else _new_c
        _prev_l = _cs["l"][-1] if _cs.get("l") else _new_c
        _up = _info.get("high", _new_c) - _prev_h
        _dn = _prev_l - _info.get("low", _new_c)
        _pdm = _up if _up > _dn and _up > 0 else 0
        _mdm = _dn if _dn > _up and _dn > 0 else 0
        _sv["adx_a14"] = round((_sv["adx_a14"] * 13 + _new_tr) / 14, 4)
        _sv["adx_sp"] = round((_sv["adx_sp"] * 13 + _pdm) / 14, 4)
        _sv["adx_sm"] = round((_sv["adx_sm"] * 13 + _mdm) / 14, 4)
        _pdi = _sv["adx_sp"] / _sv["adx_a14"] * 100 if _sv["adx_a14"] > 0 else 0
        _mdi = _sv["adx_sm"] / _sv["adx_a14"] * 100 if _sv["adx_a14"] > 0 else 0
        _dx = abs(_pdi - _mdi) / (_pdi + _mdi) * 100 if _pdi + _mdi > 0 else 0
        _sv["adx_val"] = round((_sv["adx_val"] * 13 + _dx) / 14, 4)
        # KD (use PRE-mutation cache for lo/hi arrays)
        _lo_arr = _cs["l"][-9:] + [_info.get("low", _new_c)]
        _hi_arr = _cs["h"][-9:] + [_info.get("high", _new_c)]
        _rsv = (_new_c - min(_lo_arr)) / (max(_hi_arr) - min(_lo_arr)) * 100 if max(_hi_arr) > min(_lo_arr) else 50
        _sv["kd_k_prev"] = _sv["kd_k"]
        _sv["kd_d_prev"] = _sv["kd_d"]
        _sv["kd_k"] = round(_sv["kd_k"] * 2/3 + _rsv / 3, 4)
        _sv["kd_d"] = round(_sv["kd_d"] * 2/3 + _sv["kd_k"] / 3, 4)
        _updated = True

    # Save updated states to Gist
    if _updated:
        indicator_states["updated"] = trading_date
        try:
            _h = {"Authorization": f"token {GITHUB_TOKEN}"}
            requests.patch(f"https://api.github.com/gists/{DATA_GIST_ID}", headers=_h,
                json={"files": {"indicator_state.json": {"content": json.dumps(indicator_states, ensure_ascii=False)}}}, timeout=30)
        except Exception:
            pass

# Now update history cache AFTER states (FIX #14: states read pre-mutation data)
if history_cache and cache_date and market_data and trading_date > cache_date:
    _stocks = history_cache.get("stocks", {})
    for _tk, _hist in _stocks.items():
        if _tk in market_data:
            _info = market_data[_tk]
            _hist["c"] = _hist["c"][-79:] + [_info["close"]]
            _hist["h"] = _hist["h"][-79:] + [_info.get("high", _info["close"])]
            _hist["l"] = _hist["l"][-79:] + [_info.get("low", _info["close"])]
            _hist["v"] = _hist["v"][-79:] + [_info["vol"]]
    history_cache["updated"] = trading_date
    try:
        _h = {"Authorization": f"token {GITHUB_TOKEN}"}
        _payload = {"files": {"history_cache.json": {"content": json.dumps(history_cache, ensure_ascii=False)}}}
        requests.patch(f"https://api.github.com/gists/{HISTORY_GIST_ID}", headers=_h, json=_payload, timeout=60)
        st.cache_data.clear()  # FIX #6: clear cache after write so next load reads updated version
    except Exception:
        pass

# ── Live Scan (every load, uses states = exact results) ──
scan = None
if strategy_params and history_cache and history_cache.get("stocks") and indicator_states:
    try:
        from scanner import run_scan
        scan = run_scan(dict(strategy_params), set(held_tickers), history_cache, indicator_states)
    except Exception:
        pass
if not scan or not scan.get("buy_signals"):
    scan = read_gist_file("scan_results.json")

scan_date = scan.get("date", "") if scan else ""

# ── Signal Computation ──
max_positions = 2
user_buy_signals = []
if len(user_holdings) < max_positions and scan:
    user_buy_signals = scan.get("buy_signals", [])[:1]

# Trading calendar (exact trading days from TWSE, cached 24h)
@st.cache_data(ttl=86400, show_spinner=False)
def _get_trading_cal():
    from scanner import fetch_trading_calendar
    return fetch_trading_calendar()

trading_cal = _get_trading_cal()

# Fallback: if TWSE calendar fails (cloud IP blocked), use all weekdays
if not trading_cal:
    _d = date(2025, 1, 1)
    trading_cal = set()
    while _d <= tw_today():
        if _d.weekday() < 5:
            trading_cal.add(_d)
        _d += timedelta(days=1)

# Sell signals: live tracking using strategy sell conditions
user_sell_signals = []
if user_holdings and strategy_params and market_data:
    try:
        from scanner import check_sell_signals
        _holdings_before = json.dumps(user_holdings)
        user_sell_signals = check_sell_signals(user_holdings, strategy_params, market_data, history_cache, trading_cal)
        # Only save if peak_price actually changed (avoid unnecessary Gist writes)
        if json.dumps(user_holdings) != _holdings_before:
            save_user_holdings(username, user_holdings, clear_cache=False)
    except Exception:
        pass

signal_count = len(user_buy_signals) + len(user_sell_signals)
signal_label = f"🔴 訊號 ({signal_count})" if signal_count > 0 else "訊號"

# ── Tabs ──
tab0, tab1, tab2, tab3 = st.tabs([signal_label, "📊 買入排行", "💼 持倉管理", "📋 回測績效"])

# ══════════════════════════════════════════════════════════════
# TAB 0: SIGNALS
# ══════════════════════════════════════════════════════════════
with tab0:
    if signal_count > 0:
        nd = next_trading_day(scan_date, trading_cal)
        nd_str = nd.strftime("%m/%d")
        wd = ["一", "二", "三", "四", "五", "六", "日"]

        for sig in user_buy_signals:
            st.markdown(
                f"### 🎯 買入訊號\n\n"
                f"**請於 {nd_str}（{wd[nd.weekday()]}）13:25 前買入**\n\n---\n\n"
                f"### {sig.get('name', '')}（{sig.get('ticker', '')}）\n\n"
                f"收盤價 **{sig.get('close', 0)}** 元 ｜ 評分 **{int(sig.get('score', 0))}** 分\n\n"
                f"📌 收盤前下單，跟 GPU 回測一致"
            )
        for sig in user_sell_signals:
            st.markdown(
                f"### 📤 賣出訊號\n\n"
                f"**請於 {nd_str}（{wd[nd.weekday()]}）9:00 開盤賣出**\n\n---\n\n"
                f"### {sig.get('name', '')}（{sig.get('ticker', '')}）\n\n"
                f"報酬 **{sig.get('return', 0):+.1f}%** ｜ {sig.get('reason', '')}"
            )
    else:
        if scan and scan.get("date"):
            if len(user_holdings) >= max_positions:
                st.info(f"目前滿倉（{len(user_holdings)}/{max_positions} 檔），無買入訊號")
            else:
                st.info("目前無任何訊號")
        else:
            st.warning("尚無掃描資料")
    if scan_date:
        st.caption(f"資料日期：{scan_date}")

# ══════════════════════════════════════════════════════════════
# TAB 1: BUY RANKINGS
# ══════════════════════════════════════════════════════════════
with tab1:
    if scan and scan.get("date"):
        ts = scan.get("timestamp", "")
        ts_display = ts.split("T")[-1][:5] if "T" in ts else ts
        st.markdown(f"### 📊 買入排行 — {scan['date']}（掃描於 {ts_display}）")
        st.markdown("---")

        buy_signals = scan.get("buy_signals", [])
        if buy_signals:
            top3 = buy_signals[:3]
            st.markdown("#### 🟢 達標股票（前 3 名）")
            rows = [{"排名": s.get("rank", ""), "代碼": s.get("ticker", ""),
                      "名稱": s.get("name", ""), "分數": int(s.get("score", 0)),
                      "收盤價": s.get("close", 0)} for s in top3]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            top = buy_signals[0]
            st.success(f"🏆 第一名：**{top.get('name', '')}** ({top.get('ticker', '')}) — {int(top.get('score', 0))} 分")
        else:
            st.info("今日無買入訊號")

        mkt = scan.get("market_summary", {})
        if mkt:
            st.markdown("---")
            st.caption(f"📈 上市 {mkt.get('twse_count', 0)} 檔 | 上櫃 {mkt.get('otc_count', 0)} 檔 | 掃描：成交量前 {mkt.get('scan_count', 100)}")
    else:
        st.warning("⚠️ 掃描失敗或尚無資料")

# ══════════════════════════════════════════════════════════════
# TAB 2: HOLDINGS MANAGEMENT
# ══════════════════════════════════════════════════════════════
with tab2:
    st.markdown("### 💼 持倉管理")

    # ── Current Holdings ──
    if user_holdings:
        st.markdown(f"**目前持倉（{len(user_holdings)} 檔）**")

        for i, h in enumerate(user_holdings):
            ticker = h.get("ticker", "")
            name = h.get("name", "")
            buy_price = h.get("buy_price", 0)
            buy_date_str = h.get("buy_date", "")

            # Trading days held (exact from TWSE calendar)
            try:
                _bd = date.fromisoformat(buy_date_str)
                if trading_cal:
                    days = sum(1 for d in trading_cal if _bd < d <= tw_today())
                else:
                    days = max(0, int((tw_today() - _bd).days * 5 / 7))
            except (ValueError, TypeError):
                days = 0

            # Current price: TWSE/TPEx → Gist scan → Yahoo
            cur_price = None
            if market_data:
                if ticker in market_data:
                    cur_price = market_data[ticker]["close"]
                elif f"{ticker}.TW" in market_data:
                    cur_price = market_data[f"{ticker}.TW"]["close"]
                elif f"{ticker}.TWO" in market_data:
                    cur_price = market_data[f"{ticker}.TWO"]["close"]
            if not cur_price:
                for sh in (scan.get("holdings_status", []) if scan else []):
                    if sh.get("ticker") == ticker and sh.get("current_price", 0) > 0:
                        cur_price = sh["current_price"]
                        break
            if not cur_price:
                try:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                    r = requests.get(url, params={"range": "5d", "interval": "1d"},
                                     headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
                    closes = r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                    for cv in reversed(closes):
                        if cv is not None:
                            cur_price = round(cv, 2)
                            break
                except Exception:
                    pass

            if cur_price and buy_price > 0:
                ret = (cur_price / buy_price - 1) * 100
            else:
                ret = 0

            icon = "🟢" if ret > 0 else "🔴" if ret < 0 else "⚪"

            c1, c2, c3, c4, c5 = st.columns([3, 1.5, 1.5, 1.5, 1.5])
            c1.markdown(f"**{icon} {name}** ({ticker})")
            c2.metric("買入", f"${buy_price:.2f}")
            c3.metric("現價", f"${cur_price:.2f}" if cur_price else "—")
            c4.metric("報酬", f"{ret:+.1f}%")
            c5.metric("持有", f"{days} 天")

            # Sell form (expandable)
            with st.expander(f"📤 賣出 {name}"):
                with st.form(f"sell_{i}"):
                    sc1, sc2 = st.columns(2)
                    with sc1:
                        sell_price = st.number_input(
                            "賣出價格", value=float(cur_price) if cur_price else float(buy_price),
                            min_value=0.01, step=0.01, format="%.2f", key=f"sell_price_{i}",
                        )
                    with sc2:
                        sell_date = st.date_input("賣出日期", value=tw_today(), key=f"sell_date_{i}")

                    if st.form_submit_button("確認賣出", use_container_width=True):
                        sell_ret = (sell_price / buy_price - 1) * 100 if buy_price > 0 else 0
                        new_holdings = [x for j, x in enumerate(user_holdings) if j != i]
                        if save_user_holdings(username, new_holdings):
                            st.success(f"已賣出 {name}｜{buy_price} → {sell_price}（{sell_ret:+.1f}%）")
                            st.rerun()
                        else:
                            st.error("儲存失敗")

            st.markdown("---")
    else:
        st.info("目前無持倉")

    # ── Buy Form ──
    st.markdown("---")
    if len(user_holdings) >= max_positions:
        st.warning(f"已滿倉（{len(user_holdings)}/{max_positions} 檔），賣出後才能買入")
    else:
        with st.expander("➕ 買入新股票", expanded=not bool(user_holdings)):
            with st.form("buy_form", clear_on_submit=True):
                bc1, bc2 = st.columns(2)
                with bc1:
                    new_ticker = st.text_input("股票代碼", placeholder="例：2330.TW 或 3264.TWO")
                    new_name = st.text_input("股票名稱", placeholder="例：台積電")
                with bc2:
                    new_price = st.number_input("買入價格", min_value=0.01, step=0.01, format="%.2f")
                    new_date = st.date_input("買入日期", value=tw_today())

                if st.form_submit_button("確認買入", use_container_width=True):
                    if new_ticker and new_name and new_price > 0:
                        tk = new_ticker.strip().upper()
                        # 自動補 .TW/.TWO
                        if not tk.endswith(".TW") and not tk.endswith(".TWO"):
                            if market_data and f"{tk}.TWO" in market_data:
                                tk = f"{tk}.TWO"
                            else:
                                tk = f"{tk}.TW"
                        # 查現價
                        live_price = None
                        if market_data and tk in market_data:
                            live_price = market_data[tk]["close"]
                        updated = list(user_holdings) + [{
                            "ticker": tk,
                            "name": new_name.strip(),
                            "buy_price": round(new_price, 2),
                            "buy_date": str(new_date),
                        }]
                        if save_user_holdings(username, updated):
                            msg = f"已買入 {new_name}（{tk}）@ ${new_price:.2f}"
                            if live_price:
                                msg += f"｜現價 ${live_price:.2f}"
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error("儲存失敗")
                    else:
                        st.error("請填寫完整資訊")

# ══════════════════════════════════════════════════════════════
# TAB 3: BACKTEST RESULTS
# ══════════════════════════════════════════════════════════════
with tab3:
    st.markdown("### 📋 回測績效")

    backtest = read_gist_file("backtest_results.json")
    bt_stats = backtest.get("stats", {}) if backtest else {}
    bt_trades = backtest.get("trades", []) if backtest else []

    # === Auto-extend backtest day-by-day to today (using history cache) ===
    if bt_trades and trading_date:
        bt_end = bt_stats.get("end_date", "")
        if trading_date > bt_end and trading_cal and history_cache:
            import numpy as _np
            from scanner import compute_indicators, score_stock

            _cache = history_cache.get("stocks", {}) if history_cache else {}
            _cache_updated = history_cache.get("updated", "") if history_cache else ""
            _sp = strategy_params
            _max_pos = int(_sp.get("max_positions", 2))
            _buy_th = _sp.get("buy_threshold", 10)

            # Find trading days to simulate
            _all_cal = sorted(trading_cal)
            try:
                _bt_end_d = date.fromisoformat(bt_end)
                _cache_end_d = date.fromisoformat(_cache_updated) if _cache_updated else _bt_end_d
                _sim_dates = [d for d in _all_cal if _bt_end_d < d <= _cache_end_d]
            except:
                _sim_dates = []

            # Add today if TWSE has newer data than cache
            if trading_date > _cache_updated:
                try:
                    _td = date.fromisoformat(trading_date)
                    if _td not in _sim_dates:
                        _sim_dates.append(_td)
                except:
                    pass

            # Map cache dates: last entry = cache_updated, previous = previous trading day
            _cal_up_to_cache = [d for d in _all_cal if d <= _cache_end_d] if _cache_updated else []

            if _sim_dates and (_cal_up_to_cache or market_data):
                sim_holdings = [dict(t) for t in bt_trades if t.get("reason") == "持有中"]
                bt_trades = [t for t in bt_trades if t.get("reason") != "持有中"]

                for sim_day in sorted(_sim_dates):
                    sd_str = str(sim_day)

                    # Build market data for this day from cache or live API
                    _dmkt = {}
                    if sd_str == trading_date and market_data:
                        _dmkt = market_data
                    elif sim_day in _cal_up_to_cache:
                        _offset = len(_cal_up_to_cache) - 1 - _cal_up_to_cache.index(sim_day)
                        for tk, cs in _cache.items():
                            idx = len(cs["c"]) - 1 - _offset
                            if 0 <= idx < len(cs["c"]):
                                _dmkt[tk] = {"close":cs["c"][idx],"high":cs["h"][idx],"low":cs["l"][idx],"vol":cs["v"][idx]}
                    if len(_dmkt) < 50:
                        continue

                    _top100 = sorted(_dmkt.keys(), key=lambda t: _dmkt[t]["vol"], reverse=True)[:100]

                    # SELL
                    _new_h = []
                    for h in sim_holdings:
                        tk = h["ticker"]
                        if tk not in _dmkt:
                            _new_h.append(h); continue
                        bp = h["buy_price"]; cur = _dmkt[tk]["close"]
                        ret = (cur/bp-1)*100 if bp > 0 else 0
                        try: dh = sum(1 for d in _all_cal if date.fromisoformat(h["buy_date"]) < d <= sim_day)
                        except: dh = 0
                        pk = max(h.get("peak_price", bp), cur); h["peak_price"] = pk
                        reason = None
                        if dh < 1: _new_h.append(h); continue
                        if ret <= _sp.get("stop_loss",-20): reason = f"停損 {ret:+.1f}%"
                        if not reason and _sp.get("use_take_profit",1) and ret >= _sp.get("take_profit",80): reason = f"停利 +{ret:.1f}%"
                        if not reason and _sp.get("trailing_stop",0)>0 and pk>bp*1.01 and (cur/pk-1)*100<=-_sp["trailing_stop"]: reason = f"移動停利 {(cur/pk-1)*100:.1f}%"
                        if not reason and _sp.get("use_time_decay",0):
                            hh=int(_sp.get("hold_days",30))//2
                            if dh>=hh and ret<(dh-hh)*_sp.get("ret_per_day",0.5): reason="漸進停利"
                        if not reason and _sp.get("use_profit_lock",0):
                            pg=(pk/bp-1)*100
                            if pg>=_sp.get("lock_trigger",30) and ret<_sp.get("lock_floor",10): reason="鎖利"
                        if not reason and dh>=int(_sp.get("hold_days",30)): reason=f"到期{dh}天 {ret:+.1f}%"
                        if reason:
                            bt_trades.append({"ticker":tk,"name":h.get("name",""),"buy_price":bp,
                                "sell_price":round(cur,2),"hold_days":dh,"return_pct":round(ret,1),
                                "reason":reason,"buy_date":h["buy_date"],"sell_date":sd_str})
                        else: _new_h.append(h)
                    sim_holdings = _new_h

                    # BUY
                    if len(sim_holdings) < _max_pos:
                        _held = {h["ticker"] for h in sim_holdings}
                        _sigs = []
                        for tk in _top100:
                            if tk in _held or tk not in _cache: continue
                            cs = _cache[tk]
                            if sim_day not in _cal_up_to_cache: continue
                            _off = len(_cal_up_to_cache)-1-_cal_up_to_cache.index(sim_day)
                            _ei = len(cs["c"])-_off
                            if _ei < 20: continue
                            try:
                                ind = compute_indicators(
                                    _np.array(cs["c"][:_ei],dtype=_np.float64),
                                    _np.array(cs["h"][:_ei],dtype=_np.float64),
                                    _np.array(cs["l"][:_ei],dtype=_np.float64),
                                    _np.array(cs["v"][:_ei],dtype=_np.float64))
                                if ind and score_stock(ind,_sp) >= _buy_th:
                                    # Name: try live market_data (has names), fallback to ticker
                                    _nm = ""
                                    if market_data and tk in market_data:
                                        _nm = market_data[tk].get("name", "")
                                    _sigs.append({"tk":tk,"sc":score_stock(ind,_sp),"vol":_dmkt[tk]["vol"],
                                        "name":_nm or tk.replace(".TW","").replace(".TWO",""),"price":_dmkt[tk]["close"]})
                            except: continue
                        if _sigs:
                            _sigs.sort(key=lambda x:(x["sc"],x["vol"]),reverse=True)
                            for s in _sigs[:1]:  # Only buy #1 per day (matching GPU)
                                sim_holdings.append({"ticker":s["tk"],"name":s["name"],"buy_price":s["price"],
                                    "buy_date":sd_str,"peak_price":s["price"],"sell_price":s["price"],
                                    "hold_days":0,"return_pct":0,"reason":"持有中"})

                # Update holding with latest prices
                for h in sim_holdings:
                    tk = h["ticker"]
                    if market_data and tk in market_data:
                        cur = market_data[tk]["close"]
                        h["sell_price"] = round(cur,2)
                        h["return_pct"] = round((cur/h["buy_price"]-1)*100,1) if h["buy_price"]>0 else 0
                        try: h["hold_days"] = sum(1 for d in _all_cal if date.fromisoformat(h["buy_date"])<d<=date.fromisoformat(trading_date))
                        except: pass

                bt_trades = bt_trades + sim_holdings
                bt_stats["end_date"] = trading_date
                try:
                    write_gist_file("backtest_results.json",{"stats":bt_stats,"trades":bt_trades},clear_cache=False)
                except: pass

    if bt_stats:
        # Compute trading days (calendar only covers 3 months, use approximation for full period)
        try:
            _sd = date.fromisoformat(bt_stats.get('start_date', ''))
            _ed = date.fromisoformat(bt_stats.get('end_date', ''))
            _total_days = int((_ed - _sd).days * 5 / 7)
        except:
            _total_days = bt_stats.get('total_days', 0)
        st.markdown(f"**回測期間**：{bt_stats.get('start_date', '')} ~ {bt_stats.get('end_date', '')}（{_total_days} 交易日）")
        st.markdown("---")

        # Compute all stats from trades
        _completed = [t for t in bt_trades if t.get("reason") != "持有中"]
        _rets = [t.get("return_pct", 0) for t in _completed]
        _wins = [r for r in _rets if r > 0]
        _losses = [r for r in _rets if r <= 0]
        _bt_total = sum(_rets)
        _win_rate = len(_wins) / len(_rets) * 100 if _rets else 0
        _avg_ret = sum(_rets) / len(_rets) if _rets else 0
        _avg_win = sum(_wins) / len(_wins) if _wins else 0
        _avg_loss = sum(_losses) / len(_losses) if _losses else 0
        _max_win = max(_rets) if _rets else 0
        _max_loss = min(_rets) if _rets else 0
        _avg_hold = sum(t.get("hold_days", 0) for t in _completed) / len(_completed) if _completed else 0

        # CAGR: use simple total return (matching GPU sum method)
        # Each trade uses 1/max_positions of capital
        _pos_size = 1 / max(int(strategy_params.get("max_positions", 2)), 1)
        _portfolio_growth = 1 + (_bt_total * _pos_size) / 100
        try:
            _start_d = date.fromisoformat(bt_stats.get("start_date", "2022-01-01"))
            _end_d = date.fromisoformat(bt_stats.get("end_date", str(tw_today())))
            _years = max((_end_d - _start_d).days / 365.25, 0.1)
            _cagr = (_portfolio_growth ** (1 / _years) - 1) * 100 if _portfolio_growth > 0 else 0
        except:
            _cagr = 0

        # Max Drawdown: track equity curve (scaled by position size)
        _equity = 1.0
        _peak_eq = 1.0
        _max_dd = 0
        for r in _rets:
            _equity *= (1 + r * _pos_size / 100)
            _peak_eq = max(_peak_eq, _equity)
            _dd = (_equity / _peak_eq - 1) * 100
            _max_dd = min(_max_dd, _dd)

        # Sharpe Ratio (annualized, assume 252 trading days, risk-free = 0)
        import math
        if len(_rets) >= 2:
            _mean_r = sum(_rets) / len(_rets)
            _std_r = math.sqrt(sum((r - _mean_r) ** 2 for r in _rets) / (len(_rets) - 1))
            _trades_per_year = 252 / _avg_hold if _avg_hold > 0 else 12
            _sharpe = (_mean_r / _std_r) * math.sqrt(_trades_per_year) if _std_r > 0 else 0
        else:
            _sharpe = 0

        # Profit Factor: total wins / total losses
        _total_win = sum(_wins) if _wins else 0
        _total_loss = abs(sum(_losses)) if _losses else 0
        _profit_factor = _total_win / _total_loss if _total_loss > 0 else float('inf')

        # Win/Loss Ratio (盈虧比): avg win / avg loss
        _wl_ratio = abs(_avg_win / _avg_loss) if _avg_loss != 0 else float('inf')

        # Display
        st.markdown("#### 核心指標")
        c1, c2, c3 = st.columns(3)
        c1.metric("總報酬", f"{_bt_total:,.1f}%")
        c2.metric("CAGR", f"{_cagr:.1f}%")
        c3.metric("Max Drawdown", f"{_max_dd:.1f}%")

        c4, c5, c6 = st.columns(3)
        c4.metric("Sharpe Ratio", f"{_sharpe:.2f}")
        c5.metric("勝率", f"{_win_rate:.1f}%")
        c6.metric("盈虧比", f"{_wl_ratio:.2f}")

        c7, c8, c9 = st.columns(3)
        c7.metric("Profit Factor", f"{_profit_factor:.2f}")
        c8.metric("交易次數", f"{len(_completed)}")
        c9.metric("平均報酬", f"{_avg_ret:+.1f}%")

        st.markdown("---")
        st.markdown("#### 詳細數據")
        c10, c11, c12, c13 = st.columns(4)
        c10.metric("平均獲利", f"+{_avg_win:.1f}%")
        c11.metric("平均虧損", f"{_avg_loss:.1f}%")
        c12.metric("最大獲利", f"+{_max_win:.1f}%")
        c13.metric("最大虧損", f"{_max_loss:.1f}%")

        c14, c15 = st.columns(2)
        c14.metric("平均持有天數", f"{_avg_hold:.0f} 天")
        c15.metric("持倉上限", f"{int(strategy_params.get('max_positions', 2))} 檔")

        # Trade list
        st.markdown("---")
        st.markdown(f"#### 交易明細（{len(bt_trades)} 筆）")

        if bt_trades:
            trade_rows = []
            for t in bt_trades:
                ret = t.get("return_pct", 0)
                icon = "🟢" if ret > 0 else "🔴" if ret < 0 else "⚪"
                trade_rows.append({
                    "": icon,
                    "股票": t.get("name", "") or t.get("ticker", ""),
                    "買入日": t.get("buy_date", ""),
                    "賣出日": t.get("sell_date", ""),
                    "買入價": t.get("buy_price", 0),
                    "賣出價": t.get("sell_price", 0),
                    "報酬%": f"{ret:+.1f}%",
                    "持有天數": t.get("hold_days", 0),
                    "出場原因": t.get("reason", ""),
                })
            df_trades = pd.DataFrame(trade_rows)
            st.dataframe(df_trades, use_container_width=True, hide_index=True, height=500)

            # Exit reason breakdown
            st.markdown("---")
            st.markdown("#### 出場原因分佈")
            reasons = {}
            for t in bt_trades:
                r = t.get("reason", "其他").split("！")[0].split(" ")[0]
                reasons[r] = reasons.get(r, 0) + 1
            for r, count in sorted(reasons.items(), key=lambda x: -x[1]):
                st.caption(f"  {r}：{count} 次")
    else:
        st.info("回測資料準備中...歷史資料下載完成後會自動顯示。")
