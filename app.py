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
tab0, tab1, tab2 = st.tabs([signal_label, "📊 買入排行", "💼 持倉管理"])

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
