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
STATE_GIST_ID = st.secrets.get("state_gist_id", DATA_GIST_ID)
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
                _read_gist.clear()
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
    data, td = fetch_market_data()
    # Streamlit does NOT cache exceptions → bad fetch won't be cached,
    # next page load auto-retries. This is the permanent fix.
    if len(data) < 500:
        raise RuntimeError(f"Market data incomplete: {len(data)} stocks (need 500+)")
    return data, td


# ── Live Scan (每次登入都跑，session 內快取) ─────────────────


# ── Helper ───────────────────────────────────────────────────
@st.cache_data(ttl=604800, show_spinner=False)
def _fetch_monthly_trading_days():
    """TWSE FMTQIK: 每月交易天數（含精確日期），快取 7 天"""
    from datetime import date as _d
    import time as _time
    today = tw_today()
    trading_dates = set()
    d = _d(2022, 1, 1)
    while d <= today:
        ds = d.strftime("%Y%m%d")
        try:
            r = requests.get(f"https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={ds}",
                timeout=10, verify=False, headers={"User-Agent": "Mozilla/5.0"})
            for row in r.json().get("data", []):
                parts = row[0].split("/")
                trading_dates.add(_d(int(parts[0]) + 1911, int(parts[1]), int(parts[2])))
            _time.sleep(0.15)
        except:
            pass
        m = d.month + 1; y = d.year
        if m > 12: m = 1; y += 1
        d = _d(y, m, 1)
    return trading_dates


def _count_trading_days(start_str, end_str):
    """精確計算兩個日期間的交易日數"""
    try:
        sd = date.fromisoformat(start_str)
        ed = date.fromisoformat(end_str)
        cal = _fetch_monthly_trading_days()
        if cal:
            return sum(1 for d in cal if sd <= d <= ed)
        return round((ed - sd).days * 242 / 365)
    except:
        return 0


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

# Fallback trading_date if TWSE fails
if not trading_date:
    trading_date = str(tw_today())

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

# ── Fallback: fill ANY missing stock from Gist history cache ──
# daily_scan writes 2000 stocks to history cache every trading day (reliable).
# Even if API returns 6000 stocks but misses a few (達邁/佰鴻), cache fills the gaps.
# Stocks already in market_data (from live API) are NOT overwritten.
if history_cache and history_cache.get("stocks"):
    _fb_stocks = history_cache["stocks"]
    _fb_filled = 0
    for _fb_tk, _fb_cs in _fb_stocks.items():
        if _fb_tk not in market_data and _fb_cs.get("c") and len(_fb_cs["c"]) > 0:
            market_data[_fb_tk] = {
                "close": _fb_cs["c"][-1],
                "high": _fb_cs["h"][-1] if _fb_cs.get("h") else _fb_cs["c"][-1],
                "low": _fb_cs["l"][-1] if _fb_cs.get("l") else _fb_cs["c"][-1],
                "vol": _fb_cs["v"][-1] if _fb_cs.get("v") else 0,
                "open": _fb_cs["o"][-1] if _fb_cs.get("o") and _fb_cs["o"] else _fb_cs["c"][-1],
                "name": "",
            }
            _fb_filled += 1
    if not trading_date or trading_date == str(tw_today()):
        trading_date = cache_date or str(tw_today())

# ── Indicator States (separate Gist for large file) ──
@st.cache_data(ttl=300)
def _read_state_gist():
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        r = requests.get(f"https://api.github.com/gists/{STATE_GIST_ID}", headers=headers, timeout=15)
        if r.status_code == 200:
            fdata = list(r.json().get("files", {}).values())[0]
            if fdata.get("truncated"):
                raw = requests.get(fdata["raw_url"], headers=headers, timeout=60)
                return json.loads(raw.text)
            return json.loads(fdata.get("content", "{}"))
    except Exception:
        pass
    return {}

indicator_states = _read_state_gist()
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
            requests.patch(f"https://api.github.com/gists/{STATE_GIST_ID}", headers=_h,
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
            _hist["h250"] = (_hist.get("h250") or _hist["h"])[-249:] + [_info.get("high", _info["close"])]
            _hist["l250"] = (_hist.get("l250") or _hist["l"])[-249:] + [_info.get("low", _info["close"])]
            _hist.pop("dates", None)  # per-stock dates 已搬頂層
    history_cache["dates"] = (history_cache.get("dates") or [])[-79:] + [trading_date]
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

# ── Trading Calendar ──
@st.cache_data(ttl=86400, show_spinner=False)
def _get_trading_cal():
    from scanner import fetch_trading_calendar
    return fetch_trading_calendar()

@st.cache_data(ttl=604800, show_spinner=False)
def _get_full_trading_cal():
    from scanner import fetch_trading_calendar
    return fetch_trading_calendar(months=48)

trading_cal = _get_trading_cal()
_full_trading_cal = _get_full_trading_cal()

if not trading_cal:
    _d = date(2025, 1, 1)
    trading_cal = set()
    while _d <= tw_today():
        if _d.weekday() < 5:
            trading_cal.add(_d)
        _d += timedelta(days=1)

# ── Signal Computation (SELL first, then BUY based on remaining slots) ──
max_positions = int(strategy_params.get("max_positions", 2))

# 1. Sell signals FIRST
user_sell_signals = []
if user_holdings and strategy_params and market_data:
    try:
        from scanner import check_sell_signals
        _holdings_before = json.dumps(user_holdings)
        user_sell_signals = check_sell_signals(user_holdings, strategy_params, market_data, history_cache, _full_trading_cal or trading_cal)
        if json.dumps(user_holdings) != _holdings_before:
            save_user_holdings(username, user_holdings, clear_cache=False)
    except Exception:
        pass

# 2. Buy signals (account for sells freeing slots)
user_buy_signals = []
_effective_holdings = len(user_holdings) - len(user_sell_signals)
if _effective_holdings < max_positions and scan:
    _sold_tickers = {s.get("ticker") for s in user_sell_signals}
    _buy_candidates = [s for s in scan.get("buy_signals", []) if s.get("ticker") not in _sold_tickers]
    user_buy_signals = _buy_candidates[:1]

signal_count = len(user_buy_signals) + len(user_sell_signals)
signal_label = f"🔴 訊號 ({signal_count})" if signal_count > 0 else "訊號"

# ── Tabs ──
tab0, tab1, tab2, tab3 = st.tabs([signal_label, "📊 買入排行", "💼 持倉管理", "📋 回測績效"])

# ══════════════════════════════════════════════════════════════
# TAB 0: SIGNALS
# ══════════════════════════════════════════════════════════════
with tab0:
    # Bug fix: scan_date 空白 fallback + 過期偵測
    _sig_d = scan_date or trading_date

    # 邊緣情況偵測：停牌 / 除權除息異常（針對用戶自己持股）
    # Bug fix: 非交易日不檢查（週末市場關）
    _user_edge_warnings = []
    try:
        _tab0_is_trading = date.fromisoformat(trading_date) in (_full_trading_cal or trading_cal or set())
    except:
        _tab0_is_trading = False
    if user_holdings and market_data and len(market_data) >= 500 and _tab0_is_trading:
        _user_cache = history_cache.get("stocks", {}) if history_cache else {}
        for _uh in user_holdings:
            _utk = _uh.get("ticker", "")
            _unm = _uh.get("name", _utk)
            _ubp = _uh.get("buy_price", 0)
            if _utk not in market_data:
                _user_edge_warnings.append(f"🚫 **{_unm}（{_utk}）無成交資料** — 可能停牌，無法賣出")
                continue
            _uinfo = market_data[_utk]
            if _uinfo.get("vol", 0) == 0:
                _user_edge_warnings.append(f"🚫 **{_unm}（{_utk}）成交量 0** — 可能停牌")
                continue
            if _utk in _user_cache:
                _ucs = list(_user_cache[_utk].get("c", []))
                if _ucs and _uinfo["close"] > 0:
                    _ulast = _ucs[-1]
                    _uchg = (_uinfo["close"] / _ulast - 1) * 100 if _ulast > 0 else 0
                    if _uchg <= -5:
                        _user_edge_warnings.append(
                            f"⚠️ **{_unm}（{_utk}）單日跌 {_uchg:.1f}%** — 疑似除權除息。實盤你會拿股息/配股，"
                            f"但 Web 會算成虧損。請到「持倉管理」手動調整買入成本。"
                        )
    if _user_edge_warnings:
        for _w in _user_edge_warnings:
            st.warning(_w)

    if signal_count > 0:
        nd = next_trading_day(_sig_d, trading_cal)
        nd_str = nd.strftime("%m/%d")
        wd = ["一", "二", "三", "四", "五", "六", "日"]
        try:
            _today_d = date.fromisoformat(trading_date) if trading_date else tw_today()
            _sig_stale = nd <= _today_d and _sig_d != trading_date
        except:
            _sig_stale = False
        if _sig_stale:
            st.warning(f"⚠️ 訊號日 {_sig_d} 已過期（執行日 {nd_str} 在今天之前）— daily_scan 可能中斷，請重新整理")

        # SELL first (GPU order)
        for sig in user_sell_signals:
            st.error(
                f"### 📤 賣出\n\n"
                f"**{sig.get('name', '')}（{sig.get('ticker', '')}）**\n\n"
                f"報酬 {sig.get('return', 0):+.1f}% ｜ {sig.get('reason', '')}\n\n"
                f"**{nd_str}（{wd[nd.weekday()]}）9:00 開盤賣出（D+1）**"
            )

        # BUY after sell (only #1, GPU rule: 1 per day)
        for sig in user_buy_signals:
            _sp_close = sig.get('close', 0)
            _sp_display = f"{_sp_close} 元" if _sp_close > 0 else "（無價格）"
            st.success(
                f"### 🎯 買入\n\n"
                f"**{sig.get('name', '')}（{sig.get('ticker', '')}）**\n\n"
                f"評分 {int(sig.get('score', 0))} 分 ｜ 收盤價 {_sp_display}\n\n"
                f"**{nd_str}（{wd[nd.weekday()]}）13:25 前買入（D+1）**"
            )

        # If 2 sells but only 1 buy, explain the second slot
        if len(user_sell_signals) > 1 and len(user_buy_signals) <= 1:
            nd2 = next_trading_day(str(nd), trading_cal)
            st.info(f"第 2 個空位：{nd.strftime('%m/%d')}（{wd[nd.weekday()]}）掃描 → {nd2.strftime('%m/%d')}（{wd[nd2.weekday()]}）買入")

        st.caption(f"訊號日：{_sig_d}（D）")
    else:
        if scan and scan.get("date"):
            if len(user_holdings) >= max_positions:
                st.info(f"目前滿倉（{len(user_holdings)}/{max_positions} 檔），無買賣訊號")
            else:
                st.info("目前無任何訊號")
        else:
            st.warning("尚無掃描資料")
        # Bug fix: 原本 line 466-467 重複顯示訊號日；現在只在「無訊號」分支顯示資料日期
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
            from trading_days import count_between
            _best_cal = _full_trading_cal or trading_cal
            _fb_dates = [str(d) for d in _best_cal] if _best_cal else (history_cache.get("dates", []) if history_cache else None)
            days = count_between(buy_date_str, str(tw_today()), fallback_calendar=_fb_dates)

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
            # Fallback: Gist history cache（不用 Yahoo，避免 ADR/幣別問題）
            if not cur_price and history_cache and history_cache.get("stocks"):
                _fb_cs = history_cache["stocks"].get(ticker, {})
                if _fb_cs and _fb_cs.get("c"):
                    cur_price = _fb_cs["c"][-1]

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
                    if not new_ticker or not new_name or new_price <= 0:
                        st.error("請填寫完整資訊")
                    elif any(h.get("ticker","").replace(".TW","").replace(".TWO","") == new_ticker.strip().upper().replace(".TW","").replace(".TWO","") for h in user_holdings):
                        st.error("此股票已在持倉中，不可重複買入")
                    elif new_ticker and new_name and new_price > 0:
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
                            "peak_price": round(new_price, 2),
                        }]
                        if save_user_holdings(username, updated):
                            msg = f"已買入 {new_name}（{tk}）@ ${new_price:.2f}"
                            if live_price:
                                msg += f"｜現價 ${live_price:.2f}"
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error("儲存失敗")

# ══════════════════════════════════════════════════════════════
# TAB 3: BACKTEST RESULTS
# ══════════════════════════════════════════════════════════════
with tab3:
    st.markdown("### 📋 回測績效")

    backtest = read_gist_file("backtest_results.json")
    bt_stats = backtest.get("stats", {}) if backtest else {}
    bt_trades = backtest.get("trades", []) if backtest else []

    # === 🔴 延續交易新鮮度檢查（保證資料永遠最新）===
    _bt_end_check = bt_stats.get("end_date", "")
    _cache_updated_check = history_cache.get("updated", "") if history_cache else ""
    try:
        _cal_for_check = sorted(_full_trading_cal) if _full_trading_cal else sorted(trading_cal)
        _today_check = date.fromisoformat(trading_date) if trading_date else tw_today()
        _bt_end_d_check = date.fromisoformat(_bt_end_check) if _bt_end_check else None
        # 計算回測端點和今天之間隔了幾個交易日
        if _bt_end_d_check and _cal_for_check:
            _missing_trading_days = sum(1 for d in _cal_for_check if _bt_end_d_check < d <= _today_check)
        else:
            _missing_trading_days = 0
        # 超過 1 個交易日沒更新 → 嚴重警告
        if _missing_trading_days >= 2:
            st.error(
                f"🚨 **回測資料落後 {_missing_trading_days} 個交易日！**\n\n"
                f"最後更新：{_bt_end_check}｜今天：{trading_date}\n\n"
                f"**daily_scan 可能故障**。請到 GitHub Actions 手動觸發 workflow，"
                f"或檢查 GitHub Actions 是否被 disable。"
            )
        elif _missing_trading_days == 1:
            # 正常：今天 daily_scan 可能還沒跑（16:35 前）
            import datetime as _dt2
            _now_tw2 = _dt2.datetime.now(_dt2.timezone(_dt2.timedelta(hours=8)))
            if _now_tw2.hour >= 17:  # 17:00 後還沒更新 = 異常
                st.warning(
                    f"⚠️ daily_scan 似乎還沒跑完今天（預定 16:35）— {_bt_end_check} 最後更新。"
                    f"可能網路延遲，請稍候或手動 retrigger。"
                )
        # Cache 過期檢查
        if _cache_updated_check:
            _cache_d = date.fromisoformat(_cache_updated_check) if _cache_updated_check else None
            _cache_gap = sum(1 for d in _cal_for_check if _cache_d < d <= _today_check) if _cache_d else 0
            if _cache_gap >= 2:
                st.error(f"🚨 **歷史快取落後 {_cache_gap} 天**（{_cache_updated_check}）— 指標計算會用舊資料")
    except Exception as _e:
        pass

    # === 換股狀態 ===
    _bt_holding = [t for t in bt_trades if t.get("reason") == "持有中"]
    _swap_cache = history_cache.get("stocks", {}) if history_cache else {}

    # === 邊緣情況偵測：停牌 / 除權除息異常 / 跌停 ===
    # Bug fix: 非交易日（週末、假日）不檢查停牌 — market_data 必然全空，會誤報
    _edge_warnings = []
    try:
        _today_is_trading = date.fromisoformat(trading_date) in (_full_trading_cal or trading_cal or set())
    except:
        _today_is_trading = False
    if _bt_holding and market_data and len(market_data) >= 500 and _today_is_trading:
        for _bh in _bt_holding:
            _tk = _bh.get("ticker", "")
            _nm = _bh.get("name", _tk)
            _bp = _bh.get("buy_price", 0)
            # 停牌：無成交資料 or vol=0
            if _tk not in market_data:
                _edge_warnings.append(f"🚫 **{_nm}（{_tk}）可能停牌** — 今日無成交資料，無法賣出")
                continue
            _info = market_data[_tk]
            if _info.get("vol", 0) == 0:
                _edge_warnings.append(f"🚫 **{_nm}（{_tk}）成交量 0** — 可能停牌或流動性不足")
                continue
            # 取昨日 close（從 cache 倒數第二筆，因為今天 close 可能也在 cache）
            _last_c = None
            if _tk in _swap_cache:
                _cs_list = list(_swap_cache[_tk].get("c", []))
                if len(_cs_list) >= 2:
                    _last_c = _cs_list[-2] if _cs_list[-1] == _info.get("close") else _cs_list[-1]
                elif _cs_list:
                    _last_c = _cs_list[-1]
            _today_c = _info["close"]
            # 除權除息：今日 close vs 昨日 close 跌 >=5% 且有成交，疑似
            if _last_c and _last_c > 0:
                _daily_change = (_today_c / _last_c - 1) * 100
                if _daily_change <= -5:
                    _edge_warnings.append(
                        f"⚠️ **{_nm}（{_tk}）單日跌 {_daily_change:.1f}%** — 疑似除權除息。"
                        f"實盤你會拿到股息/配股，Web 用原始價計算會顯示虛假虧損。"
                        f"請到「持倉管理」手動調整買入成本（或到銀行券商 App 查除權日）。"
                    )
                # 跌停：今日開盤相對昨日收盤跌 >=9.5%
                _open_p = _info.get("open", _today_c)
                if _open_p > 0 and (_open_p / _last_c - 1) * 100 <= -9.5:
                    _edge_warnings.append(
                        f"🔴 **{_nm}（{_tk}）開盤跌停** — 若今日需賣出可能無法成交，或成交價很差"
                    )

    if _edge_warnings:
        for _w in _edge_warnings:
            st.warning(_w)

    # === 換股狀態永遠顯示（不管有沒有持倉）===
    st.markdown("#### 換股狀態")
    if not bt_trades:
        st.info("尚無回測資料。請推策略到 Gist（backtest_to_web.py）。")
    elif not strategy_params:
        st.warning("尚未載入策略參數")
    elif not market_data:
        st.warning("尚未載入市場資料（TWSE/TPEx）")
    elif not _bt_holding:
        # 沒持倉 — 等下個買入訊號
        _next_td = next_trading_day(trading_date, trading_cal)
        _wdn = ["一", "二", "三", "四", "五", "六", "日"]
        _buy_candidates_empty = scan.get("buy_signals", []) if scan else []
        if _buy_candidates_empty:
            _b0 = _buy_candidates_empty[0]
            st.success(
                f"**🎯 D+1 買入**（持倉 0 檔，直接開新倉）{_b0.get('name', '')}（{_b0.get('ticker', '')}）\n\n"
                f"評分 {int(_b0.get('score', 0))} 分｜收盤價 {_b0.get('close', 0)}｜"
                f"**{_next_td.strftime('%m/%d')}（{_wdn[_next_td.weekday()]}）13:25 前買入**"
            )
        else:
            st.info(f"目前 0 檔持倉，掃描也無達標買入候選（下個交易日 {_next_td.strftime('%m/%d')} 若無訊號繼續觀望）")
        st.markdown("---")
    elif _bt_holding and strategy_params and market_data:
        _has_swap = False
        _sp = strategy_params
        _sell_list = []  # Collect all sells first
        _cal = _full_trading_cal or trading_cal  # Bug fix: 統一日曆 source，避免 gate 和 sum 用不同

        # FIX #7+#10: 用 scan_results 的日期（= daily_scan 寫的），不用 live scan 的日期
        _scan_data = read_gist_file("scan_results.json")
        _scan_results_date = (_scan_data or {}).get("date", "")
        _d_date = _scan_results_date or trading_date
        _nd = next_trading_day(_d_date, _full_trading_cal or trading_cal)
        _wd = ["一", "二", "三", "四", "五", "六", "日"]

        # FIX #2: stale = 超過 1 個交易日沒更新（正常 D+1 morning 不該觸發）
        _scan_not_yet_today = False
        try:
            # 用真實日曆日期（tw_today），不用市場資料日期（trading_date）
            # 因為 trading_date 可能跟 scan_date 相同（都是今天），但 scan 其實是昨天跑的
            _today_real = tw_today()
            _scan_d = date.fromisoformat(_d_date) if _d_date else _today_real
            _cal_list = sorted(_full_trading_cal or trading_cal or [])
            _days_since_scan = sum(1 for d in _cal_list if _scan_d < d <= _today_real) if _cal_list else 0
            _stale = _days_since_scan >= 2
            # scan 不是今天的 + 今天是工作日 → daily_scan 還沒跑
            # 不依賴交易日曆快取（可能不含今天），直接看週一~週五
            if _scan_d < _today_real and _today_real.weekday() < 5:
                _scan_not_yet_today = True
        except:
            _stale = False

        for _bh in _bt_holding:
            _tk = _bh.get("ticker", "")
            _bp = _bh.get("buy_price", 0)
            _nm = _bh.get("name", _tk)
            if _tk not in market_data or _bp <= 0:
                continue
            _cur = market_data[_tk]["close"]
            if not _cur or _cur <= 0:  # Bug fix: bad market data shouldn't yield 0% return silently
                continue
            _ret = (_cur / _bp - 1) * 100
            # 統一到 trading_days 模組
            from trading_days import count_between
            _upto_str = _d_date if _d_date else str(_today_d)
            _dh = count_between(_bh.get("buy_date", ""), _upto_str,
                                 fallback_calendar=[str(d) for d in _cal] if _cal else None)
            _pk = max(_bh.get("peak_price", _bp), _cur)

            _reason = None
            if _dh >= 1:
                # Delegate to shared sell_rules (matches kernel 1:1, same as scanner/daily_scan)
                from sell_rules import should_sell
                _cs_c = list(_swap_cache.get(_tk,{}).get("c",[])) if _tk in _swap_cache else None
                if _cs_c is not None and market_data and _tk in market_data:
                    _cs_c = _cs_c + [market_data[_tk]["close"]]  # FIX M3: append today's close for MA60
                # Compute indicators if strategy uses indicator-based sell conditions
                _ind_t3 = None
                if _tk in _swap_cache and any(_sp.get(k, 0) for k in ("use_rsi_sell", "use_macd_sell", "use_kd_sell", "sell_vol_shrink", "use_mom_exit")):
                    try:
                        import numpy as _np_t3
                        from scanner import compute_indicators as _ci_t3
                        _cs_t3 = _swap_cache[_tk]
                        _c_t3 = _np_t3.array(list(_cs_t3["c"]) + ([market_data[_tk]["close"]] if _tk in market_data else []), dtype=_np_t3.float64)
                        _h_t3 = _np_t3.array(list(_cs_t3["h"]) + ([market_data[_tk]["high"]] if _tk in market_data else []), dtype=_np_t3.float64)
                        _l_t3 = _np_t3.array(list(_cs_t3["l"]) + ([market_data[_tk]["low"]] if _tk in market_data else []), dtype=_np_t3.float64)
                        _v_t3 = _np_t3.array(list(_cs_t3["v"]) + ([market_data[_tk]["vol"]] if _tk in market_data else []), dtype=_np_t3.float64)
                        _h250_t3 = _np_t3.array(list(_cs_t3.get("h250", [])) + ([market_data[_tk]["high"]] if _tk in market_data else []), dtype=_np_t3.float64) if _cs_t3.get("h250") else None
                        _l250_t3 = _np_t3.array(list(_cs_t3.get("l250", [])) + ([market_data[_tk]["low"]] if _tk in market_data else []), dtype=_np_t3.float64) if _cs_t3.get("l250") else None
                        if len(_c_t3) >= 20:
                            _ind_t3 = _ci_t3(_c_t3, _h_t3, _l_t3, _v_t3, h250=_h250_t3, l250=_l250_t3)
                    except Exception:
                        pass
                _reason = should_sell(_bp, _cur, _pk, _dh, _sp, cache_closes=_cs_c, indicators=_ind_t3)

            if _reason:
                _sell_list.append({"name": _nm, "ticker": _tk, "reason": _reason, "ret": _ret, "dh": _dh, "buy_date": _bh.get("buy_date","")})

        # Bug fix: 無論有沒有賣出都顯示訊號日，避免「有持倉但靜默」
        _nd_str = _nd.strftime("%m/%d")
        _d_display = _d_date if _d_date else "（未掃描）"
        if _stale:
            st.warning(f"⚠️ 訊號日 {_d_display} 晚於預期執行日 — daily_scan 可能中斷。建議按「重新整理」或等下次自動掃描。")
        elif _scan_not_yet_today:
            st.info(f"⏳ 今日 daily_scan 尚未完成（預定 16:35）。目前顯示的是 **{_d_display}** 的掃描結果，完成後請按「重新整理」查看最新訊號。")

        # FIX #1+#10: 判斷 pending 是「今天要執行」還是「明天要執行」
        _today_dt = tw_today()
        _pending_sells_data = (_scan_data or {}).get("pending_sells") or []
        _pending_buy_data = (_scan_data or {}).get("pending_buy")
        _max_pos = int(_sp.get("max_positions", 2))

        # pending 的執行日 = scan_results 日期的下一個交易日
        # 如果執行日 <= 今天 → 今天應該已經執行了（等 daily_scan 16:35 確認）
        # 如果執行日 > 今天 → 明天執行
        _pending_is_for_today = (_nd <= _today_dt)

        if _pending_is_for_today and (_pending_sells_data or _pending_buy_data):
            st.markdown(f"**訊號日：{_d_display}（D）→ 今天 {_nd_str}（{_wd[_nd.weekday()]}）執行**")
            st.caption("16:35 daily_scan 執行後，下方 pending 會更新為明天的訊號。")
        else:
            st.markdown(f"**訊號日：{_d_display}（D）→ {_nd_str}（{_wd[_nd.weekday()]}）執行（D+1）**")

        # FIX #8: 只在 pending 有資料時顯示（不比對 local _sell_list 避免假警告）

        if _pending_sells_data or _pending_buy_data:
            _has_swap = True
            # 顯示 pending sells
            for _ps in _pending_sells_data:
                st.error(
                    f"**📤 賣出** {_ps.get('name','')}（{_ps.get('ticker','')}）\n\n"
                    f"原因：{_ps.get('reason','')}"
                )
            # 顯示 pending buy
            if _pending_buy_data:
                _bp1 = _pending_buy_data.get('close', 0)
                _bp_display = f"{_bp1}" if _bp1 > 0 else "（無價格）"
                _exec_label = f"今天（{_nd_str}）" if _pending_is_for_today else f"{_nd_str}（{_wd[_nd.weekday()]}）"
                st.success(
                    f"**🎯 買入** {_pending_buy_data.get('name', '')}（{_pending_buy_data.get('ticker', '')}）\n\n"
                    f"評分 {int(_pending_buy_data.get('score', 0))} 分｜收盤價 {_bp_display}｜"
                    f"**{_exec_label} 13:25 前買入**"
                )
            # FIX #5: 空位編號正確（有 pending_buy 時從 2 開始，沒有時從 1 開始）
            _slots_after_pending = len(_bt_holding) - len(_pending_sells_data) + (1 if _pending_buy_data else 0)
            if _slots_after_pending < _max_pos:
                _n_empty = _max_pos - _slots_after_pending
                _slot_start = 2 if _pending_buy_data else 1
                _next_scan_d = _nd if _pending_is_for_today else next_trading_day(str(_nd), _full_trading_cal or trading_cal)
                _next_buy_d = next_trading_day(str(_next_scan_d), _full_trading_cal or trading_cal)
                for _ei in range(_n_empty):
                    st.info(f"第 {_ei + _slot_start} 個空位：{_next_scan_d.strftime('%m/%d')}（{_wd[_next_scan_d.weekday()]}）掃描 → "
                            f"{_next_buy_d.strftime('%m/%d')}（{_wd[_next_buy_d.weekday()]}）買入")
        elif _sell_list:
            # Fallback: scan_results 沒有 pending 欄位（舊版 daily_scan）→ 用本地計算
            _has_swap = True
            for _s in _sell_list:
                _bd_display = f"（{_s.get('buy_date','')} 買）" if _s.get("buy_date") else ""
                st.error(
                    f"**📤 賣出** {_s['name']}（{_s['ticker']}）{_bd_display}\n\n"
                    f"原因：{_s['reason']}｜報酬 {_s['ret']:+.1f}%｜持有 {_s['dh']} 交易日"
                )
        else:
            _held_count = len(_bt_holding)
            _max_p = int(_sp.get("max_positions", 2))
            if _held_count < _max_p:
                _empty = _max_p - _held_count
                st.warning(f"📭 {_held_count}/{_max_p} 檔持有中，{_empty} 個空位。"
                           f"今日掃描**無達標候選**（score < {int(_sp.get('buy_threshold', 8))}），"
                           f"明日 16:35 再掃描。")
            else:
                st.info(f"目前沒有要換股（{_held_count} 檔持有中，無賣出訊號）")
        st.markdown("---")

    # === Auto-extend backtest from GPU end to today ===
    # 規則：app.py 只填「過去完整交易日」，絕不模擬「今天」
    # 今天的交易是 daily_scan 16:35 的職責。若 daily_scan 未跑，用戶不該看到盤中半成品訊號
    bt_end = bt_stats.get("end_date", "")
    if bt_trades and trading_date and bt_end and trading_date > bt_end and trading_cal and history_cache:
        import numpy as _np
        from scanner import compute_indicators, score_stock

        _cache = history_cache.get("stocks", {}) if history_cache else {}
        _cache_updated = history_cache.get("updated", "") if history_cache else ""
        _sp = strategy_params
        _max_pos = int(_sp.get("max_positions", 2))
        _buy_th = _sp.get("buy_threshold", 10)

        # Gap handling: sell + buy on past trading days ONLY（嚴格 < trading_date）
        _all_cal = sorted(_full_trading_cal) if _full_trading_cal else sorted(trading_cal)
        _sim_dates = []
        try:
            _bt_end_d = date.fromisoformat(bt_end)
            _today_d_parsed = date.fromisoformat(trading_date)
            # 嚴格小於今天：絕不模擬今天
            _gap = [d for d in _all_cal if _bt_end_d < d < _today_d_parsed]
            for _gd in _gap:
                _sim_dates.append((_gd, True, False))  # 歷史日，use_states=False（只用 cache）
        except:
            pass

        # 警告：今天的交易尚未由 daily_scan 記錄
        try:
            import datetime as _dt_mod
            _now_tw = _dt_mod.datetime.now(_dt_mod.timezone(_dt_mod.timedelta(hours=8)))
            _is_trading_day_today = _today_d_parsed in (_all_cal or [])
            if _is_trading_day_today and bt_end < trading_date:
                if _now_tw.hour < 16 or (_now_tw.hour == 16 and _now_tw.minute < 40):
                    st.warning(f"⏳ 今日 daily_scan 尚未執行（預定 16:35），目前訊號為**昨日（{bt_end}）收盤後**的資料。請於 16:40 後重新整理以取得今日訊號。")
                else:
                    st.warning(f"⚠️ 今日 daily_scan 應已執行但 Web 資料尚未更新到 {trading_date}。可能網路或 GitHub Actions 異常，請稍候或手動觸發。")
        except:
            pass

        # Map cache dates: use top-level dates (fallback to per-stock for old format)
        _ref_stock = next(iter(_cache.values()), {}) if _cache else {}
        _stored_dates = history_cache.get("dates", []) or _ref_stock.get("dates", [])
        _date_to_idx = {}
        if _stored_dates:
            # dates 存的是 string "2026-04-01"，轉成 date object
            _date_to_idx = {date.fromisoformat(d) if isinstance(d,str) else d: i for i, d in enumerate(_stored_dates)}
        else:
            # Fallback: guess from calendar (old cache without dates)
            try:
                _cache_end_d = date.fromisoformat(_cache_updated)
            except:
                _cache_end_d = date.fromisoformat(trading_date)
            _cal_up = sorted([d for d in _all_cal if d <= _cache_end_d]) if _cache_updated else []
            _cache_len = len(_ref_stock.get("c", []))
            _cache_dates_fb = _cal_up[-_cache_len:] if _cache_len > 0 else []
            _date_to_idx = {d: i for i, d in enumerate(_cache_dates_fb)}

        if _sim_dates and (_date_to_idx or market_data):
            sim_holdings = [dict(t) for t in bt_trades if t.get("reason") == "持有中"]
            bt_trades = [t for t in bt_trades if t.get("reason") != "持有中"]

            for sim_day, _can_buy, _use_states in _sim_dates:
                sd_str = str(sim_day)

                # Build market data for this day from cache or live API
                _dmkt = {}
                if sd_str == trading_date and market_data:
                    _dmkt = market_data
                elif sim_day in _date_to_idx:
                    _idx = _date_to_idx[sim_day]
                    for tk, cs in _cache.items():
                        if _idx < len(cs["c"]):
                            _dmkt[tk] = {"close":cs["c"][_idx],"high":cs["h"][_idx],"low":cs["l"][_idx],"vol":cs["v"][_idx]}
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
                    ret = (cur/bp-1)*100 - 0.585 if bp > 0 else 0  # match GPU: subtract transaction cost
                    from trading_days import count_between
                    dh = count_between(h.get("buy_date",""), str(sim_day),
                                        fallback_calendar=[str(d) for d in _all_cal] if _all_cal else None)
                    pk = max(h.get("peak_price", bp), cur); h["peak_price"] = pk
                    if dh < 1: _new_h.append(h); continue
                    # Delegate to shared sell_rules (matches kernel 1:1)
                    from sell_rules import should_sell
                    _cs_c_sim = list(_cache[tk]["c"]) if tk in _cache else None
                    # Compute indicators if strategy uses indicator-based sell conditions
                    _ind_ext = None
                    if tk in _cache and any(_sp.get(k, 0) for k in ("use_rsi_sell", "use_macd_sell", "use_kd_sell", "sell_vol_shrink", "use_mom_exit")):
                        try:
                            _ei_end = _date_to_idx.get(sim_day, len(_cache[tk]["c"])-1) + 1
                            if _ei_end >= 20:
                                _ind_ext = compute_indicators(
                                    _np.array(_cache[tk]["c"][:_ei_end], dtype=_np.float64),
                                    _np.array(_cache[tk]["h"][:_ei_end], dtype=_np.float64),
                                    _np.array(_cache[tk]["l"][:_ei_end], dtype=_np.float64),
                                    _np.array(_cache[tk]["v"][:_ei_end], dtype=_np.float64))
                        except Exception:
                            pass
                    reason = should_sell(bp, cur, pk, dh, _sp, cache_closes=_cs_c_sim, indicators=_ind_ext)
                    if reason:
                        # 理由格式統一（去掉 % 數，跟 GPU 一致）
                        for _pf, _cl in [("移動停利","移動停利"),("保本","保本出場"),("停損","停損"),
                                          ("停利","停利"),("跌破","跌破均線"),("停滯","停滯出場"),
                                          ("漸進","漸進停利"),("鎖利","鎖利出場"),("動量","動量反轉"),
                                          ("到期","到期"),("RSI","RSI超買"),("MACD","MACD死叉"),
                                          ("KD","KD死叉"),("量能","量縮")]:
                            if reason.startswith(_pf):
                                reason = _cl; break
                        # D+1 執行日（GPU sell 在 D+1 open）
                        _next_sim = sim_day + timedelta(days=1)
                        while _next_sim.weekday() >= 5:
                            _next_sim += timedelta(days=1)
                        bt_trades.append({"ticker":tk,"name":h.get("name",""),"buy_price":bp,
                            "sell_price":round(cur,2),"hold_days":dh,"return_pct":round(ret,1),
                            "reason":reason,"buy_date":h["buy_date"],"sell_date":str(_next_sim)})
                    else: _new_h.append(h)
                sim_holdings = _new_h

                # BUY (every gap day using cache indicators, today using states)
                if _can_buy and len(sim_holdings) < _max_pos:
                    _held = {h["ticker"] for h in sim_holdings}
                    _sigs = []
                    for tk in _top100:
                        if tk in _held or tk not in _cache: continue
                        cs = _cache[tk]
                        if sim_day not in _date_to_idx: continue
                        _ei = _date_to_idx[sim_day] + 1
                        if _ei < 20: continue
                        try:
                            _c = _np.array(cs["c"][:_ei],dtype=_np.float64)
                            _h = _np.array(cs["h"][:_ei],dtype=_np.float64)
                            _l = _np.array(cs["l"][:_ei],dtype=_np.float64)
                            _v = _np.array(cs["v"][:_ei],dtype=_np.float64)
                            # 新加：若 cache 有 open 陣列，傳進去算精確 consecutive_green / gap_up
                            _o_raw = cs.get("o", [])
                            _o = _np.array(_o_raw[:_ei], dtype=_np.float64) if _o_raw and len(_o_raw) >= _ei else None
                            _istates = indicator_states.get("states",{}) if indicator_states else {}
                            if _use_states and tk in _istates:
                                from scanner import compute_indicators_with_state
                                ind = compute_indicators_with_state(_c,_h,_l,_v,_istates[tk], o=_o)
                            else:
                                ind = compute_indicators(_c,_h,_l,_v, o=_o)
                            _sc = score_stock(ind,_sp)
                            if ind and _sc >= _buy_th:
                                _nm = ""
                                if market_data and tk in market_data:
                                    _nm = market_data[tk].get("name", "")
                                _vr = round(ind.get("vol_ratio",1.0),1)
                                _sigs.append({"tk":tk,"sc":_sc,"vol_ratio":_vr,
                                    "name":_nm or tk.replace(".TW","").replace(".TWO",""),"price":_dmkt[tk]["close"]})
                        except: continue
                    if _sigs:
                        _sigs.sort(key=lambda x:(-x["sc"],-x.get("vol_ratio",0),x.get("tk","")))
                        for s in _sigs[:1]:  # Only buy #1 per day (matching GPU)
                            # D+1 執行日（GPU buy 在 D+1 close）
                            _next_buy = sim_day + timedelta(days=1)
                            while _next_buy.weekday() >= 5:
                                _next_buy += timedelta(days=1)
                            sim_holdings.append({"ticker":s["tk"],"name":s["name"],"buy_price":s["price"],
                                "buy_date":str(_next_buy),"peak_price":s["price"],"sell_price":s["price"],
                                "hold_days":0,"return_pct":0,"reason":"持有中"})

            # Update holding with latest prices
            for h in sim_holdings:
                tk = h["ticker"]
                if market_data and tk in market_data:
                    cur = market_data[tk]["close"]
                    h["sell_price"] = round(cur,2)
                    h["return_pct"] = round((cur/h["buy_price"]-1)*100,1) if h["buy_price"]>0 else 0
                    from trading_days import count_between
                    h["hold_days"] = count_between(h.get("buy_date",""), trading_date,
                                                    fallback_calendar=[str(d) for d in _all_cal] if _all_cal else None)

            bt_trades = sorted(bt_trades + sim_holdings, key=lambda t: t.get("buy_date", ""))
            # Bug fix: 只把 end_date 設到實際模擬過的最後一天，不要假裝走到今天
            if _sim_dates:
                _last_sim_day = _sim_dates[-1][0]
                bt_stats["end_date"] = str(_last_sim_day)
            # Bug fix: 和 daily_scan 一致，要重算所有 stats（之前只更新 end_date）
            _ext_completed = [t for t in bt_trades if t.get("reason") != "持有中"]
            _ext_rets = [t.get("return_pct", 0) for t in _ext_completed]
            _ext_wins = [r for r in _ext_rets if r > 0]
            _ext_losses = [r for r in _ext_rets if r <= 0]
            bt_stats["total_trades"] = len(_ext_completed)
            bt_stats["total_return_pct"] = round(sum(_ext_rets), 1)
            bt_stats["win_rate"] = round(len(_ext_wins) / len(_ext_rets) * 100, 1) if _ext_rets else 0
            bt_stats["avg_return"] = round(sum(_ext_rets) / len(_ext_rets), 1) if _ext_rets else 0
            bt_stats["avg_win"] = round(sum(_ext_wins) / len(_ext_wins), 1) if _ext_wins else 0
            bt_stats["avg_loss"] = round(sum(_ext_losses) / len(_ext_losses), 1) if _ext_losses else 0
            bt_stats["max_win"] = round(max(_ext_rets), 1) if _ext_rets else 0
            bt_stats["max_loss"] = round(min(_ext_rets), 1) if _ext_rets else 0
            bt_stats["avg_hold_days"] = round(sum(t.get("hold_days", 0) for t in _ext_completed) / len(_ext_completed), 1) if _ext_completed else 0
            # FIX C3+M5: auto-extension 只在本地顯示，不寫 Gist
            # 避免跟 daily_scan 的 2-phase pending 機制衝突（race condition）
            # daily_scan 是唯一寫 backtest_results 的來源

    if bt_stats:
        # Bug fix: 回測期間用「首筆交易」到「末筆/今天」而非「資料起始」
        # kernel 有 60 天指標暖機，資料起始 ≠ 交易起始
        _data_start = bt_stats.get('start_date', '')
        _data_end = bt_stats.get('end_date', '')
        _all_buy_dates = sorted([t.get("buy_date", "") for t in bt_trades if t.get("buy_date")])
        _first_trade_date = _all_buy_dates[0] if _all_buy_dates else _data_start
        _total_days = _count_trading_days(_first_trade_date, _data_end)
        st.markdown(f"**回測期間**：{_first_trade_date} ~ {_data_end}（{_total_days} 交易日，首筆交易起算）")
        if _data_start and _first_trade_date and _data_start != _first_trade_date:
            st.caption(f"📝 資料自 {_data_start} 起，前 60 個交易日為指標暖機期（MA60/ADX 等需歷史資料）")
        st.caption(
            "💡 **價格為 yfinance 除權息調整後價**，不等於當時券商實際成交價。"
            "例：保瑞 2022-11-18 實際交易 ~308，因後續配息/配股調整後顯示 264。"
            "**報酬率 % 已含除權息**，是你實際會拿到的（與券商對 Yahoo『調整後收盤價』核對）。"
        )
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
            # Bug fix: CAGR 用「首筆交易 → 末筆/今天」的實際交易期
            _start_d = date.fromisoformat(_first_trade_date) if _first_trade_date else date.fromisoformat(bt_stats.get("start_date", "2022-01-01"))
            _end_d = date.fromisoformat(_data_end) if _data_end else date.fromisoformat(str(tw_today()))
            _years = max((_end_d - _start_d).days / 365.25, 1.0)
            _cagr = (_portfolio_growth ** (1 / _years) - 1) * 100 if _portfolio_growth > 0 else 0
        except:
            _cagr = 0

        # Max Drawdown: track equity curve (scaled by position size)
        _equity = 1.0
        _peak_eq = 1.0
        _max_dd = 0
        for r in _rets:
            _equity *= (1 + r * _pos_size / 100)
            # Bug fix: equity 降到 0 會讓後面 div/0；用 max 保底
            if _equity <= 0:
                _equity = 0.0001
            _peak_eq = max(_peak_eq, _equity)
            _dd = (_equity / _peak_eq - 1) * 100 if _peak_eq > 0 else 0
            _max_dd = min(_max_dd, _dd)

        # Sharpe Ratio (annualized, assume 252 trading days, risk-free = 0)
        import math
        if len(_rets) >= 2:
            _mean_r = sum(_rets) / len(_rets)
            _std_r = math.sqrt(sum((r - _mean_r) ** 2 for r in _rets) / (len(_rets) - 1))
            # Bug fix: _avg_hold fallback 從 12（猜測）改成用實際交易密度計算
            if _avg_hold > 1:
                _trades_per_year = 252 / _avg_hold
            elif len(_rets) > 0 and _years > 0:
                _trades_per_year = len(_rets) / _years  # 用實際交易密度，不是 12 魔術數
            else:
                _trades_per_year = 12
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
        # Bug fix: ∞ 閾值從 999 改成用 isfinite 判斷（profit factor 合理可達 20-100）
        c6.metric("盈虧比", f"{_wl_ratio:.2f}" if math.isfinite(_wl_ratio) else "∞")

        c7, c8, c9 = st.columns(3)
        c7.metric("Profit Factor", f"{_profit_factor:.2f}" if math.isfinite(_profit_factor) else "∞")
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
                _holding = t.get("reason") == "持有中"
                icon = "📌" if _holding else ("🟢" if ret > 0 else "🔴" if ret < 0 else "⚪")
                trade_rows.append({
                    "": icon,
                    "股票": t.get("name", "") or t.get("ticker", ""),
                    "買入日": t.get("buy_date", ""),
                    "賣出日": "—" if _holding else (t.get("sell_date", "") or "—"),
                    "買入價": t.get("buy_price", 0),
                    "賣出/現價": t.get("sell_price", 0),
                    "報酬%": f"{ret:+.1f}%",
                    "天數": t.get("hold_days", 0),
                    "狀態": "持有中" if _holding else t.get("reason", ""),
                })
            df_trades = pd.DataFrame(trade_rows)
            st.dataframe(df_trades, use_container_width=True, hide_index=True, height=500)

            # Exit reason breakdown
            st.markdown("---")
            st.markdown("#### 出場原因分佈")
            reasons = {}
            _reason_map = {"停損":"停損","保本出場":"保本出場","保本":"保本出場","停利":"停利",
                           "移動停利":"移動停利","跌破MA60":"跌破MA60","跌破":"跌破均線",
                           "停滯出場":"停滯出場","漸進停利":"漸進停利","鎖利":"鎖利",
                           "到期":"到期","持有中":"持有中","換股":"換股",
                           "RSI":"RSI 超買","MACD":"MACD 死叉","KD":"KD 死叉",
                           "量縮":"量縮","量能萎縮":"量縮","動量反轉":"動量反轉"}
            for t in bt_trades:
                _raw = t.get("reason", "其他")
                _cat = "其他"
                for _k in _reason_map:
                    if _raw.startswith(_k):
                        _cat = _reason_map[_k]; break
                reasons[_cat] = reasons.get(_cat, 0) + 1
            for r, count in sorted(reasons.items(), key=lambda x: -x[1]):
                st.caption(f"  {r}：{count} 次")
    else:
        st.info("回測資料準備中...歷史資料下載完成後會自動顯示。")
