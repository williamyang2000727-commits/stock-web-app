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

# ── Strategy Tag (main = 89.905, short = 短波段第二策略) ────
# 必須在 set_page_config 之前讀，才能動態決定 page_title
STRATEGY_TAG = st.secrets.get("strategy_tag", "main")
_IS_SHORT = STRATEGY_TAG == "short"
_SITE_TITLE = "Yang's 短波段第二策略" if _IS_SHORT else "Yang's 選股系統"
_SITE_ICON = "⏱️" if _IS_SHORT else "📈"

# ── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title=_SITE_TITLE,
    page_icon=_SITE_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Secrets ──────────────────────────────────────────────────
GITHUB_TOKEN = st.secrets["github_token"]
DATA_GIST_ID = st.secrets["data_gist_id"]
HISTORY_GIST_ID = st.secrets.get("history_gist_id", DATA_GIST_ID)
STATE_GIST_ID = st.secrets.get("state_gist_id", DATA_GIST_ID)
GPU_GIST_ID = st.secrets.get("gpu_gist_id", "c1bef892d33589baef2142ce250d18c2")  # GPU evolution pushes here


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
        st.markdown(f"# {_SITE_ICON} {_SITE_TITLE}")
        st.caption("Taiwan Stock Selection System" + (" — 短波段（hold 10 天）" if _IS_SHORT else ""))
        st.markdown("---")
        with st.form("login"):
            username = st.text_input("帳號").strip().lower()
            password = st.text_input("密碼", type="password")
            if st.form_submit_button("登入", use_container_width=True):
                if username and password:
                    users = dict(st.secrets.get("users", {}))
                    users.pop("rbcy06", None)
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
    # Check BOTH exchanges: TWSE(.TW) and TPEx(.TWO) must each have data
    _tw_n = sum(1 for k in data if ".TW" in k and ".TWO" not in k)
    _otc_n = sum(1 for k in data if ".TWO" in k)
    if _tw_n < 200 or _otc_n < 200:
        raise RuntimeError(f"Market data incomplete: TWSE={_tw_n} TPEx={_otc_n} (need 200+ each)")
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
    prev = portfolios.get(username, {}) if isinstance(portfolios.get(username), dict) else {}
    portfolios[username] = {
        "holdings": holdings,
        "updated": tw_now().isoformat(),
        "last_checked": prev.get("last_checked", ""),
        "telegram_chat_id": prev.get("telegram_chat_id", ""),
    }
    return write_gist_file("portfolios.json", portfolios, clear_cache=clear_cache)


def touch_last_checked(username):
    """每天每用戶最多 patch 一次 last_checked（證明 Web 有開過 + check_sell_signals 跑過）"""
    portfolios = read_gist_file("portfolios.json")
    if not isinstance(portfolios, dict) or username not in portfolios:
        return False
    user_data = portfolios[username]
    if not isinstance(user_data, dict):
        return False
    today_str = tw_today().isoformat()
    last = (user_data.get("last_checked") or "")[:10]
    if last == today_str:
        return False
    user_data["last_checked"] = tw_now().isoformat()
    portfolios[username] = user_data
    return write_gist_file("portfolios.json", portfolios, clear_cache=False)


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
    st.caption(f"{_SITE_ICON} {_SITE_TITLE} v1.0")

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

# ── Buy Rankings: 永遠用 daily_scan 的 Gist 結果（穩定、一致）──
# 不再跑 live scan — Streamlit Cloud 的 TWSE API 不穩定，
# 導致排行在 live/Gist 之間跳來跳去。daily_scan 在 GitHub Actions
# 上跑，TWSE 每次都成功（7000+ 支），結果可靠。
scan = read_gist_file("scan_results.json")
if not scan or not scan.get("buy_signals"):
    scan = read_gist_file("scan_results.json")

scan_date = scan.get("date", "") if scan else ""

# ── Trading Calendar ──
@st.cache_data(ttl=3600, show_spinner=False)  # 1 hour (was 24h)
def _get_trading_cal():
    from scanner import fetch_trading_calendar
    return fetch_trading_calendar()

@st.cache_data(ttl=3600, show_spinner=False)  # 1 hour (was 7 days — too long, holiday days got stale)
def _get_full_trading_cal():
    from scanner import fetch_trading_calendar
    return fetch_trading_calendar(months=48)


def _get_twse_ex_dividend_tickers(date_str):
    """Look up TWSE 上市 ex-dividend tickers for a given date (YYYY-MM-DD).
    Reads from Data Gist's ex_dividend.json, which daily_scan (GitHub Actions)
    refreshes every trading day 16:35 TW time. We read via Gist because
    Streamlit Cloud (US egress) cannot reliably reach TWSE directly.

    Return semantics:
    - set(tickers)  → key exists, that day has ex-dividend stocks
    - set()         → key absent BUT cache was refreshed AFTER date_str's
                      16:35 cron, meaning daily_scan saw the schedule and
                      that day had no ex-dividend events
    - None          → cache hasn't been refreshed since date_str's 16:35
                      yet (e.g. cron failed, or looking at a future date),
                      so we genuinely don't know
    """
    try:
        ex_data = read_gist_file("ex_dividend.json") or {}
        tickers_by_date = ex_data.get("tickers_by_date", {})
        if date_str in tickers_by_date:
            return set(tickers_by_date[date_str])
        # Key absent: check if cache was refreshed after date_str's 16:35 cron.
        # If yes → daily_scan ran and confirmed no ex-dividend → empty set.
        # If no → genuinely unknown → None.
        updated = ex_data.get("updated", "")
        if updated:
            try:
                upd_dt = datetime.fromisoformat(updated)
                # Compare against date_str at 16:35 TW (when cron writes)
                cutoff = datetime.fromisoformat(f"{date_str}T16:35:00+08:00")
                if upd_dt >= cutoff:
                    return set()  # confirmed no ex-dividend that day
            except Exception:
                pass
    except Exception:
        pass
    return None


def _format_drop_warning(name, ticker, chg_pct, ex_set):
    """Message for a single-day drop >=5%.
    ex_set is a set of ticker codes TWSE marked ex-dividend on trading_date,
    or None when that day hasn't been cached yet (e.g. cron not run, or 上櫃).
    stop_loss is pulled live from strategy_params so the message stays
    correct when the deployed strategy changes."""
    pure = ticker.split(".")[0]
    is_otc = ticker.endswith(".TWO")
    _sl_raw = (strategy_params or {}).get("stop_loss")
    _sl_txt = f"{_sl_raw:g}%" if isinstance(_sl_raw, (int, float)) else "策略停損"
    if ex_set is not None and not is_otc:
        if pure in ex_set:
            return (f"⚠️ **{name}（{ticker}）TWSE 確認今日除權除息（單日跌 {chg_pct:.1f}%）** — "
                    f"實盤會拿股息/配股。請到「持倉管理」手動調整 buy_price。")
        return (f"🔻 **{name}（{ticker}）單日重跌 {chg_pct:.1f}%（非除權除息）** — "
                f"TWSE 確認今日無除權息公告，是真實下跌。若仍在策略停損範圍（{_sl_txt}）內則繼續持有，"
                f"不要動 buy_price。")
    return (f"🔻 **{name}（{ticker}）單日重跌 {chg_pct:.1f}%** — 兩種可能：\n"
            f"  1. 真實下跌 → 若仍在策略停損範圍（{_sl_txt}）內則繼續持有，不要動 buy_price。\n"
            f"  2. 除權除息（上櫃或 Gist 尚未更新）→ 請到銀行券商 App 確認配息公告，"
            f"有股息則到「持倉管理」調 buy_price。")


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
        else:
            touch_last_checked(username)
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

# ══════════════════════════════════════════════════════════════
# ⭐ Pipeline 新鮮度警告（全域，4 個 tab 都看得到）
# Pipeline 每日 16:30 跑（auto_daily_pipeline.py），重置 state Gist + Tab 3
# 跑完前所有訊號（Tab 0 訊號 / Tab 1 買入排行 / Tab 3 回測）都可能是 daily_scan 80 天版本
# 跟 cpu_replay 1500 天真公式可能偏差（曾把達邁排第 1 但 cpu_replay 應選聯茂）
# ══════════════════════════════════════════════════════════════
import datetime as _dt_pipe
_now_tw_pipe = _dt_pipe.datetime.now(_dt_pipe.timezone(_dt_pipe.timedelta(hours=8)))
_bt_for_freshness = read_gist_file("backtest_results.json")
_pipeline_updated = (_bt_for_freshness or {}).get("stats", {}).get("pipeline_updated", "")
_pipeline_today = _now_tw_pipe.strftime("%Y-%m-%d")
_pipeline_ran_today = _pipeline_updated.startswith(_pipeline_today)
# 用交易日曆判斷今天是否為交易日（含國定假日），fallback 才用週一~週五
_today_pipe = _now_tw_pipe.date()
if trading_cal:
    _is_trading_day_pipe = _today_pipe in trading_cal
else:
    _is_trading_day_pipe = _now_tw_pipe.weekday() < 5
_after_settle = _now_tw_pipe.hour >= 17 or (_now_tw_pipe.hour == 16 and _now_tw_pipe.minute >= 30)

if _is_trading_day_pipe and _after_settle and not _pipeline_ran_today:
    if not _pipeline_updated:
        st.warning(
            f"⚠️ **這是舊資料（daily_scan 80 天版本，可能跟 cpu_replay 真公式偏差）**\n\n"
            f"自動 pipeline 還沒跑（每日 16:30 Windows 排程）。"
            f"訊號 / 買入排行 / 回測都可能是失真版，僅供參考。"
        )
    else:
        try:
            _pu_dt = _dt_pipe.datetime.fromisoformat(_pipeline_updated)
            _hours_old = (_now_tw_pipe - _pu_dt).total_seconds() / 3600
            if _hours_old > 24:
                st.warning(
                    f"⚠️ **這是舊資料 — Pipeline 已 {_hours_old:.0f} 小時沒跑**（最後 {_pipeline_updated[:19]}）\n\n"
                    f"訊號 / 買入排行 / 回測都可能跟 cpu_replay 真公式偏差。Windows 排程可能掛了。"
                )
        except Exception:
            pass
elif _pipeline_ran_today:
    # 從 backtest stats 拿真實全期天數（每天會增加）
    _real_days = (_bt_for_freshness or {}).get("stats", {}).get("total_days", "?")
    st.success(f"✅ Pipeline 今日 {_pipeline_updated[11:19]} 已跑 — 所有資料對齊 cpu_replay 真公式（全期 {_real_days} 天）")

# ── Tabs ──
# 篩選器 Tab 4 只在主策略 Web 顯示（短波段不需要 — 5/12 加）
# 因為篩選器用主 Data Gist 的 screener_results.json + golden_optimal_hold.json
# 短波段 Data Gist 沒這兩個檔，顯示會錯亂
if _IS_SHORT:
    # 短波段：6 個 Tab，無 Tab 4 篩選器，多 Tab 5 投信突襲 + Tab 6 題材熱度
    tab0, tab1, tab2, tab3, tab5, tab6 = st.tabs([signal_label, "📊 買入排行", "💼 持倉管理", "📋 回測績效", "🏦 投信突襲", "🔥 題材熱度"])
    tab4 = st.container()
    _tab4_active = False
    _tab5_active = True
    _tab6_active = True
else:
    # 主策略：5 個 Tab，有 Tab 4 篩選器，無 Tab 5/6
    tab0, tab1, tab2, tab3, tab4 = st.tabs([signal_label, "📊 買入排行", "💼 持倉管理", "📋 回測績效", "🔍 篩選器"])
    _tab4_active = True
    tab5 = st.container()
    tab6 = st.container()
    _tab5_active = False
    _tab6_active = False

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
        _user_ex_set = _get_twse_ex_dividend_tickers(trading_date)
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
                            _format_drop_warning(_unm, _utk, _uchg, _user_ex_set)
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
            _sp_display = f"{_sp_close:.2f} 元" if _sp_close > 0 else "（無價格）"
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

    # ── Last checked indicator (證明監控有跑) ──
    _user_meta = portfolios.get(username, {}) if isinstance(portfolios, dict) else {}
    _last_chk = _user_meta.get("last_checked", "")
    _last_upd = _user_meta.get("updated", "")
    if _last_chk:
        _chk_date = _last_chk[:10]
        _today_str = tw_today().isoformat()
        if _chk_date == _today_str:
            st.caption(f"✅ 監控狀態：今日 {_last_chk[11:16]} 已檢查（套用{'短波段' if _IS_SHORT else ' 89.905 '}賣出規則）")
        else:
            st.caption(f"⚠️ 監控狀態：上次檢查 {_chk_date}（今日尚未開過 Web，daily_scan 雲端仍會跑）")
    elif _last_upd:
        st.caption(f"ℹ️ 監控狀態：持倉建立於 {_last_upd[:10]}（首次開 Web 後會記錄今日檢查時間）")

    # ── Telegram 警報設定（每個 user 自己填 chat_id）──
    with st.expander("🔔 Telegram 警報設定（觸發賣出規則時自動推播）", expanded=False):
        _cur_chat = _user_meta.get("telegram_chat_id", "")
        _scan_time = "17:00" if _IS_SHORT else "16:35"
        _strat_name = "短波段第二策略" if _IS_SHORT else "89.905"
        _sell_rules_desc = (
            "停損 -12% / 停利 +80%（半關閉）/ 移動停利 -20% / 鎖利 +15% 跌到 +3% / 到期 10 天"
            if _IS_SHORT
            else "停損 -20% / 保本 / 停利 +40% / 移動停利 -20% / 鎖利 / 到期 30 天"
        )
        st.markdown(
            f"daily_scan 每天 {_scan_time} 雲端跑，持倉觸發 {_strat_name} 的賣出規則"
            f"（{_sell_rules_desc}）會立刻推 Telegram 給你。\n\n"
            "### 📲 取得 chat_id\n"
            "打開 Telegram → 搜尋 [`@getmyid_bot`](https://t.me/getmyid_bot) → 按 `START` → "
            "bot 立刻回你一串數字（例如 `1234567890`），那就是你的 chat_id。\n\n"
            "⚠️ **避雷**：Telegram 上有 spam bot 假冒 `@userinfobot`，回應「To use this bot you must join our channel」"
            "強迫你加頻道，那是釣魚 — 直接 BLOCK。請用上面的 `@getmyid_bot`。\n\n"
            "### 📲 啟用警報推播\n"
            "再去 [`@Yyang_stock_alert_bot`](https://t.me/Yyang_stock_alert_bot)（我們的警報 bot）→ 按 `START`\n"
            "_（沒對警報 bot 按過 START，Telegram 規則不允許 bot 主動傳訊息給你）_\n\n"
            "完成後，把第一步拿到的數字貼進下方 → 儲存。"
        )
        with st.form("telegram_form"):
            new_chat = st.text_input(
                "你的 chat_id",
                value=_cur_chat,
                placeholder="例：1234567890（從 @getmyid_bot 取得）",
            )
            if st.form_submit_button("💾 儲存設定", use_container_width=True):
                _v = new_chat.strip()
                if not _v or _v.lstrip("-").isdigit():
                    _portfolios = read_gist_file("portfolios.json")
                    if not isinstance(_portfolios, dict):
                        _portfolios = {}
                    _udata = _portfolios.get(username, {}) if isinstance(_portfolios.get(username), dict) else {}
                    _udata["holdings"] = _udata.get("holdings", user_holdings)
                    _udata["telegram_chat_id"] = _v
                    _udata["updated"] = tw_now().isoformat()
                    _portfolios[username] = _udata
                    if write_gist_file("portfolios.json", _portfolios):
                        if _v:
                            st.success(f"✅ 已儲存 chat_id={_v}。下次觸發賣出規則會推給你（記得也對 @Yyang_stock_alert_bot 按過 START）")
                        else:
                            st.success("已清空 chat_id")
                        st.rerun()
                    else:
                        st.error("儲存失敗")
                else:
                    st.error("chat_id 必須是純數字（例：1234567890）")

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

    # Pipeline 警告已移到全域顯示（line ~492），4 個 tab 都看得到
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
        _bt_ex_set = _get_twse_ex_dividend_tickers(trading_date)
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
            # 單日跌 >=5% 且有成交 → 查 TWSE 除權息 API 確認是除權息還是真跌
            if _last_c and _last_c > 0:
                _daily_change = (_today_c / _last_c - 1) * 100
                if _daily_change <= -5:
                    _edge_warnings.append(
                        _format_drop_warning(_nm, _tk, _daily_change, _bt_ex_set)
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
                f"評分 {int(_b0.get('score', 0))} 分｜收盤價 {_b0.get('close', 0):.2f}｜"
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
        # 提前定義（讓後面區塊也能用）
        _today_real = tw_today()
        _cal_list = sorted(_full_trading_cal or trading_cal or [])
        _is_trading_today = False
        if _cal_list:
            _is_trading_today = _today_real in _cal_list
        else:
            # 日曆 fetch 失敗時退回到週一~週五
            _is_trading_today = _today_real.weekday() < 5
        try:
            # 用真實日曆日期（tw_today），不用市場資料日期（trading_date）
            # 因為 trading_date 可能跟 scan_date 相同（都是今天），但 scan 其實是昨天跑的
            _scan_d = date.fromisoformat(_d_date) if _d_date else _today_real
            _days_since_scan = sum(1 for d in _cal_list if _scan_d < d <= _today_real) if _cal_list else 0
            _stale = _days_since_scan >= 2
            # scan 不是今天的 + 今天是交易日 → daily_scan 還沒跑
            if _scan_d < _today_real and _is_trading_today:
                _scan_not_yet_today = True
        except:
            _stale = False

        # FIX: daily_scan 還沒跑時，不要用 today 即時 market_data 算 cur
        # 原因：market_data 取得 today 收盤後，但 daily_scan 還沒寫 scan_results
        # → 換股狀態會搶先用「今日收盤」算 should_sell → 顯示 D+1 賣（偷跑）
        # 對齊 backtest_results bt_end：用 cache 末日 close 算 cur（= bt 末日狀態）
        # daily_scan 跑完後（scan_date == today），才用 market_data
        _use_live_cur = not _scan_not_yet_today  # daily_scan 已跑完才用 live
        for _bh in _bt_holding:
            _tk = _bh.get("ticker", "")
            _bp = _bh.get("buy_price", 0)
            _nm = _bh.get("name", _tk)
            if _bp <= 0:
                continue
            _cur = None
            if _use_live_cur and _tk in market_data:
                _cur = market_data[_tk]["close"]
            else:
                # daily_scan 還沒跑 → 用 cache 末日 close（= bt 末日，跟 Tab 3 交易明細一致）
                _cs_fallback = _swap_cache.get(_tk, {})
                _c_arr = _cs_fallback.get("c", [])
                if _c_arr:
                    _cur = _c_arr[-1]
                elif _tk in market_data:
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
                # daily_scan 已跑完才把 today close append（防偷跑）
                if _cs_c is not None and _use_live_cur and market_data and _tk in market_data:
                    _cs_c = _cs_c + [market_data[_tk]["close"]]  # FIX M3: append today's close for MA60
                # Compute indicators if strategy uses indicator-based sell conditions
                _ind_t3 = None
                if _tk in _swap_cache and any(_sp.get(k, 0) for k in ("use_rsi_sell", "use_macd_sell", "use_kd_sell", "sell_vol_shrink", "use_mom_exit")):
                    try:
                        import numpy as _np_t3
                        from scanner import compute_indicators as _ci_t3
                        _cs_t3 = _swap_cache[_tk]
                        # 防偷跑：scan 沒跑完不 append today
                        _append_today = _use_live_cur and (_tk in market_data)
                        _c_t3 = _np_t3.array(list(_cs_t3["c"]) + ([market_data[_tk]["close"]] if _append_today else []), dtype=_np_t3.float64)
                        _h_t3 = _np_t3.array(list(_cs_t3["h"]) + ([market_data[_tk]["high"]] if _append_today else []), dtype=_np_t3.float64)
                        _l_t3 = _np_t3.array(list(_cs_t3["l"]) + ([market_data[_tk]["low"]] if _append_today else []), dtype=_np_t3.float64)
                        _v_t3 = _np_t3.array(list(_cs_t3["v"]) + ([market_data[_tk]["vol"]] if _append_today else []), dtype=_np_t3.float64)
                        _h250_t3 = _np_t3.array(list(_cs_t3.get("h250", [])) + ([market_data[_tk]["high"]] if _append_today else []), dtype=_np_t3.float64) if _cs_t3.get("h250") else None
                        _l250_t3 = _np_t3.array(list(_cs_t3.get("l250", [])) + ([market_data[_tk]["low"]] if _append_today else []), dtype=_np_t3.float64) if _cs_t3.get("l250") else None
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
        # 判斷今天是否為休市日。3 個條件：
        # (A) 今天是週六/週日 → 必休市（最可靠）
        # (B) cal_list 有「未來日」（信任度高）AND 今天不在 cal → 國定假日
        #     ⚠️ 不能只看「不在 cal」，因為 TWSE 盤後才會把今天加進 cal，
        #     盤中/早盤呼叫時今天不在 cal 不代表休市（5/12 bug 根因）
        # (C) pipeline 今天有跑 AND scan_results.date < 今天 → pipeline 偵測到沒新交易日
        _today_is_holiday = False
        try:
            # (A) 週末判斷（鐵律）
            _is_weekend = _today_real.weekday() >= 5
            # (B) 日曆判斷 — 只在 cal_list 含「未來日」才信任（代表 TWSE 已標示假期）
            _cal_has_future = bool(_cal_list) and any(d > _today_real for d in _cal_list)
            _by_cal = _cal_has_future and (_today_real not in _cal_list)
            # (C) pipeline 證據：pipeline 跑完但寫的還是舊日期
            _scan_d_check = date.fromisoformat(_d_date) if _d_date else None
            _by_pipeline = bool(_pipeline_ran_today) and bool(_scan_d_check) and (_scan_d_check < _today_real)
            _today_is_holiday = _is_weekend or _by_cal or _by_pipeline
        except Exception:
            _today_is_holiday = False
        # 下個交易日（給休市訊息用）
        try:
            _next_trading_after_today = next((d for d in _cal_list if d > _today_real), None) if _cal_list else None
        except Exception:
            _next_trading_after_today = None
        _next_label_full = (
            f"{_next_trading_after_today.strftime('%m/%d')}（{_wd[_next_trading_after_today.weekday()]}）"
            if _next_trading_after_today else "下個交易日"
        )

        if _stale:
            st.warning(f"⚠️ 訊號日 {_d_display} 晚於預期執行日 — daily_scan 可能中斷。建議按「重新整理」或等下次自動掃描。")
        elif _today_is_holiday:
            st.info(f"🏖️ 今日（{_today_real.strftime('%m/%d')}）台股休市，daily_scan 不會跑。目前顯示的是 **{_d_display}** 的掃描結果，{_next_label_full} 16:35 會跑下次掃描。")
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
            # 顯示 pending sells（補從 _bt_holding 算缺的欄位）
            for _ps in _pending_sells_data:
                _ps_tk = _ps.get("ticker", "")
                _ps_ret = _ps.get("return_pct")
                _ps_dh = _ps.get("days_held")
                _ps_bd = _ps.get("buy_date", "")
                # fallback from _bt_holding
                if _ps_ret is None or _ps_dh is None or not _ps_bd:
                    for _bh in _bt_holding:
                        if _bh.get("ticker") == _ps_tk:
                            if not _ps_bd:
                                _ps_bd = _bh.get("buy_date", "")
                            if _ps_dh is None:
                                from trading_days import count_between
                                _ps_dh = count_between(_ps_bd, str(tw_today()),
                                                       fallback_calendar=[str(d) for d in (_full_trading_cal or trading_cal)] if (_full_trading_cal or trading_cal) else None)
                            if _ps_ret is None:
                                _bh_bp = _bh.get("buy_price", 0)
                                _bh_cur = _bh.get("display_price") or _bh.get("sell_price", 0)
                                if _bh_bp > 0 and _bh_cur:
                                    _ps_ret = round((_bh_cur / _bh_bp - 1) * 100, 2)
                            break
                _bd_disp = f"（{_ps_bd} 買）" if _ps_bd else ""
                _detail_parts = [f"原因：{_ps.get('reason','')}"]
                if _ps_ret is not None:
                    _detail_parts.append(f"報酬 {_ps_ret:+.1f}%")
                if _ps_dh is not None:
                    _detail_parts.append(f"持有 {_ps_dh} 交易日")
                st.error(
                    f"**📤 賣出** {_ps.get('name','')}（{_ps.get('ticker','')}）{_bd_disp}\n\n"
                    + "｜".join(_detail_parts)
                )
            # 顯示 pending buy
            if _pending_buy_data:
                _bp1 = _pending_buy_data.get('close', 0)
                _bp_display = f"{_bp1:.2f}" if _bp1 and _bp1 > 0 else "（無價格）"
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
                    f"原因：{_s['reason']}｜報酬 {_s['ret']:+.1f}%（已扣 0.585% 摩擦成本）｜持有 {_s['dh']} 交易日"
                )
        else:
            _held_count = len(_bt_holding)
            _max_p = int(_sp.get("max_positions", 2))
            # 算「下次 daily_scan 會在哪天」 = 今天若是交易日就是明天，今天若休市就是下個交易日
            try:
                _next_scan_day = next((d for d in _cal_list if d > _today_real), None) if _cal_list else None
            except Exception:
                _next_scan_day = None
            _next_scan_label = (
                f"{_next_scan_day.strftime('%m/%d')}（{_wd[_next_scan_day.weekday()]}）"
                if _next_scan_day else "下個交易日"
            )
            if _held_count < _max_p:
                _empty = _max_p - _held_count
                st.warning(f"📭 {_held_count}/{_max_p} 檔持有中，{_empty} 個空位。"
                           f"今日掃描**無達標候選**（score < {int(_sp.get('buy_threshold', 8))}），"
                           f"{_next_scan_label} 16:35 再掃描。")
            else:
                # 列出持倉名稱，明確告訴用戶「繼續持有不要動」
                _hold_names = "、".join(f"{h.get('name', '')}（{h.get('ticker','')}）" for h in _bt_holding)
                # 如果今天休市，標題改成「今日休市」而非「明日無動作」
                if _today_is_holiday:
                    _no_action_title = f"🏖️ 今日（{_today_real.strftime('%m/%d')}）台股休市 — 繼續持有 {_held_count} 檔"
                    _no_action_msg = f"今日無 daily_scan，{_next_scan_label} 16:35 才會跑下次掃描。"
                else:
                    _no_action_title = f"✋ **{_nd_str}（{_wd[_nd.weekday()]}）無動作 — 繼續持有 {_held_count} 檔**"
                    _no_action_msg = f"今日掃描無賣出訊號、滿倉無新買入。{_next_scan_label} 16:35 再掃描。"
                st.info(
                    f"{_no_action_title}\n\n"
                    f"持倉：{_hold_names}\n\n"
                    f"{_no_action_msg}"
                )
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
            "💡 **歷史交易價（買入/賣出）為 yfinance 除權息調整後價**，"
            "不等於當時券商實際成交價（例：保瑞 2022-11-18 實際交易 ~308，配息/配股調整後顯示 264）。"
            "**報酬率 % 已含除權息 + 已扣台股摩擦成本 0.585%**"
            "（買賣手續費 0.285% + 證交稅 0.3%），是你實際會拿到的。"
            "「持有中」現價已切換為 **TWSE/TPEx 官方未調整價**（與看盤軟體一致）。"
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
                _holding = t.get("reason") == "持有中"
                # 雙價系統：「持有中」優先用 display_price (TWSE/TPEx unadjusted) 顯示
                # display_price 由 rebuild_tab3 寫入；歷史完成 trade 用 sell_price (adjusted)
                if _holding and t.get("display_price"):
                    show_price = t.get("display_price")
                    ret = t.get("display_return_pct", t.get("return_pct", 0))
                else:
                    show_price = t.get("sell_price", 0)
                    ret = t.get("return_pct", 0)
                icon = "📌" if _holding else ("🟢" if ret > 0 else "🔴" if ret < 0 else "⚪")
                trade_rows.append({
                    "": icon,
                    "股票": t.get("name", "") or t.get("ticker", ""),
                    "買入日": t.get("buy_date", ""),
                    "賣出日": "—" if _holding else (t.get("sell_date", "") or "—"),
                    "買入價": t.get("buy_price", 0),
                    "賣出/現價": show_price,
                    "報酬%（淨）": f"{ret:+.1f}%",
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


# ══════════════════════════════════════════════════════════════
# TAB 4: SCREENER（5/12 新增：3 類股票篩選）
# ══════════════════════════════════════════════════════════════
with tab4:
  if _tab4_active:
      st.subheader("🔍 股票篩選器")
      st.caption("過去 22 個交易日內觸發過 3 類技術條件的股票（每天 Windows 16:35 pipeline 自動更新）")

      screener_data = read_gist_file("screener_results.json")

      # 🚨 資料新鮮度檢查（5/12 加：避免顯示過期資料而不自知）
      if screener_data:
          _scr_today = screener_data.get("today", "")
          _scr_updated = screener_data.get("updated", "")[:10]
          _today_tw_check = _now_tw_pipe.strftime("%Y-%m-%d") if '_now_tw_pipe' in dir() else date.today().isoformat()
          if _scr_today and _scr_today != _today_tw_check and _scr_updated != _today_tw_check:
              st.warning(
                  f"⚠️ **篩選資料是 {_scr_today or _scr_updated} 的（不是今天 {_today_tw_check}）**。"
                  f"Windows 16:35 pipeline 可能還沒跑或失敗。"
                  f"訊號清單可能過期，請等 pipeline 跑完或手動 `python auto_daily_pipeline.py --force`。"
              )

      if not screener_data:
          st.warning("篩選資料尚未產生，等 Windows 16:35 pipeline 第一次跑完即顯示。")
      else:
          # 摘要區
          st.markdown("---")
          stats = screener_data.get("stats", {})
          results_data = screener_data.get("results", {})

          # ═══════════════════════════════════════════
          # 🎯 推薦今天可進場 — 只列黃金組合（勝率最高）
          # ═══════════════════════════════════════════
          # 載入最佳 hold（折衷版：勝率+整齊，hold 8-14 天區間找勝率最高）
          golden_hold_data = read_gist_file("golden_optimal_hold.json") or {}
          # 優先用折衷版（hold 8-14），fallback 用最高勝率
          best_hold = (golden_hold_data.get("best_hold_balanced")
                       or golden_hold_data.get("best_hold_by_wr", 10))

          # 找出該 hold 的完整 perf
          _best_perf = None
          for hp in golden_hold_data.get("hold_perf", []):
              if hp["hold_days"] == best_hold:
                  _best_perf = hp
                  break
          _best_wr = _best_perf["wr"] if _best_perf else "?"
          _best_avg = _best_perf["avg_ret"] if _best_perf else "?"
          _best_exp = _best_perf["expected"] if _best_perf else "?"
          _best_pl = _best_perf["pl_ratio"] if _best_perf else "?"

          # 22 日近期觸發數（純資訊不顯示勝率，避免 7 樣本誤導 + timing 不一致）
          _golden_stats = stats.get("golden", {}).get("perf", {})
          _g_n_recent = _golden_stats.get("n", 0)

          st.markdown("### 🎯 推薦今天可進場（只列黃金組合）")
          st.caption(
              f"💎 **黃金組合（**同日 MACD AND 量爆 同時觸發**）— "
              f"{golden_hold_data.get('backtest_days', '?')} 天回測實證**："
              f"勝率 **{_best_wr}%** / 平均 {_best_avg}% / 期望值 {_best_exp}% / "
              f"盈虧比 {_best_pl} / {golden_hold_data.get('total_triggers', '?')} 個觸發點"
              f"｜ ⭐ **建議 hold = {best_hold} 天**"
              f"（D+1 收盤前買 → D+{best_hold+1} 開盤賣，對齊主策略 SOP）"
              f"｜ 📌 過去 22 日內觸發 {_g_n_recent} 檔（顯示在下方表格）"
          )

          cb_data = screener_data.get("confluence_buckets", {})
          golden_all = sorted(cb_data.get("golden", []),
                              key=lambda x: x.get("days_after", 99))

          # 🚨 只列「還在 hold 期內」的 (days_after < best_hold)
          # 已過 hold 天數的 = 早就該出場，不該顯示在「推薦今天可進場」
          golden = [r for r in golden_all if r.get("days_after", 99) < best_hold]
          n_expired = len(golden_all) - len(golden)

          if not golden:
              st.info(
                  f"過去 {best_hold} 個交易日內無黃金組合觸發 — 今天不進場，等下一天 "
                  f"(總共 {len(golden_all)} 個歷史觸發但都已過 {best_hold} 天 hold 期)"
              )
          else:
              golden_df = pd.DataFrame([{
                  "新鮮度": ("🟢 今天" if r.get("days_after", 99) == 0
                            else "🟡 昨天" if r.get("days_after", 99) == 1
                            else f"📅 {r.get('days_after', '?')} 天前"),
                  "股號": r["ticker"].split(".")[0],
                  "公司名": r["name"],
                  "目前價": r["current_price"],
                  "觸發日": r["trigger_date"],
                  "已持有": r.get("days_after", 0),
                  f"⏰ hold 剩餘（最佳{best_hold}天）": best_hold - r.get("days_after", 0),
                  "當天漲幅": f"{r.get('daily_return', 0):+.2f}%" if r.get('daily_return') is not None else "-",
                  "乖離MA20": f"{r.get('bias_MA20', 0):+.1f}%" if r.get('bias_MA20') is not None else "-",
                  "符合類別": " + ".join(r.get("confluence_tags", [])),
                  "浮動報酬": f"{r.get('ret_to_today', 0):+.2f}%",
              } for r in golden])
              st.dataframe(golden_df, use_container_width=True, hide_index=True,
                           height=min(450, len(golden) * 38 + 50))
              if n_expired > 0:
                  st.caption(f"📌 另有 {n_expired} 個歷史觸發已過 {best_hold} 天 hold 期（不顯示）")

          n_golden_today = sum(1 for r in golden if r.get("days_after", 99) == 0)
          n_golden_recent = len(golden)
          st.success(
              f"📌 **黃金組合 {n_golden_recent} 檔**（今天剛觸發 {n_golden_today} 檔）｜"
              f"**下單時機**：明日 13:25 收盤前買（對齊主策略 SOP）｜ "
              f"**最高勝率 hold**：{best_hold} 天（勝率 {_best_wr}%）｜ "
              f"**出場**：hold 滿開盤賣"
          )

          # 全期回測 hold 績效表（可展開）
          if golden_hold_data.get("hold_perf"):
              _bd = golden_hold_data.get('backtest_days', '?')
              _maxh = golden_hold_data.get('max_hold_tested', 30)
              with st.expander(f"📊 {_bd} 天回測：hold 1-{_maxh} 天完整績效（⭐ = 勝率最高 {best_hold} 天）"):
                  hp_rows = []
                  # 排序：勝率高到低（讓你一眼看勝率排名）
                  sorted_hp = sorted(golden_hold_data["hold_perf"], key=lambda x: -x["wr"])
                  for hp in sorted_hp:
                      is_best = hp["hold_days"] == best_hold
                      hp_rows.append({
                          "hold天": ("⭐ " if is_best else "") + str(hp["hold_days"]),
                          "樣本": hp["n_samples"],
                          "勝率": f"{hp['wr']:.1f}%",
                          "平均報酬(扣手續費)": f"{hp['avg_net']:+.2f}%",
                          "期望值(扣手續費)": f"{hp['expected_net']:+.2f}%",
                          "盈虧比": f"{hp['pl_ratio']:.2f}",
                          "最佳/最差": f"{hp['best']:+.1f}% / {hp['worst']:+.1f}%",
                      })
                  st.dataframe(pd.DataFrame(hp_rows), use_container_width=True, hide_index=True)
                  st.caption("依勝率高到低排序，⭐ = 全期最高勝率 hold 天數")

          st.markdown("---")

          # 5/12 簡化：刪掉「📊 各類完整績效」3 類表（只 KD / 只 量爆 / 只 MACD）
          # 這些單一類別資料不參與決策（只看黃金組合），刪掉讓頁面乾淨
          # 只保留底部 metadata caption
          st.caption(
              f"⏰ {screener_data.get('updated', '?')} ｜ "
              f"📅 cache 末日：{screener_data.get('today', '?')} ｜ "
              f"🔁 回顧期：{screener_data.get('lookback_days', 22)} 日 ｜ "
              f"📉 日均量 ≥ {screener_data.get('min_volume_lots', 2000)} 張"
          )

          results = screener_data.get("results", {})
          kd_list = results.get("kd_low", [])
          vol_list = results.get("volume_burst", [])
          macd_list = results.get("macd", [])

          # ─────── 用 screener 算好的加權 confluence buckets（業界共識，KD 不算）───────
          # 若沒有 confluence_buckets（舊版 Gist）→ fallback 同日 confluence
          cb = screener_data.get("confluence_buckets", None)
          if cb is not None and "golden" in cb:
              # 新版：MACD+量爆 黃金組合 + KD 純參考
              buckets = {
                  "🌟 黃金組合 (MACD + 量爆 5 日內)": cb.get("golden", []),
                  "🔵 只 MACD": cb.get("macd_only", []),
                  "🟠 只 量爆": cb.get("vol_only", []),
                  "📌 KD 低位 (僅參考，業界平均 46-50%)": cb.get("kd_reference", []),
              }
          elif cb is not None and "triple" in cb:
              # 中間版（5/12 早 commit 117beb9）：3 類滑動視窗
              buckets = {
                  "三冠王 (5日內 3 類)": cb.get("triple", []),
                  "中 2 類 (5日內任 2 類)": cb.get("double", []),
                  "只 1 類": cb.get("single", []),
              }
          else:
              # 舊版 fallback：同日 confluence（兼容）
              tag_map = {}
              for r in kd_list:
                  k = (r["ticker"], r["trigger_date"])
                  tag_map.setdefault(k, [set(), {}])
                  tag_map[k][0].add("KD")
                  tag_map[k][1].update({"kd_K": r["K"], "kd_D": r["D"]})
                  tag_map[k][1].update({"ticker": r["ticker"], "name": r["name"],
                                        "current_price": r["current_price"],
                                        "trigger_date": r["trigger_date"],
                                        "trigger_close": r["trigger_close"],
                                        "days_after": r["days_after"],
                                        "ret_to_today": r["ret_to_today"]})
              for r in vol_list:
                  k = (r["ticker"], r["trigger_date"])
                  tag_map.setdefault(k, [set(), {}])
                  tag_map[k][0].add("量爆")
                  tag_map[k][1].update({"vol_today": r["vol_today"], "vol_yest": r["vol_yest"],
                                        "vol_pre": r["vol_pre"],
                                        "ratio_1": r["ratio_1"], "ratio_2": r["ratio_2"]})
                  tag_map[k][1].update({"ticker": r["ticker"], "name": r["name"],
                                        "current_price": r["current_price"],
                                        "trigger_date": r["trigger_date"],
                                        "trigger_close": r["trigger_close"],
                                        "days_after": r["days_after"],
                                        "ret_to_today": r["ret_to_today"]})
              for r in macd_list:
                  k = (r["ticker"], r["trigger_date"])
                  tag_map.setdefault(k, [set(), {}])
                  tag_map[k][0].add("MACD")
                  tag_map[k][1].update({"macd_type": r["macd_type"], "DIF": r["DIF"],
                                        "MACD": r["MACD"], "OSC": r["OSC"]})
                  tag_map[k][1].update({"ticker": r["ticker"], "name": r["name"],
                                        "current_price": r["current_price"],
                                        "trigger_date": r["trigger_date"],
                                        "trigger_close": r["trigger_close"],
                                        "days_after": r["days_after"],
                                        "ret_to_today": r["ret_to_today"]})
              buckets = {
                  "三冠王": [],
                  "KD+量爆": [],
                  "KD+MACD": [],
                  "量爆+MACD": [],
                  "只 KD": [],
                  "只 量爆": [],
                  "只 MACD": [],
              }
              for k, (tags, info) in tag_map.items():
                  if tags == {"KD", "量爆", "MACD"}: buckets["三冠王"].append(info)
                  elif tags == {"KD", "量爆"}: buckets["KD+量爆"].append(info)
                  elif tags == {"KD", "MACD"}: buckets["KD+MACD"].append(info)
                  elif tags == {"量爆", "MACD"}: buckets["量爆+MACD"].append(info)
                  elif tags == {"KD"}: buckets["只 KD"].append(info)
                  elif tags == {"量爆"}: buckets["只 量爆"].append(info)
                  elif tags == {"MACD"}: buckets["只 MACD"].append(info)

          # 算各 bucket 完整績效（勝率 + 平均報酬 + 期望值 + 盈虧比）
          def bucket_perf(lst):
              valid = [r for r in lst if r.get("days_after", 0) >= 1]
              n = len(valid)
              if n == 0:
                  return {"n": 0, "wr": 0.0, "avg_ret": 0.0, "expected": 0.0,
                          "pl_ratio": 0.0, "total_ret": 0.0, "best": 0.0, "worst": 0.0}
              rets = [r.get("ret_to_today", 0) for r in valid]
              wins = [r for r in rets if r > 0]
              losses = [r for r in rets if r <= 0]
              avg_ret = sum(rets) / n
              wr = len(wins) / n * 100
              avg_win = sum(wins) / len(wins) if wins else 0
              avg_loss = sum(losses) / len(losses) if losses else 0
              pl_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
              expected = (wr / 100) * avg_win + (1 - wr / 100) * avg_loss
              return {"n": n, "wr": round(wr, 1), "avg_ret": round(avg_ret, 2),
                      "expected": round(expected, 2), "pl_ratio": round(pl_ratio, 2),
                      "total_ret": round(sum(rets), 1),
                      "best": round(max(rets), 2), "worst": round(min(rets), 2)}

          def bucket_wr(lst):
              p = bucket_perf(lst)
              return p["wr"], p["n"]

          # 5/12 簡化：刪掉「7 個互斥區塊」標題，只剩黃金組合一個 bucket
          st.markdown("---")
          st.markdown("### 🌟 黃金組合詳情")
          bucket_labels = {
              # 新版加權 confluence labels (MACD+量爆 黃金組合)
              "🌟 黃金組合 (MACD + 量爆 5 日內)": "🌟 黃金組合",
              "🔵 只 MACD": "🔵 只 MACD",
              "🟠 只 量爆": "🟠 只 量爆",
              "📌 KD 低位 (僅參考，業界平均 46-50%)": "📌 KD 參考",
              # 中間版 labels
              "三冠王 (5日內 3 類)": "🌟 三冠王 (5日內 KD+量爆+MACD)",
              "中 2 類 (5日內任 2 類)": "💎 中 2 類 (5日內 2 類觸發)",
              "只 1 類": "▫️ 只 1 類",
              "三冠王":     "🌟 三冠王 (同日 KD + 量爆 + MACD)",
              "KD+量爆":   "💎 同日 KD + 量爆",
              "KD+MACD":   "💎 同日 KD + MACD",
              "量爆+MACD": "💎 同日 量爆 + MACD",
              "只 KD":     "🟢 只 KD",
              "只 量爆":   "🟠 只 量爆",
              "只 MACD":   "🔵 只 MACD",
          }
          # ─── Confluence buckets 完整績效表 ───
          # 5/12 簡化：只顯示黃金組合績效，不顯示「只 MACD / 只 量爆 / KD 參考」
          active_keys = [k for k in buckets.keys() if "黃金組合" in k or "三冠王" in k]
          bucket_perf_rows = []
          for key in active_keys:
              label = bucket_labels.get(key, key)
              p = bucket_perf(buckets[key])
              bucket_perf_rows.append({
                  "區塊": label,
                  "檔數": len(buckets[key]),
                  "樣本": p["n"],
                  "勝率": f"{p['wr']:.1f}%",
                  "平均報酬": f"{p['avg_ret']:+.2f}%",
                  "💎 期望值": f"{p['expected']:+.2f}%",
                  "盈虧比": f"{p['pl_ratio']:.2f}",
                  "總報酬": f"{p['total_ret']:+.1f}%",
                  "最佳/最差": f"{p['best']:+.1f}% / {p['worst']:+.1f}%",
              })
          if bucket_perf_rows:
              st.dataframe(pd.DataFrame(bucket_perf_rows), use_container_width=True, hide_index=True)
          st.caption("💎 **期望值 > 0 = 長期能賺錢**（最重要）｜盈虧比 > 1.5 = 賺得比虧得多")

          # ─── 各區塊清單（從強到弱）───
          def show_bucket(name, label, infos):
              st.markdown("---")
              p = bucket_perf(infos)
              st.markdown(
                  f"### {label}  ｜  **{len(infos)} 個觸發** ｜ "
                  f"💎 期望值 **{p['expected']:+.2f}%** ｜ "
                  f"平均 {p['avg_ret']:+.2f}% ｜ "
                  f"勝率 {p['wr']:.1f}% ｜ "
                  f"盈虧比 {p['pl_ratio']:.2f} ｜ "
                  f"樣本 {p['n']}"
              )
              if not infos:
                  st.info("過去 22 日無觸發")
                  return
              rows = []
              for r in infos:
                  row = {
                      "股號": r["ticker"].split(".")[0],
                      "公司名": r["name"],
                      "目前價": r["current_price"],
                      "觸發日": r["trigger_date"],
                      "觸發收盤": r["trigger_close"],
                  }
                  # 加 KD 欄
                  if "kd_K" in r:
                      row["K"] = r["kd_K"]
                      row["D"] = r["kd_D"]
                  # 加量爆欄
                  if "vol_today" in r:
                      row["前→昨→今(張)"] = f"{r['vol_pre']}→{r['vol_yest']}→{r['vol_today']}"
                      row["今/昨"] = f"{r['ratio_1']}x"
                      row["昨/前"] = f"{r['ratio_2']}x"
                  # 加 MACD 欄
                  if "macd_type" in r:
                      row["MACD類型"] = r["macd_type"]
                      row["DIF"] = r["DIF"]
                      row["MACD"] = r["MACD"]
                      row["OSC"] = r["OSC"]
                  row["距今"] = r["days_after"]
                  row["剩餘天數"] = r.get("days_to_expire", "?")
                  row["浮動報酬%"] = f"{r['ret_to_today']:+.2f}%"
                  row["贏輸"] = "🟢 贏" if r["ret_to_today"] > 0 else ("🔴 輸" if r["days_after"] >= 1 else "⏳ 當日")
                  rows.append(row)
              df_bucket = pd.DataFrame(rows).sort_values("觸發日", ascending=False).reset_index(drop=True)
              st.dataframe(df_bucket, use_container_width=True, hide_index=True, height=min(400, max(150, len(rows) * 38 + 50)))

          # 從強到弱顯示（5/12 半夜改：只顯示黃金組合）
          for key in active_keys:
              if "黃金組合" in key or "三冠王" in key:
                  show_bucket(key, bucket_labels.get(key, key), buckets[key])

          # ─── 🔵 單獨 MACD 區塊（5/12 補回，用戶要求）───
          # 業界共識: MACD 零軸下方 signal line 黃金交叉 + 200MA 過濾 = 75-85% 勝率頂級
          # 我們版本 22 日內勝率 78.3% / 期望值 +14.97% / 盈虧比 5.36 (業界頂級)
          # 排除「已在黃金組合」的 ticker (避免重複看到同檔)
          st.markdown("---")
          st.markdown("### 🔵 單獨 MACD 區塊（業界頂級單一指標）")
          _macd_perf = stats.get("macd", {}).get("perf", {})
          st.caption(
              f"💎 **業界共識 MACD 最強 setup**：零軸下方 OSC 由負轉正 + DIF 上升 + close > MA50 "
              f"（業界 75-85% 勝率上限） ｜ "
              f"📊 22 日實測：勝率 {_macd_perf.get('wr', '?')}% / "
              f"期望值 {_macd_perf.get('expected', '?')}% / "
              f"盈虧比 {_macd_perf.get('pl_ratio', '?')} "
              f"／ {_macd_perf.get('n', 0)} 樣本 ｜ "
              f"⚠️ **已排除黃金組合**（避免重複），這裡只列「MACD 觸發但量沒爆」的股票"
          )
          macd_list_all = results_data.get("macd", [])
          # 排除黃金組合的 ticker
          golden_tks_set = {r["ticker"] for r in cb_data.get("golden", [])}
          macd_only_list = sorted(
              [r for r in macd_list_all if r["ticker"] not in golden_tks_set],
              key=lambda x: x.get("days_after", 99)
          )
          if not macd_only_list:
              st.info("過去 22 日內無單獨 MACD 觸發（不在黃金組合中的）")
          else:
              macd_df = pd.DataFrame([{
                  "新鮮度": ("🟢 今天" if r["days_after"] == 0
                            else "🟡 昨天" if r["days_after"] == 1
                            else f"📅 {r['days_after']} 天前"),
                  "股號": r["ticker"].split(".")[0],
                  "公司名": r["name"],
                  "目前價": r["current_price"],
                  "觸發日": r["trigger_date"],
                  "觸發收盤": r["trigger_close"],
                  "DIF": r.get("DIF", "?"),
                  "MACD": r.get("MACD", "?"),
                  "OSC": r.get("OSC", "?"),
                  "MA50": r.get("MA50", "?"),
                  "當天漲幅": f"{r.get('daily_return', 0):+.2f}%",
                  "乖離MA20": f"{r.get('bias_MA20', 0):+.1f}%",
                  "浮動報酬%": f"{r['ret_to_today']:+.2f}%",
                  "贏輸": "🟢 贏" if r["ret_to_today"] > 0 else ("🔴 輸" if r["days_after"] >= 1 else "⏳ 當日"),
              } for r in macd_only_list])
              st.dataframe(macd_df, use_container_width=True, hide_index=True,
                          height=min(450, len(macd_only_list) * 38 + 50))


# ══════════════════════════════════════════════════════════════
# TAB 5: 投信突襲（5/15 新增，只短波段 Web 顯示）
# ══════════════════════════════════════════════════════════════
with tab5:
  if _tab5_active:
      st.subheader("🏦 投信突襲訊號")
      st.caption("過去 22 個交易日，過去 5 天投信完全沒動 → 突然淨買 ≥ 50 張的純個股清單")

      st.info(
          "🕕 **每天 18:00 自動更新**（5/20 改）。"
          "原 16:35 排程因 TWSE T86 投信欄位未完整釋出多次寫入假值（5/19 凌巨/華夏、5/20 台積電/聯電），"
          "現拆到 18:00 獨立跑（含 19:00 / 20:00 兩道 retry）。"
      )
      st.warning("⚠️ **這是研究工具不是策略**。實證顯示投信買 vs 賣勝率差 < 1%（5/15 backtest 投信籌碼無方向性 alpha）。請自行配合其他指標判斷。")

      trust_data = read_gist_file("trust_screener_results.json")
      if not trust_data:
          st.info("還沒有資料 — 等今天 18:00 排程跑完會自動更新（或 William 在 Windows 手動跑 `python screener_trust.py`）")
      else:
          updated = trust_data.get("updated", "?")
          today = trust_data.get("today", "?")
          params = trust_data.get("params", {})
          n_signals = trust_data.get("n_signals", 0)
          signals = trust_data.get("signals", [])

          c1, c2, c3, c4 = st.columns(4)
          c1.metric("訊號日總數", f"{n_signals} 個")
          c2.metric("資料末日", today)
          c3.metric("更新時間", updated[:16] if updated != "?" else "?")
          c4.metric("成交量門檻", f"{params.get('min_volume_lots', '?')} 張")

          st.markdown(
              f"**訊號定義**：過去 {params.get('lookback_trust_days', 5)} 個交易日投信淨買賣超累計 = 0 "
              f"→ 今日投信淨買 ≥ {params.get('min_net_lots_today', 50)} 張 "
              f"AND 訊號日成交量 ≥ {params.get('min_volume_lots', 1000)} 張 "
              f"AND 純個股 (4 位數字 1101-9999)"
          )

          if not signals:
              st.info(f"過去 {params.get('display_window_days', 22)} 個交易日內無訊號")
          else:
              import pandas as pd

              # 概覽簡表
              st.markdown("### 📋 訊號股清單（點下方展開看該股投信逐日表）")
              df = pd.DataFrame([{
                  "新鮮度": ("🆕" if s["days_held"] <= 3 else "📅"),
                  "股號": s["ticker"],
                  "公司": s.get("name", s["ticker"]),
                  "訊號日": s["sig_date"][:4] + "-" + s["sig_date"][4:6] + "-" + s["sig_date"][6:8],
                  "已過天數": f"{s['days_held']} 天",
                  "投信買超": f"{s['today_net_lots']} 張",
                  "當天成交量": f"{s['vol_lots_at_sig']:,} 張",
                  "當天漲幅": f"{s.get('sig_day_return_pct', 0):+.2f}%",
                  "D 收": f"{s['sig_close']}",
                  "目前價": f"{s['current_price']}",
                  "浮動報酬%": f"{s['float_ret_pct']:+.2f}%",
                  "贏輸": (
                      "🆕 今日訊號" if s.get("days_since_buy", 99) < 0
                      else "⏳ 待觀察" if s.get("days_since_buy", s["days_held"]) < 1
                      else ("🟢 漲" if s["float_ret_pct"] > 0
                            else ("⚪ 持平" if s["float_ret_pct"] == 0 else "🔴 跌"))
                  ),
              } for s in signals])
              st.dataframe(df, use_container_width=True, hide_index=True,
                          height=min(600, len(signals) * 38 + 60))

              # 統計概覽
              st.markdown("---")
              # 用 days_since_buy >= 1 判斷「已持有」（買入當天浮動 0 沒意義）
              # 向後相容：舊資料沒 days_since_buy 用 days_held - 1
              def _is_finished(s):
                  return s.get("days_since_buy", max(0, s["days_held"] - 1)) >= 1
              n_win = sum(1 for s in signals if s["float_ret_pct"] > 0 and _is_finished(s))
              n_total_finished = sum(1 for s in signals if _is_finished(s))
              if n_total_finished > 0:
                  wr = n_win / n_total_finished * 100
                  avg_ret = sum(s["float_ret_pct"] for s in signals if _is_finished(s)) / n_total_finished
                  st.caption(
                      f"📊 統計（已進場 {n_total_finished} 筆）："
                      f"勝率 {wr:.1f}% | 平均浮動報酬 {avg_ret:+.2f}% "
                      f"(僅供參考，N 太小不具統計顯著性)"
                  )

              # 詳細：每個訊號股展開過去 30 天投信買賣超
              st.markdown("---")
              st.markdown("### 🔍 個股投信逐日表（過去 30 天）")
              for s in signals:
                  hist = s.get("trust_history", [])
                  if not hist:
                      continue
                  cum_lots = sum(h["trust_lots"] for h in hist)
                  badge = "🟢" if s["float_ret_pct"] > 0 else ("🔴" if s["days_held"] >= 1 else "⏳")
                  with st.expander(
                      f"{badge}  **{s['ticker']} {s.get('name','')}**  "
                      f"訊號 {s['sig_date'][:4]}-{s['sig_date'][4:6]}-{s['sig_date'][6:8]}  "
                      f"／投信 30 天累計 {cum_lots:+,} 張  "
                      f"／浮動 {s['float_ret_pct']:+.2f}%"
                  ):
                      sig_date_fmt = f"{s['sig_date'][:4]}-{s['sig_date'][4:6]}-{s['sig_date'][6:8]}"
                      hist_df = pd.DataFrame([{
                          "日期": h["date"] + ("  ⭐" if h["date"] == sig_date_fmt else ""),
                          "投信買賣超(張)": f"{h['trust_lots']:+,}" if h["trust_lots"] != 0 else "0",
                      } for h in hist])
                      st.dataframe(hist_df, use_container_width=False, hide_index=True,
                                  height=min(500, len(hist) * 35 + 50))
                      st.caption(f"⭐ = 訊號日／累計 {cum_lots:+,} 張／訊號前 5 天累計 = 0（過去 5 天投信完全沒動）")


# ══════════════════════════════════════════════════════════════
# TAB 6: 題材熱度榜（5/16 新增，只短波段 Web 顯示）
# ══════════════════════════════════════════════════════════════
with tab6:
  if _tab6_active:
      st.subheader("🔥 題材熱度榜")
      st.caption("熱度 = sqrt(成交額倍率 × 漲幅中位數)。爆量 + 漲價 = 真實題材輪動")

      st.info("🕕 **每天 18:00 自動更新**（跟投信突襲 Tab 一起跑，含 19:00 / 20:00 retry）")

      theme_data = read_gist_file("theme_screener_results.json")
      if not theme_data:
          st.info("還沒有資料 — 等今天 18:00 排程跑完會自動更新（或 Windows 手動跑 `python screener_themes.py`）")
      else:
          updated = theme_data.get("updated", "?")
          today = theme_data.get("today", "?")
          today_ranking = theme_data.get("today_ranking", [])
          daily_top5 = theme_data.get("daily_top5", {})
          window = theme_data.get("window_days", 22)
          baseline = theme_data.get("baseline_days", 10)

          c1, c2, c3, c4 = st.columns(4)
          c1.metric("題材總數", f"{theme_data.get('theme_count', 0)}")
          c2.metric("資料末日", today)
          c3.metric("更新", updated[:16] if updated != "?" else "?")
          c4.metric("基期", f"{baseline} 日均")

          st.caption(f"📐 {theme_data.get('metric_desc', '')}")

          # 顯示控制 toggle
          col_t1, col_t2 = st.columns([1, 3])
          with col_t1:
              _only_up = st.checkbox("🟢 只看漲幅 > 0", value=True,
                                     help="預設只列「爆量+漲」的強勢題材，避免大戶出貨假訊號")

          def _filter(rows):
              return [r for r in rows if (r.get("return_pct_median", 0) > 0)] if _only_up else rows

          # 今日 Top 10
          st.markdown(f"### 🏆 今日 ({today}) Top 10 熱題材")
          if today_ranking:
              import pandas as pd
              filtered_today = _filter(today_ranking)
              top10 = filtered_today[:10]
              if not top10:
                  st.warning("今日無「爆量+漲」題材（關掉 toggle 看全部）")
              else:
                  df_top = pd.DataFrame([{
                      "排名": f"#{i+1}",
                      "題材": r["theme"],
                      "熱度": f"{r['heat']:.2f}",
                      "成交額倍率": f"{r['amount_ratio']:.2f}x",
                      "漲幅中位": f"{r['return_pct_median']:+.2f}%",
                      "成交額(億)": f"{r['amount_ntd'] / 1e8:,.1f}",
                      "成分股": r.get('n_stocks_traded', 0),
                  } for i, r in enumerate(top10)])
                  st.dataframe(df_top, use_container_width=True, hide_index=True, height=420)

          # 累計次數榜（你最初要的）
          st.markdown("### 🥇 累計「當日第 1 名」次數榜（只算漲幅>0 那天才算強勢）")
          col_a, col_b = st.columns(2)

          cum_5d = theme_data.get("cumulative_top1_5d", [])
          cum_22d = theme_data.get("cumulative_top1_22d", [])

          with col_a:
              st.markdown("**過去 5 天**")
              if cum_5d:
                  import pandas as pd
                  df5 = pd.DataFrame([{
                      "排名": f"#{i+1}",
                      "題材": r["theme"],
                      "次數": f"{r['count']}/5",
                      "最後當第 1": r.get("last_top1_date") or "—",
                  } for i, r in enumerate(cum_5d)])
                  st.dataframe(df5, use_container_width=True, hide_index=True,
                              height=min(400, len(cum_5d) * 38 + 50))
              else:
                  st.caption("（5 天內無強勢題材）")

          with col_b:
              st.markdown("**過去 22 天**")
              if cum_22d:
                  import pandas as pd
                  df22 = pd.DataFrame([{
                      "排名": f"#{i+1}",
                      "題材": r["theme"],
                      "次數": f"{r['count']}/22",
                      "最後當第 1": r.get("last_top1_date") or "—",
                  } for i, r in enumerate(cum_22d)])
                  st.dataframe(df22, use_container_width=True, hide_index=True,
                              height=min(400, len(cum_22d) * 38 + 50))
              else:
                  st.caption("（22 天內無強勢題材）")

          # 過去 5 天每天 Top 5
          st.markdown(f"### 📅 過去 5 天每日 Top 5（看輪動）")
          if daily_top5:
              import pandas as pd
              sorted_days = sorted(daily_top5.keys(), reverse=True)[:5]
              for d in sorted_days:
                  rows = _filter(daily_top5[d])
                  if not rows:
                      with st.expander(f"📆 **{d}** — （該日無爆量+漲題材）", expanded=False):
                          st.caption("關掉 toggle 看全部")
                      continue
                  with st.expander(f"📆 **{d}** — Top 1: {rows[0]['theme']} (熱度 {rows[0]['heat']:.2f}, 漲 {rows[0]['return_pct_median']:+.2f}%)", expanded=(d == today)):
                      df_d = pd.DataFrame([{
                          "排名": f"#{i+1}",
                          "題材": r["theme"],
                          "熱度": f"{r['heat']:.2f}",
                          "成交額倍率": f"{r['amount_ratio']:.2f}x",
                          "漲幅中位": f"{r['return_pct_median']:+.2f}%",
                      } for i, r in enumerate(rows)])
                      st.dataframe(df_d, use_container_width=True, hide_index=True, height=220)

          # 點題材展開成分股
          st.markdown("### 🔍 個別題材成分股（今日 Top 15 展開）")
          for r in _filter(today_ranking)[:15]:
              with st.expander(
                  f"**{r['theme']}**  熱度 {r['heat']:.2f}  "
                  f"／成交額倍率 {r['amount_ratio']:.2f}x  "
                  f"／漲幅中位 {r['return_pct_median']:+.2f}%  "
                  f"／成分股 {len(r['stocks'])} 檔"
              ):
                  st.caption("成分股清單（編輯題材請改 stock-evolution-engine/themes.json）：")
                  cols = st.columns(6)
                  for i, tk in enumerate(r["stocks"]):
                      cols[i % 6].markdown(f"• `{tk}`")
