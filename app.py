"""
龍蝦選股系統 Web App
Taiwan stock selection system - Streamlit dashboard
"""

import streamlit as st
import requests
import json
import pandas as pd
from datetime import datetime, date, timedelta
import hashlib

# ── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="龍蝦選股系統",
    page_icon="🦞",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Secrets ──────────────────────────────────────────────────
GITHUB_TOKEN = st.secrets["github_token"]
DATA_GIST_ID = st.secrets["data_gist_id"]
HISTORY_GIST_ID = st.secrets.get("history_gist_id", DATA_GIST_ID)


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


def write_gist_file(filename, data_dict):
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    payload = {"files": {filename: {"content": json.dumps(data_dict, ensure_ascii=False, indent=2)}}}
    try:
        r = requests.patch(f"https://api.github.com/gists/{DATA_GIST_ID}",
                           headers=headers, json=payload, timeout=30)
        if r.status_code == 200:
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
        st.markdown("# 🦞 龍蝦選股系統")
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
def next_trading_day(scan_date_str):
    try:
        d = date.fromisoformat(scan_date_str)
        nd = d + timedelta(days=1)
        while nd.weekday() >= 5:
            nd += timedelta(days=1)
        return nd
    except (ValueError, TypeError):
        return date.today()


def save_user_holdings(username, holdings):
    portfolios = read_gist_file("portfolios.json")
    if not isinstance(portfolios, dict):
        portfolios = {}
    portfolios[username] = {"holdings": holdings, "updated": datetime.now().isoformat()}
    return write_gist_file("portfolios.json", portfolios)


# ══════════════════════════════════════════════════════════════
# MAIN APP
# ══════════════════════════════════════════════════════════════
if not authenticate():
    st.stop()

username = st.session_state.username

# ── Sidebar ──
with st.sidebar:
    st.markdown(f"### 👤 {username}")
    st.markdown(f"📅 {date.today().strftime('%Y/%m/%d')}")
    st.markdown("---")
    if st.button("🔄 重新整理", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    if st.button("🚪 登出", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()
    st.markdown("---")
    st.caption("🦞 龍蝦選股系統 v1.0")

# ── Load Data ──
strategy_params = read_gist_file("strategy_params.json")
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

if history_cache and cache_date and market_data and trading_date > cache_date:
    stocks = history_cache.get("stocks", {})
    for ticker, hist in stocks.items():
        if ticker in market_data:
            info = market_data[ticker]
            hist["c"] = hist["c"][-59:] + [info["close"]]
            hist["h"] = hist["h"][-59:] + [info.get("high", info["close"])]
            hist["l"] = hist["l"][-59:] + [info.get("low", info["close"])]
            hist["v"] = hist["v"][-59:] + [info["vol"]]
    history_cache["updated"] = trading_date
    try:
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        payload = {"files": {"history_cache.json": {"content": json.dumps(history_cache, ensure_ascii=False)}}}
        requests.patch(f"https://api.github.com/gists/{HISTORY_GIST_ID}", headers=headers, json=payload, timeout=60)
    except Exception:
        pass

# ── Live Scan (每次都跑，確保最新) ──
scan = None
try:
    from scanner import run_scan
    scan = run_scan(dict(strategy_params), set(held_tickers), history_cache)
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

# Sell signals: live tracking using strategy sell conditions
user_sell_signals = []
if user_holdings and strategy_params and market_data:
    try:
        from scanner import check_sell_signals
        user_sell_signals = check_sell_signals(user_holdings, strategy_params, market_data, history_cache)
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
        nd = next_trading_day(scan_date)
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

            # Days held
            try:
                days = (date.today() - date.fromisoformat(buy_date_str)).days
            except (ValueError, TypeError):
                days = 0

            # Current price: TWSE/TPEx → Gist scan → Yahoo
            cur_price = None
            if market_data and ticker in market_data:
                cur_price = market_data[ticker]["close"]
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
                        sell_date = st.date_input("賣出日期", value=date.today(), key=f"sell_date_{i}")

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
                    new_date = st.date_input("買入日期", value=date.today())

                if st.form_submit_button("確認買入", use_container_width=True):
                    if new_ticker and new_name and new_price > 0:
                        tk = new_ticker.strip()
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
