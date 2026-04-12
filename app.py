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


# ── Gist I/O ────────────────────────────────────────────────
@st.cache_data(ttl=300)
def _read_gist(gist_id):
    """Read all files from a Gist (cached 5 min)."""
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        r = requests.get(
            f"https://api.github.com/gists/{gist_id}",
            headers=headers,
            timeout=15,
        )
        if r.status_code == 200:
            result = {}
            for fname, fdata in r.json().get("files", {}).items():
                try:
                    result[fname] = json.loads(fdata["content"])
                except (json.JSONDecodeError, KeyError):
                    result[fname] = fdata.get("content", "")
            return result
    except Exception:
        pass
    return {}


def read_gist_file(filename):
    data = _read_gist(DATA_GIST_ID)
    return data.get(filename, {})


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
            submitted = st.form_submit_button("登入", use_container_width=True)

            if submitted and username and password:
                users = dict(st.secrets.get("users", {}))
                if username in users:
                    pw_hash = hashlib.sha256(password.encode()).hexdigest()
                    if pw_hash == users[username]:
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        st.rerun()
                    else:
                        st.error("密碼錯誤")
                else:
                    st.error("帳號不存在")
    return False


# ── Live Scan ────────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner="正在掃描市場資料...")
def do_live_scan(strategy_params, held_tickers_tuple):
    """Run live scan (cached 30 min)."""
    from scanner import run_scan
    return run_scan(strategy_params, set(held_tickers_tuple))


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


# ══════════════════════════════════════════════════════════════
# MAIN APP
# ══════════════════════════════════════════════════════════════

if not authenticate():
    st.stop()

username = st.session_state.username

# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"### 👤 {username}")
    st.markdown(f"📅 {date.today().strftime('%Y/%m/%d')}")
    st.markdown("---")

    if st.button("🔄 重新掃描", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    if st.button("🚪 登出", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

    st.markdown("---")
    st.caption("🦞 龍蝦選股系統 v1.0")
    st.caption("策略引擎：GPU RTX 3060")

# ── Load data ────────────────────────────────────────────────
# Strategy params from Gist
strategy_params = read_gist_file("strategy_params.json")

# User holdings from Gist
portfolios = read_gist_file("portfolios.json")
user_holdings = portfolios.get(username, {}).get("holdings", [])
held_tickers = tuple(h.get("ticker", "") for h in user_holdings)

# Sell signals from Mac scan (Gist)
mac_scan = read_gist_file("scan_results.json")
sell_signals_from_mac = mac_scan.get("sell_signals", []) if mac_scan else []

# Live scan (runs on page load, cached 30 min)
scan = None
if strategy_params:
    scan = do_live_scan(dict(strategy_params), held_tickers)

# Fallback to Mac scan if live scan fails
if not scan:
    scan = mac_scan

scan_date = scan.get("date", "") if scan else ""

# ── Signal computation ───────────────────────────────────────
max_positions = 2

user_buy_signals = []
if len(user_holdings) < max_positions and scan:
    user_buy_signals = scan.get("buy_signals", [])[:1]

user_sell_signals = []
user_tickers_set = {h.get("ticker") for h in user_holdings}
for sig in sell_signals_from_mac:
    if sig.get("ticker") in user_tickers_set:
        user_sell_signals.append(sig)

signal_count = len(user_buy_signals) + len(user_sell_signals)
signal_label = f"🔴 訊號 ({signal_count})" if signal_count > 0 else "訊號"

# ── Tabs ─────────────────────────────────────────────────────
tab0, tab1, tab2 = st.tabs([signal_label, "📊 買入排行", "💼 持倉狀態"])

# ══════════════════════════════════════════════════════════════
# TAB 0: SIGNALS
# ══════════════════════════════════════════════════════════════
with tab0:
    if signal_count > 0:
        nd = next_trading_day(scan_date)
        nd_str = nd.strftime("%m/%d")
        weekdays = ["一", "二", "三", "四", "五", "六", "日"]

        for sig in user_buy_signals:
            st.markdown(
                f"### 🎯 買入訊號\n\n"
                f"**請於 {nd_str}（{weekdays[nd.weekday()]}）"
                f"13:25 前買入**\n\n"
                f"---\n\n"
                f"### {sig.get('name', '')}（{sig.get('ticker', '')}）\n\n"
                f"收盤價 **{sig.get('close', 0)}** 元 ｜ 評分 **{int(sig.get('score', 0))}** 分\n\n"
                f"📌 收盤前下單，跟 GPU 回測一致"
            )

        for sig in user_sell_signals:
            st.markdown(
                f"### 📤 賣出訊號\n\n"
                f"**請於 {nd_str}（{weekdays[nd.weekday()]}）"
                f"9:00 開盤賣出**\n\n"
                f"---\n\n"
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
            st.markdown(f"#### 🟢 達標股票（前 3 名）")

            rows = []
            for s in top3:
                rows.append({
                    "排名": s.get("rank", ""),
                    "代碼": s.get("ticker", ""),
                    "名稱": s.get("name", ""),
                    "分數": int(s.get("score", 0)),
                    "收盤價": s.get("close", 0),
                })

            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True)

            top = buy_signals[0]
            st.success(
                f"🏆 第一名：**{top.get('name', '')}** "
                f"({top.get('ticker', '')}) — {int(top.get('score', 0))} 分"
            )
        else:
            st.info("今日無買入訊號 — 沒有股票達到門檻分數")

        mkt = scan.get("market_summary", {})
        if mkt:
            st.markdown("---")
            st.caption(
                f"📈 上市 {mkt.get('twse_count', 0)} 檔 | "
                f"上櫃 {mkt.get('otc_count', 0)} 檔 | "
                f"掃描範圍：成交量前 {mkt.get('scan_count', 100)}"
            )
    else:
        st.warning("⚠️ 掃描失敗或尚無資料")

# ══════════════════════════════════════════════════════════════
# TAB 2: HOLDINGS STATUS
# ══════════════════════════════════════════════════════════════
with tab2:
    st.markdown("### 💼 持倉狀態")

    # Sell signals
    if user_sell_signals:
        for sig in user_sell_signals:
            st.error(
                f"⚠️ **賣出訊號** — {sig.get('name', '')} ({sig.get('ticker', '')})\n\n"
                f"報酬 {sig.get('return', 0):+.1f}% | {sig.get('reason', '')}"
            )
        st.markdown("---")

    if user_holdings:
        st.markdown(f"#### 持倉（{len(user_holdings)} 檔）")

        for h in user_holdings:
            ticker = h.get("ticker", "")
            name = h.get("name", "")
            buy_price = h.get("buy_price", 0)
            buy_date_str = h.get("buy_date", "")

            try:
                days = (date.today() - date.fromisoformat(buy_date_str)).days
            except (ValueError, TypeError):
                days = 0

            # Price from Mac scan or Yahoo
            cur_price = None
            for sh in (mac_scan.get("holdings_status", []) if mac_scan else []):
                if sh.get("ticker") == ticker and sh.get("current_price", 0) > 0:
                    cur_price = sh["current_price"]
                    break

            if not cur_price:
                try:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                    r = requests.get(url, params={"range": "5d", "interval": "1d"},
                                     headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
                    closes = r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                    for c in reversed(closes):
                        if c is not None:
                            cur_price = round(c, 2)
                            break
                except Exception:
                    pass

            ret = (cur_price / buy_price - 1) * 100 if cur_price and buy_price > 0 else 0
            icon = "🟢" if ret > 0 else "🔴" if ret < 0 else "⚪"

            c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
            c1.markdown(f"#### {icon} {name} ({ticker})")
            c2.metric("買入價", f"${buy_price:.2f}")
            c3.metric("現價", f"${cur_price:.2f}" if cur_price else "—")
            c4.metric("報酬", f"{ret:+.1f}%")

            st.caption(f"📅 {buy_date_str} | 持有 {days} 天")
            st.markdown("---")
    else:
        st.info("目前無持倉")
