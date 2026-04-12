"""
龍蝦選股系統 Web App
Taiwan stock selection system - Streamlit dashboard
"""

import streamlit as st
import requests
import json
import pandas as pd
from datetime import datetime, date
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
    """Read a specific file from the data Gist."""
    data = _read_gist(DATA_GIST_ID)
    return data.get(filename, {})


# ── Authentication ───────────────────────────────────────────
def authenticate():
    """SHA256-based login. Users configured in .streamlit/secrets.toml."""
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

    if st.button("🔄 重新整理", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    if st.button("🚪 登出", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

    st.markdown("---")
    st.caption("🦞 龍蝦選股系統 v1.0")
    st.caption("策略引擎：GPU RTX 3060")

# ── Load scan data ───────────────────────────────────────────
scan = read_gist_file("scan_results.json")

# ── Tabs ─────────────────────────────────────────────────────
tab1, tab2 = st.tabs(["📊 買入排行", "💼 持倉狀態"])

# ══════════════════════════════════════════════════════════════
# TAB 1: BUY RANKINGS
# ══════════════════════════════════════════════════════════════
with tab1:
    if scan and scan.get("date"):
        st.markdown(f"### 📊 買入排行 — {scan['date']}")

        # Strategy info bar
        c1, c2, c3 = st.columns(3)
        c1.metric("策略", f"v{scan.get('strategy_version', '?')}")
        c2.metric("分數", f"{scan.get('strategy_score', '?')}")
        ts = scan.get("timestamp", "")
        c3.metric("掃描時間", ts.split("T")[-1][:5] if "T" in ts else ts)

        st.markdown("---")

        # Buy signals table
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
                f"🏆 今日第一名：**{top.get('name', '')}** "
                f"({top.get('ticker', '')}) — {int(top.get('score', 0))} 分"
            )
        else:
            st.info("今日無買入訊號 — 沒有股票達到門檻分數")

        # Market summary
        mkt = scan.get("market_summary", {})
        if mkt:
            st.markdown("---")
            st.caption(
                f"📈 上市 {mkt.get('twse_count', 0)} 檔 | "
                f"上櫃 {mkt.get('otc_count', 0)} 檔 | "
                f"掃描範圍：成交量前 {mkt.get('scan_count', 100)}"
            )
    else:
        st.warning("⚠️ 尚無掃描資料。掃描會在每個交易日 16:30 自動執行。")

# ══════════════════════════════════════════════════════════════
# TAB 2: HOLDINGS STATUS (Mac's tracked positions)
# ══════════════════════════════════════════════════════════════
with tab2:
    st.markdown("### 💼 持倉狀態")

    # Sell signals (prominent)
    sell_signals = scan.get("sell_signals", []) if scan else []
    if sell_signals:
        for sig in sell_signals:
            st.error(
                f"⚠️ **賣出訊號** — {sig.get('name', '')} ({sig.get('ticker', '')})\n\n"
                f"報酬 {sig.get('return', 0):+.1f}% | {sig.get('reason', '')}"
            )
        st.markdown("---")

    # Holdings
    holdings_status = scan.get("holdings_status", []) if scan else []
    if holdings_status:
        st.markdown(f"#### 持倉 ({len(holdings_status)} / 2 檔)")

        for h in holdings_status:
            ret = h.get("return_pct", 0)
            icon = "🟢" if ret > 0 else "🔴" if ret < 0 else "⚪"

            c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
            c1.markdown(f"#### {icon} {h.get('name', '')} ({h.get('ticker', '')})")
            c2.metric("買入價", f"${h.get('buy_price', 0):.2f}")
            c3.metric("現價", f"${h.get('current_price', 0):.2f}")
            c4.metric("報酬", f"{ret:+.1f}%")

            st.caption(
                f"📅 {h.get('buy_date', '')} | "
                f"持有 {h.get('days_held', 0)} 天 | "
                f"停損 {h.get('stop_loss', -20)}% | "
                f"停利 +{h.get('take_profit', 80)}% | "
                f"最長 {h.get('hold_days', 30)} 天"
            )
            st.markdown("---")
    else:
        if scan and scan.get("date"):
            st.info("目前無持倉")
        else:
            st.info("等待掃描資料...")
