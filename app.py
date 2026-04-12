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


def write_gist_file(filename, data):
    """Write a single file to the data Gist."""
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    payload = {
        "files": {
            filename: {
                "content": json.dumps(data, ensure_ascii=False, indent=2)
            }
        }
    }
    try:
        r = requests.patch(
            f"https://api.github.com/gists/{DATA_GIST_ID}",
            headers=headers,
            json=payload,
            timeout=15,
        )
        return r.status_code == 200
    except Exception:
        return False


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


# ── Stock Price (for personal portfolio) ─────────────────────
@st.cache_data(ttl=600)
def get_stock_price(ticker):
    """Fetch latest closing price from Yahoo Finance."""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        r = requests.get(
            url,
            params={"range": "5d", "interval": "1d"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        closes = r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        for c in reversed(closes):
            if c is not None:
                return round(c, 2)
    except Exception:
        pass
    return None


# ── Portfolio (per-user, stored in Gist) ─────────────────────
def get_user_holdings(username):
    portfolios = read_gist_file("portfolios.json")
    return portfolios.get(username, {}).get("holdings", [])


def save_user_portfolio(username, holdings):
    portfolios = read_gist_file("portfolios.json")
    portfolios[username] = {
        "holdings": holdings,
        "updated": datetime.now().isoformat(),
    }
    if write_gist_file("portfolios.json", portfolios):
        st.cache_data.clear()
        return True
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
tab1, tab2, tab3 = st.tabs(["📊 買入排行", "💼 持倉狀態", "⚙️ 我的追蹤"])

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
            st.markdown(f"#### 🟢 達標股票 ({len(buy_signals)} 檔)")

            rows = []
            for s in buy_signals:
                rows.append({
                    "排名": s.get("rank", ""),
                    "代碼": s.get("ticker", ""),
                    "名稱": s.get("name", ""),
                    "分數": int(s.get("score", 0)),
                    "收盤價": s.get("close", 0),
                    "量能比": f"{s.get('vol_ratio', 0):.1f}x",
                    "訊號": s.get("reasons", ""),
                })

            df = pd.DataFrame(rows)

            # Highlight #1
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "分數": st.column_config.ProgressColumn(
                        "分數",
                        min_value=0,
                        max_value=20,
                        format="%d",
                    ),
                },
            )

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
        st.info("首次使用？Mac 端 16:30 掃描後，這裡會自動顯示最新排行。")

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

# ══════════════════════════════════════════════════════════════
# TAB 3: PERSONAL PORTFOLIO
# ══════════════════════════════════════════════════════════════
with tab3:
    st.markdown("### ⚙️ 我的追蹤清單")
    st.caption("個人追蹤用，不影響系統交易訊號")

    holdings = get_user_holdings(username)

    # ── Add new stock ──
    with st.form("add_stock", clear_on_submit=True):
        st.markdown("**新增追蹤**")
        c1, c2 = st.columns(2)
        with c1:
            new_ticker = st.text_input(
                "股票代碼", placeholder="例：2330.TW 或 3264.TWO"
            )
            new_name = st.text_input("股票名稱", placeholder="例：台積電")
        with c2:
            new_price = st.number_input(
                "買入價格", min_value=0.01, step=0.01, format="%.2f"
            )
            new_date = st.date_input("買入日期", value=date.today())

        if st.form_submit_button("✅ 新增追蹤", use_container_width=True):
            if new_ticker and new_name and new_price > 0:
                holdings.append({
                    "ticker": new_ticker.strip(),
                    "name": new_name.strip(),
                    "buy_price": round(new_price, 2),
                    "buy_date": str(new_date),
                })
                if save_user_portfolio(username, holdings):
                    st.success(f"已新增 {new_name}")
                    st.rerun()
                else:
                    st.error("儲存失敗，請稍後再試")
            else:
                st.error("請填寫完整資訊")

    # ── Show tracked stocks ──
    if holdings:
        st.markdown("---")
        st.markdown(f"**追蹤中 ({len(holdings)} 檔)**")

        for i, h in enumerate(holdings):
            ticker = h.get("ticker", "")
            name = h.get("name", "")
            buy_price = h.get("buy_price", 0)
            buy_date_str = h.get("buy_date", "")

            # Days held
            try:
                days = (date.today() - date.fromisoformat(buy_date_str)).days
            except (ValueError, TypeError):
                days = 0

            # Current price
            cur_price = get_stock_price(ticker)
            if cur_price and buy_price > 0:
                pnl = (cur_price / buy_price - 1) * 100
                icon = "🟢" if pnl > 0 else "🔴" if pnl < 0 else "⚪"
                pnl_str = f"{pnl:+.2f}%"
            else:
                icon = "⚪"
                pnl_str = "N/A"
                cur_price = 0

            c1, c2, c3, c4, c5 = st.columns([3, 1.5, 1.5, 1.5, 1])
            c1.markdown(f"**{icon} {name}** ({ticker})")
            c2.write(f"成本 ${buy_price}")
            c3.write(f"現價 ${cur_price}" if cur_price else "現價 --")
            c4.write(f"報酬 {pnl_str}")
            with c5:
                if st.button("❌", key=f"del_{i}"):
                    holdings.pop(i)
                    save_user_portfolio(username, holdings)
                    st.rerun()

            st.caption(f"📅 {buy_date_str} | 持有 {days} 天")
            st.markdown("---")
    else:
        st.info("尚未追蹤任何股票。使用上方表單新增。")
