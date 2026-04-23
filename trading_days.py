"""台股交易日計算的唯一真相（Single source of truth）。

❌ 禁止在其他地方自己寫 `(end-start).days * 5/7` 或類似近似
✅ 所有需要算「持有天數」「距今天幾個交易日」的地方，**只能 import 這裡的函式**

修復記錄：
- 2026-04-17：daily_scan 用 *5/7 估，結果中探針 4 天 算成 2，創威 16 天算成 17。
  統一到這個模組後，不會再有人寫出近似公式。

caching 邏輯：
- Module-level dict 快取 TWSE 抓回的交易日列表
- 同一個 process 只抓一次
- 要強制重抓 → clear_cache()
"""

import logging
from datetime import date

_cal_cache = {"dates": None}


def _get_calendar():
    """Lazy-load TWSE 交易日曆。同 process 只抓一次。"""
    if _cal_cache["dates"] is None:
        try:
            from scanner import fetch_trading_calendar
            cal = fetch_trading_calendar(months=12)  # 1 年範圍夠用
            _cal_cache["dates"] = sorted(str(d) for d in cal) if cal else []
        except Exception as e:
            logging.warning(f"[trading_days] TWSE 交易日曆抓取失敗：{e}")
            _cal_cache["dates"] = []
    return _cal_cache["dates"]


def count_between(start_date_str, end_date_str, fallback_calendar=None):
    """算 (start, end] 區間的交易日數。

    Args:
      start_date_str: 開始日（EXCLUSIVE，例如 buy_date）
      end_date_str: 結束日（INCLUSIVE，例如 today / sell_date）
      fallback_calendar: 若 TWSE 失敗時用這份備援（list of "YYYY-MM-DD"）

    Returns:
      int — 交易日數；壞輸入回 0

    Rules:
      1. 優先用 TWSE 真實日曆（含台股假日）
      2. TWSE 失敗 → 用 fallback_calendar（通常是 cache dates）
      3. 都沒有 → *5/7 近似 + 警告 log（最後手段，不推薦）
    """
    if not start_date_str or not end_date_str:
        return 0
    if start_date_str >= end_date_str:
        return 0

    # 驗證日期格式（若格式錯直接回 0，不偷偷算）
    try:
        date.fromisoformat(start_date_str)
        date.fromisoformat(end_date_str)
    except (ValueError, TypeError):
        logging.warning(f"[trading_days] 日期格式錯：start={start_date_str} end={end_date_str}")
        return 0

    dates = _get_calendar()
    # 合併 TWSE 日曆 + fallback（兩者取聯集，避免任一來源不完整）
    if fallback_calendar:
        fb = set(str(d) for d in fallback_calendar)
        if dates:
            fb.update(dates)
        dates = sorted(fb)

    if dates:
        return sum(1 for d in dates if start_date_str < d <= end_date_str)

    # 最後手段
    try:
        sd = date.fromisoformat(start_date_str)
        ed = date.fromisoformat(end_date_str)
        approx = max(0, int((ed - sd).days * 5 / 7))
        logging.warning(
            f"[trading_days] 退化到 *5/7 近似：{start_date_str} → {end_date_str} ≈ {approx}。"
            f"TWSE 和 fallback 都沒有，結果可能誤差 ±2。"
        )
        return approx
    except:
        return 0


def clear_cache():
    """強制下次呼叫時重抓 TWSE 日曆。"""
    _cal_cache["dates"] = None


def get_calendar_info():
    """除錯用：回傳目前快取的日曆資訊。"""
    dates = _get_calendar()
    return {
        "count": len(dates),
        "first": dates[0] if dates else None,
        "last": dates[-1] if dates else None,
        "sample_recent": dates[-5:] if dates else [],
    }
