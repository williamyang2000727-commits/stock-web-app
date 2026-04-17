"""Single source of truth for Web-side sell logic.

Mirrors GPU kernel's 13 sell conditions 1:1. All 4 Python callers in the Web
repo (scanner.check_sell_signals / daily_scan / app.py 換股狀態 / app.py
backtest extension) MUST use this module instead of duplicating logic.

Rule of thumb: if GPU kernel adds a new sell condition, add it HERE once and
all 4 callers pick it up automatically.

Indicators dict expected keys (all optional; required only if the matching
use_X flag is on):
  rsi, macd_hist, macd_hist_prev, kd_dead_cross, vol_ratio,
  momentum_N (N = params["momentum_days"]), ma_fast, ma_slow
"""


def should_sell(bp, cur, peak, days_held, params, cache_closes=None, indicators=None):
    """Return (reason_str_or_None). None = no sell condition met."""
    if bp <= 0 or cur <= 0 or days_held < 1:
        return None

    ret = (cur / bp - 1) * 100
    peak_gain = (peak / bp - 1) * 100 if bp > 0 else 0

    # 1. Stop loss (with breakeven modifier — matches kernel effective_stop)
    eff_stop = params.get("stop_loss", -20)
    if params.get("use_breakeven", 0) and peak_gain >= params.get("breakeven_trigger", 20):
        eff_stop = 0
    if ret <= eff_stop:
        return f"保本 {ret:+.1f}%（曾漲 +{peak_gain:.1f}%）" if eff_stop == 0 else f"停損 {ret:+.1f}%"

    # 2. Take profit
    if params.get("use_take_profit", 1) and ret >= params.get("take_profit", 80):
        return f"停利 +{ret:.1f}%"

    # 3. Trailing stop
    trailing = params.get("trailing_stop", 0)
    if trailing > 0 and peak > bp * 1.01:
        dd = (cur / peak - 1) * 100
        if dd <= -trailing:
            return f"移動停利 {dd:.1f}%"

    # 4. RSI sell
    if params.get("use_rsi_sell", 0) and indicators:
        rsi = indicators.get("rsi", 0)
        if rsi >= params.get("rsi_sell", 90):
            return f"RSI 超買 {rsi:.0f}"

    # 5. MACD sell (hist turns negative)
    if params.get("use_macd_sell", 0) and indicators:
        if indicators.get("macd_hist", 0) < 0 and indicators.get("macd_hist_prev", 0) >= 0:
            return "MACD 死叉"

    # 6. KD sell (K crosses below D)
    if params.get("use_kd_sell", 0) and indicators:
        if indicators.get("kd_dead_cross", 0):
            return "KD 死叉"

    # 7. Volume shrink (requires >= 2 days held)
    vs = params.get("sell_vol_shrink", 0)
    if vs > 0 and days_held >= 2 and indicators:
        vr = indicators.get("vol_ratio", 99)
        if vr < vs:
            return f"量能萎縮 vol_ratio={vr:.1f}"

    # 8. Sell below MA (mode 1=fast, 2=slow, 3=MA60)
    sbm = int(params.get("sell_below_ma", 0))
    if sbm > 0:
        ma_val = None
        label = None
        if sbm == 3 and cache_closes and len(cache_closes) > 60:
            ma_val = sum(cache_closes[-61:-1]) / 60
            label = "MA60"
        elif sbm == 1 and indicators:
            mfw = int(params.get("ma_fast_w", 5))
            ma_val = indicators.get(f"ma{mfw}")
            label = f"MA{mfw}"
        elif sbm == 2 and indicators:
            msw = int(params.get("ma_slow_w", 20))
            ma_val = indicators.get(f"ma{msw}")
            label = f"MA{msw}"
        if ma_val and ma_val > 0 and bp >= ma_val and cur < ma_val:
            return f"跌破 {label}"

    # 9. Stagnation
    if params.get("use_stagnation_exit", 0):
        stag_d = int(params.get("stagnation_days", 10))
        stag_min = params.get("stagnation_min_ret", 5)
        if days_held >= stag_d and ret < stag_min:
            return f"停滯出場（{days_held}天僅 {ret:+.1f}%）"

    # 10. Time decay (gradual profit expectation)
    if params.get("use_time_decay", 0):
        hh = int(params.get("hold_days", 30)) // 2
        if days_held >= hh:
            min_req = (days_held - hh) * params.get("ret_per_day", 0.5)
            if ret < min_req:
                return f"漸進停利（{days_held}天 {ret:+.1f}% < +{min_req:.1f}%）"

    # 11. Profit lock
    if params.get("use_profit_lock", 0):
        if peak_gain >= params.get("lock_trigger", 30) and ret < params.get("lock_floor", 10):
            return f"鎖利（曾 +{peak_gain:.1f}% 跌回 {ret:+.1f}%）"

    # 12. Momentum reversal (requires >= 10 days held, matches kernel)
    if params.get("use_mom_exit", 0) and days_held >= 10 and indicators:
        mom_days = int(params.get("momentum_days", 5))
        mom_val = indicators.get(f"momentum_{mom_days}", 0)
        if mom_val < -params.get("mom_exit_th", 2):
            return f"動量反轉 {mom_val:+.1f}%"

    # 13. Max hold days
    if days_held >= int(params.get("hold_days", 30)):
        return f"到期 {days_held} 天 {ret:+.1f}%"

    return None
