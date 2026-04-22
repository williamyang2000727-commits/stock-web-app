"""V32: ATR-scaled exit rules.
Instead of fixed stop_loss=-20%, take_profit=40%, trailing=20%,
scale them per-stock based on ATR% at buy time.

High ATR stock (8%): wider stops = more room to breathe
Low ATR stock (2%): tighter stops = quick exit on weakness
"""
import pickle, numpy as np, os
from collections import defaultdict

print("=" * 70)
print("  V32: ATR-Scaled Exit Rules")
print("  Baseline: fixed -20%/+40%/20% trailing")
print("  Test: ATR-adaptive exits")
print("=" * 70)

pre_pkl = r"C:\stock-evolution\pre_indicators.pkl"
if not os.path.exists(pre_pkl):
    print(f"  ERROR: {pre_pkl} not found!"); exit()

pre = pickle.load(open(pre_pkl, "rb"))
ns, nd = pre["n_stocks"], pre["n_days"]
dates = pre["dates"]; tickers = pre["tickers"]
close = pre["close"]; opn = pre.get("open", close)
print(f"  Data: {ns}x{nd} ({dates[0].date()}~{dates[-1].date()})")

# Indicators for scoring (89.90 exact)
rsi = pre["rsi"]; bb_pos = pre["bb_pos"]; vol_ratio = pre["vol_ratio"]
macd_hist = pre["macd_hist"]; k_val = pre["k_val"]; near_high = pre["near_high"]
adx = pre["adx"]; bias = pre["bias"]; obv_rising = pre["obv_rising"]; atr_pct = pre["atr_pct"]
up_days = pre.get("up_days", np.zeros((ns, nd)))
week52_pos = pre.get("week52_pos", np.zeros((ns, nd)))
vol_up_days = pre.get("vol_up_days", np.zeros((ns, nd)))
mom_accel = pre.get("mom_accel", np.zeros((ns, nd)))
is_green = pre["is_green"]; gap = pre["gap"]; top100_mask = pre["top100_mask"]
new_high_60 = pre["new_high_60"]
mom3 = pre.get("mom_d", {}).get(3, np.zeros((ns, nd)))
ma3 = pre.get("ma_d", {}).get(3, np.zeros((ns, nd)))


def calc_score(si, day):
    cur = float(close[si, day])
    if cur <= 0 or np.isnan(cur) or top100_mask[si, day] < 0.5: return -1
    sc = 0
    if rsi[si, day] >= 70: sc += 3
    if bb_pos[si, day] >= 0.95: sc += 3
    if cur > ma3[si, day] > 0: sc += 2
    if day >= 1 and macd_hist[si, day] > 0 and macd_hist[si, day - 1] <= 0: sc += 3
    if k_val[si, day] >= 80: sc += 2
    if mom3[si, day] >= 8: sc += 3
    if abs(near_high[si, day]) <= 10: sc += 2
    if new_high_60[si, day] > 0.5: sc += 1
    if adx[si, day] >= 40: sc += 2
    if 0 <= bias[si, day] <= 5: sc += 1
    if obv_rising[si, day] > 0.5: sc += 2
    if atr_pct[si, day] >= 3: sc += 1
    if up_days[si, day] >= 5: sc += 2
    if week52_pos[si, day] >= 0.7: sc += 1
    if vol_up_days[si, day] >= 2: sc += 1
    if mom_accel[si, day] >= 0: sc += 2
    if is_green[si, day] > 0.5: sc += 1
    if gap[si, day] >= 1.0: sc += 1
    return sc if sc >= 8 else -1


def replay(exit_mode="fixed", sl_mult=3.0, tp_mult=6.0, tr_mult=2.5, be_mult=1.5, lk_mult=3.0):
    """
    exit_mode:
      "fixed" = 89.90 baseline (stop=-20, tp=40, trail=20, be=10, lock=20/3)
      "atr"   = ATR-scaled exits
    """
    hold_si = [-1, -1]; hold_bp = [0, 0]; hold_pk = [0, 0]; hold_bd = [0, 0]
    hold_atr = [0, 0]  # ATR% at buy time
    n_holding = 0
    trades = []

    for day in range(60, nd - 1):
        for h in range(2):
            if hold_si[h] < 0: continue
            si = hold_si[h]; cur = float(close[si, day]); dh = day - hold_bd[h]
            if dh < 1: continue
            if cur > hold_pk[h]: hold_pk[h] = cur
            ret = (cur / hold_bp[h] - 1) * 100
            pk = (hold_pk[h] / hold_bp[h] - 1) * 100

            if exit_mode == "fixed":
                # 89.90 exact exits
                eff = -20
                if pk >= 10: eff = 0
                sell = ret <= eff
                if not sell and ret >= 40: sell = True
                if not sell and hold_pk[h] > hold_bp[h] and (1 - cur / hold_pk[h]) * 100 >= 20: sell = True
                if not sell and pk >= 20 and ret < 3: sell = True
                if not sell and dh >= 30: sell = True
            else:
                # ATR-scaled exits
                a = max(hold_atr[h], 1.0)  # ATR% at buy, floor 1%
                eff_stop = max(-30, -sl_mult * a)        # e.g., -3*5% = -15%
                eff_tp = min(60, tp_mult * a)             # e.g., 6*5% = 30%
                eff_trail = max(8, tr_mult * a)           # e.g., 2.5*5% = 12.5%
                eff_be_trigger = max(5, be_mult * a)      # e.g., 1.5*5% = 7.5%
                eff_lock_trigger = max(10, lk_mult * a)   # e.g., 3*5% = 15%
                eff_lock_floor = max(1, 0.5 * a)          # e.g., 0.5*5% = 2.5%

                eff = eff_stop
                if pk >= eff_be_trigger: eff = 0
                sell = ret <= eff
                if not sell and ret >= eff_tp: sell = True
                if not sell and hold_pk[h] > hold_bp[h] and (1 - cur / hold_pk[h]) * 100 >= eff_trail: sell = True
                if not sell and pk >= eff_lock_trigger and ret < eff_lock_floor: sell = True
                if not sell and dh >= 30: sell = True

            if sell:
                sp = float(opn[si, day + 1]) if day + 1 < nd else cur
                if sp <= 0 or np.isnan(sp): sp = cur
                ar = (sp / hold_bp[h] - 1) * 100 - 0.585
                trades.append({"year": str(dates[hold_bd[h]].year), "return": ar, "win": ar > 0, "atr": hold_atr[h]})
                hold_si[h] = -1; n_holding -= 1

        if n_holding >= 2: continue
        held_set = set(hh for hh in hold_si if hh >= 0)
        best_sc = -1; best_si = -1; best_vr = 0
        for si in range(ns):
            if si in held_set: continue
            sc = calc_score(si, day)
            if sc < 0: continue
            vr = float(vol_ratio[si, day])
            if sc > best_sc or (sc == best_sc and vr > best_vr):
                best_sc = sc; best_si = si; best_vr = vr
        if best_si >= 0 and day + 1 < nd:
            bp = float(close[best_si, day + 1])
            if bp > 0 and not np.isnan(bp):
                for h in range(2):
                    if hold_si[h] < 0:
                        hold_si[h] = best_si; hold_bp[h] = bp; hold_pk[h] = bp
                        hold_bd[h] = day + 1; n_holding += 1
                        hold_atr[h] = float(atr_pct[best_si, day])  # ATR at signal day
                        break
    return trades


def report(name, trades):
    nt = len(trades)
    if nt == 0: print(f"  {name:<35}   NO TRADES"); return
    rets = [t["return"] for t in trades]
    total = sum(rets); wr = sum(t["win"] for t in trades) / nt * 100; avg = total / nt
    wins = [r for r in rets if r > 0]; losses = [r for r in rets if r <= 0]
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0
    by_yr = defaultdict(list)
    for t in trades: by_yr[t["year"]].append(t["return"])
    yr_str = " ".join(f"{y}:{sum(1 for r in rs if r > 0) / len(rs) * 100:.0f}%" for y, rs in sorted(by_yr.items()))
    print(f"  {name:<35} {nt:>4} {total:>7.0f} {wr:>5.1f}% {avg:>5.1f}% w={avg_win:>+5.1f} l={avg_loss:>+5.1f}  {yr_str}")


# Baseline
print(f"\n{'=' * 70}")
print(f"  RESULTS")
print(f"{'=' * 70}")
print(f"\n  {'Config':<35} {'N':>4} {'Total':>7} {'WR':>6} {'Avg':>6} {'AvgW':>7} {'AvgL':>7}  Yearly")
print(f"  {'-' * 35} {'-' * 4} {'-' * 7} {'-' * 6} {'-' * 6} {'-' * 7} {'-' * 7}  {'-' * 50}")

report("BASELINE (fixed -20/+40/20)", replay("fixed"))

# ATR-scaled with different multiplier combos
configs = [
    # (sl_mult, tp_mult, tr_mult, be_mult, lk_mult)
    (3.0, 6.0, 2.5, 1.5, 3.0),   # balanced
    (2.5, 5.0, 2.0, 1.5, 2.5),   # tighter
    (3.5, 7.0, 3.0, 2.0, 3.5),   # wider
    (4.0, 8.0, 3.0, 2.0, 4.0),   # very wide
    (2.0, 4.0, 1.5, 1.0, 2.0),   # very tight
    (3.0, 5.0, 2.0, 1.5, 3.0),   # sl=3 tp=5 (lower TP)
    (3.0, 8.0, 2.5, 1.5, 3.0),   # sl=3 tp=8 (higher TP)
    (2.5, 6.0, 2.0, 1.0, 2.5),   # mixed
    (3.0, 6.0, 2.5, 1.5, 2.0),   # lower lock
    (3.0, 6.0, 1.5, 1.5, 3.0),   # tighter trail
]

for sl, tp, tr, be, lk in configs:
    label = f"ATR sl={sl} tp={tp} tr={tr} be={be} lk={lk}"
    report(label, replay("atr", sl, tp, tr, be, lk))

# ATR distribution of bought stocks
print(f"\n{'=' * 70}")
print(f"  ATR DISTRIBUTION OF TRADED STOCKS")
print(f"{'=' * 70}")
baseline = replay("fixed")
atrs = [t["atr"] for t in baseline if "atr" in t]
if atrs:
    print(f"  Mean ATR%: {np.mean(atrs):.1f}%")
    print(f"  Median ATR%: {np.median(atrs):.1f}%")
    print(f"  Min: {min(atrs):.1f}%, Max: {max(atrs):.1f}%")
    for lo, hi in [(0, 2), (2, 4), (4, 6), (6, 8), (8, 100)]:
        sub = [t for t in baseline if lo <= t.get("atr", 0) < hi]
        if sub:
            wr = sum(t["win"] for t in sub) / len(sub) * 100
            avg = sum(t["return"] for t in sub) / len(sub)
            print(f"  ATR [{lo}-{hi}%): n={len(sub):>3} wr={wr:>5.1f}% avg={avg:>+5.1f}%")

print(f"\n{'=' * 70}")
