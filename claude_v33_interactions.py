"""V33: Nonlinear indicator interaction bonuses.
Test adding bonus score for specific indicator COMBINATIONS.
Currently scoring is purely additive (linear). This tests if
certain pairs/triples are more predictive together.
"""
import pickle, numpy as np, os
from collections import defaultdict

print("=" * 70)
print("  V33: Indicator Interaction Bonuses")
print("=" * 70)

pre_pkl = r"C:\stock-evolution\pre_indicators.pkl"
if not os.path.exists(pre_pkl):
    print(f"  ERROR: {pre_pkl} not found!"); exit()

pre = pickle.load(open(pre_pkl, "rb"))
ns, nd = pre["n_stocks"], pre["n_days"]
dates = pre["dates"]; tickers = pre["tickers"]
close = pre["close"]; opn = pre.get("open", close)

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

print(f"  Data: {ns}x{nd} ({dates[0].date()}~{dates[-1].date()})")


def calc_score_with_bonus(si, day, bonuses=None):
    """89.90 base score + optional interaction bonuses."""
    cur = float(close[si, day])
    if cur <= 0 or np.isnan(cur) or top100_mask[si, day] < 0.5: return -1
    sc = 0
    # Base 89.90 scoring (20 indicators)
    r = rsi[si, day] >= 70
    b = bb_pos[si, day] >= 0.95
    m = cur > ma3[si, day] > 0
    mc = day >= 1 and macd_hist[si, day] > 0 and macd_hist[si, day - 1] <= 0
    k = k_val[si, day] >= 80
    mo = mom3[si, day] >= 8
    nh = abs(near_high[si, day]) <= 10
    h60 = new_high_60[si, day] > 0.5
    ax = adx[si, day] >= 40
    bi = 0 <= bias[si, day] <= 5
    ob = obv_rising[si, day] > 0.5
    at = atr_pct[si, day] >= 3
    ud = up_days[si, day] >= 5
    w52 = week52_pos[si, day] >= 0.7
    vu = vol_up_days[si, day] >= 2
    ma_ok = mom_accel[si, day] >= 0
    gr = is_green[si, day] > 0.5
    gp = gap[si, day] >= 1.0
    vr = vol_ratio[si, day] >= 3

    if r: sc += 3
    if b: sc += 3
    if m: sc += 2
    if mc: sc += 3
    if k: sc += 2
    if mo: sc += 3
    if nh: sc += 2
    if h60: sc += 1
    if ax: sc += 2
    if bi: sc += 1
    if ob: sc += 2
    if at: sc += 1
    if ud: sc += 2
    if w52: sc += 1
    if vu: sc += 1
    if ma_ok: sc += 2
    if gr: sc += 1
    if gp: sc += 1
    if vr: sc += 1

    # Interaction bonuses
    if bonuses:
        for combo_name, combo_cond, bonus_val in bonuses:
            if combo_cond(r, b, m, mc, k, mo, nh, h60, ax, bi, ob, at, ud, w52, vu, ma_ok, gr, gp, vr):
                sc += bonus_val

    return sc if sc >= 8 else -1


def replay(bonuses=None, buy_th=8):
    hold_si = [-1, -1]; hold_bp = [0, 0]; hold_pk = [0, 0]; hold_bd = [0, 0]
    n_holding = 0; trades = []
    for day in range(60, nd - 1):
        for h in range(2):
            if hold_si[h] < 0: continue
            si = hold_si[h]; cur = float(close[si, day]); dh = day - hold_bd[h]
            if dh < 1: continue
            if cur > hold_pk[h]: hold_pk[h] = cur
            ret = (cur / hold_bp[h] - 1) * 100; pk = (hold_pk[h] / hold_bp[h] - 1) * 100
            sell = False; eff = -20
            if pk >= 10: eff = 0
            if ret <= eff: sell = True
            if not sell and ret >= 40: sell = True
            if not sell and hold_pk[h] > hold_bp[h] and (1 - cur / hold_pk[h]) * 100 >= 20: sell = True
            if not sell and pk >= 20 and ret < 3: sell = True
            if not sell and dh >= 30: sell = True
            if sell:
                sp = float(opn[si, day + 1]) if day + 1 < nd else cur
                if sp <= 0 or np.isnan(sp): sp = cur
                ar = (sp / hold_bp[h] - 1) * 100 - 0.585
                trades.append({"year": str(dates[hold_bd[h]].year), "return": ar, "win": ar > 0})
                hold_si[h] = -1; n_holding -= 1
        if n_holding >= 2: continue
        held_set = set(hh for hh in hold_si if hh >= 0)
        best_sc = -1; best_si = -1; best_vr = 0
        for si in range(ns):
            if si in held_set: continue
            sc = calc_score_with_bonus(si, day, bonuses)
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
                        hold_bd[h] = day + 1; n_holding += 1; break
    return trades


def report(name, trades, baseline_wr=0):
    nt = len(trades)
    if nt == 0: print(f"  {name:<50}   NO TRADES"); return 0
    rets = [t["return"] for t in trades]
    total = sum(rets); wr = sum(t["win"] for t in trades) / nt * 100; avg = total / nt
    by_yr = defaultdict(list)
    for t in trades: by_yr[t["year"]].append(t["return"])
    yr_str = " ".join(f"{y}:{sum(1 for r in rs if r > 0) / len(rs) * 100:.0f}%" for y, rs in sorted(by_yr.items()))
    flag = ""
    if baseline_wr > 0:
        if wr > baseline_wr + 0.5: flag = " *** WIN"
        elif wr < baseline_wr - 0.5: flag = " --- LOSE"
    print(f"  {name:<50} {nt:>4} {total:>7.0f} {wr:>5.1f}% {avg:>5.1f}%  {yr_str}{flag}")
    return wr


# Define interaction combos to test
# Each: (name, condition_fn(r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr), bonus)
# r=RSI, b=BB, m=MA, mc=MACD_cross, k=KD, mo=mom, nh=near_high, h60=new_high_60
# ax=ADX, bi=bias, ob=OBV, at=ATR, ud=up_days, w52=week52, vu=vol_up, ma_ok=mom_accel
# gr=green, gp=gap, vr=vol_ratio

combos_to_test = [
    # Triple confirmations
    ("RSI+MACD+KD triple confirm +3",
     [("+RSI+MACD+KD", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: r and mc and k, 3)]),

    ("RSI+BB+VOL breakout +3",
     [("+RSI+BB+VOL", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: r and b and vr, 3)]),

    ("NearHigh+ADX+Mom trend accel +2",
     [("+NH+ADX+MOM", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: nh and ax and mo, 2)]),

    ("BB+VOL+OBV volume confirm +2",
     [("+BB+VOL+OBV", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: b and vr and ob, 2)]),

    ("MACD+ADX+Mom strong trend +3",
     [("+MACD+ADX+MOM", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: mc and ax and mo, 3)]),

    ("New60High+ADX+UpDays breakout +2",
     [("+H60+ADX+UD", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: h60 and ax and ud, 2)]),

    ("RSI+BB+MACD+KD quad confirm +5",
     [("+QUAD", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: r and b and mc and k, 5)]),

    # Double confirmations
    ("MACD+KD double cross +2",
     [("+MACD+KD", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: mc and k, 2)]),

    ("ADX+ATR volatility trend +2",
     [("+ADX+ATR", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: ax and at, 2)]),

    ("UpDays+GreenK momentum +2",
     [("+UD+GREEN", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: ud and gr, 2)]),

    ("Week52+NearHigh position +2",
     [("+W52+NH", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: w52 and nh, 2)]),

    # Multi-combo (best of above combined)
    ("TOP3 combos together",
     [
         ("+RSI+MACD+KD", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: r and mc and k, 3),
         ("+NH+ADX+MOM", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: nh and ax and mo, 2),
         ("+BB+VOL+OBV", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: b and vr and ob, 2),
     ]),

    # ATR-based (from V32 insight: ATR 4%+ stocks are better)
    ("ATR>=4 bonus +2",
     [("+ATR4", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: at, 2)]),

    ("ATR>=4 bonus +3",
     [("+ATR4", lambda r,b,m,mc,k,mo,nh,h60,ax,bi,ob,at,ud,w52,vu,ma_ok,gr,gp,vr: at, 3)]),
]

print(f"\n{'=' * 70}")
print(f"  INTERACTION BONUS RESULTS")
print(f"{'=' * 70}")
print(f"\n  {'Config':<50} {'N':>4} {'Total':>7} {'WR':>6} {'Avg':>6}  Yearly")
print(f"  {'-' * 50} {'-' * 4} {'-' * 7} {'-' * 6} {'-' * 6}  {'-' * 50}")

baseline_wr = report("BASELINE (no bonus)", replay())
print()

for name, bonuses in combos_to_test:
    report(name, replay(bonuses), baseline_wr)

# Also test: what if we RAISE buy_threshold instead of adding bonuses?
print(f"\n{'=' * 70}")
print(f"  BUY THRESHOLD COMPARISON")
print(f"{'=' * 70}")
print(f"\n  {'Config':<50} {'N':>4} {'Total':>7} {'WR':>6} {'Avg':>6}  Yearly")
print(f"  {'-' * 50} {'-' * 4} {'-' * 7} {'-' * 6} {'-' * 6}  {'-' * 50}")

for th in [8, 9, 10, 11, 12, 13, 14, 15]:
    report(f"buy_threshold={th}", replay(buy_th=th), baseline_wr)

print(f"\n{'=' * 70}")
