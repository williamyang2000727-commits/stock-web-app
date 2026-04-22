"""V31: Institutional flow as TIEBREAKER (not filter).
When two stocks have the same score, pick the one with higher institutional buying.
Tests: vol_ratio tiebreaker (current) vs trust_cum5 vs foreign_cum5 vs total_cum5

Requires: inst_data_full.pkl + pre_indicators.pkl on Windows.
"""
import pickle, json, numpy as np, os
from collections import defaultdict

print("=" * 70)
print("  V31: Institutional Tiebreaker Test")
print("  Current: vol_ratio | Test: trust_cum5 / foreign_cum5 / total_cum5")
print("=" * 70)

# Load data
inst_pkl = r"C:\stock-evolution\inst_data_full.pkl"
pre_pkl = r"C:\stock-evolution\pre_indicators.pkl"

if not os.path.exists(inst_pkl):
    print(f"  ERROR: {inst_pkl} not found! Run fetch_institutional.py first.")
    exit()
if not os.path.exists(pre_pkl):
    print(f"  ERROR: {pre_pkl} not found!")
    exit()

inst_raw = pickle.load(open(inst_pkl, "rb"))
pre = pickle.load(open(pre_pkl, "rb"))
ns, nd = pre["n_stocks"], pre["n_days"]
dates = pre["dates"]
tickers = pre["tickers"]
close = pre["close"]
opn = pre.get("open", close)
print(f"  Price data: {ns}x{nd} ({dates[0].date()}~{dates[-1].date()})")
print(f"  Institutional data: {len(inst_raw)} days")

# Map institutional data
date_to_idx = {d.strftime("%Y%m%d"): i for i, d in enumerate(dates)}
tk_map = {tk: i for i, tk in enumerate(tickers)}

inst_total = np.zeros((ns, nd), dtype=np.float32)
inst_foreign = np.zeros((ns, nd), dtype=np.float32)
inst_trust = np.zeros((ns, nd), dtype=np.float32)

matched_days = 0
for dt_str, day_data in inst_raw.items():
    if dt_str not in date_to_idx:
        continue
    di = date_to_idx[dt_str]
    matched_days += 1
    for tk, info in day_data.items():
        if tk in tk_map:
            si = tk_map[tk]
            inst_total[si, di] = info.get("total", 0)
            inst_foreign[si, di] = info.get("foreign", 0)
            inst_trust[si, di] = info.get("trust", 0)

print(f"  Matched days: {matched_days}/{len(inst_raw)}")

# Compute rolling signals
print("  Computing rolling signals...")
trust_cum5 = np.zeros((ns, nd), dtype=np.float32)
foreign_cum5 = np.zeros((ns, nd), dtype=np.float32)
total_cum5 = np.zeros((ns, nd), dtype=np.float32)
trust_cum20 = np.zeros((ns, nd), dtype=np.float32)

for d in range(5, nd):
    trust_cum5[:, d] = np.sum(inst_trust[:, d - 4:d + 1], axis=1)
    foreign_cum5[:, d] = np.sum(inst_foreign[:, d - 4:d + 1], axis=1)
    total_cum5[:, d] = np.sum(inst_total[:, d - 4:d + 1], axis=1)
for d in range(20, nd):
    trust_cum20[:, d] = np.sum(inst_trust[:, d - 19:d + 1], axis=1)
print("  Done.")

# 89.90 indicators
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
    if cur <= 0 or np.isnan(cur) or top100_mask[si, day] < 0.5:
        return -1
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


def replay(tiebreaker_fn):
    """Replay 89.90 with a custom tiebreaker function.
    tiebreaker_fn(si, day) -> float (higher = preferred when scores are tied)
    """
    hold_si = [-1, -1]; hold_bp = [0, 0]; hold_pk = [0, 0]; hold_bd = [0, 0]
    n_holding = 0
    trades = []
    tie_count = 0
    tie_diff = 0  # times tiebreaker picked different stock than vol_ratio would

    for day in range(60, nd - 1):
        # Sell evaluation
        for h in range(2):
            if hold_si[h] < 0: continue
            si = hold_si[h]; cur = float(close[si, day]); dh = day - hold_bd[h]
            if dh < 1: continue
            if cur > hold_pk[h]: hold_pk[h] = cur
            ret = (cur / hold_bp[h] - 1) * 100
            pk = (hold_pk[h] / hold_bp[h] - 1) * 100
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
                trades.append({"year": str(dates[hold_bd[h]].year), "return": ar, "win": ar > 0,
                               "ticker": tickers[si], "buy_day": hold_bd[h]})
                hold_si[h] = -1; n_holding -= 1

        if n_holding >= 2: continue
        held_set = set(hh for hh in hold_si if hh >= 0)

        # Find all candidates with their scores
        candidates = []
        for si in range(ns):
            if si in held_set: continue
            sc = calc_score(si, day)
            if sc < 0: continue
            tb = tiebreaker_fn(si, day)
            vr = float(vol_ratio[si, day])
            candidates.append((sc, tb, vr, si))

        if not candidates: continue

        # Sort: best score first, then tiebreaker, then vol_ratio as final fallback
        candidates.sort(key=lambda x: (-x[0], -x[1], -x[2]))
        best_si = candidates[0][3]

        # Count ties (for analysis)
        best_sc = candidates[0][0]
        tied = [c for c in candidates if c[0] == best_sc]
        if len(tied) > 1:
            tie_count += 1
            # Check if vol_ratio would pick a different stock
            vr_best = max(tied, key=lambda x: x[2])
            if vr_best[3] != best_si:
                tie_diff += 1

        if day + 1 < nd:
            bp = float(close[best_si, day + 1])
            if bp > 0 and not np.isnan(bp):
                for h in range(2):
                    if hold_si[h] < 0:
                        hold_si[h] = best_si; hold_bp[h] = bp; hold_pk[h] = bp
                        hold_bd[h] = day + 1; n_holding += 1; break

    return trades, tie_count, tie_diff


# Define tiebreaker functions
tiebreakers = [
    ("vol_ratio (baseline)", lambda si, d: float(vol_ratio[si, d])),
    ("trust_cum5", lambda si, d: float(trust_cum5[si, max(d - 1, 0)])),
    ("foreign_cum5", lambda si, d: float(foreign_cum5[si, max(d - 1, 0)])),
    ("total_cum5", lambda si, d: float(total_cum5[si, max(d - 1, 0)])),
    ("trust_cum20", lambda si, d: float(trust_cum20[si, max(d - 1, 0)])),
    ("trust+foreign", lambda si, d: float(trust_cum5[si, max(d - 1, 0)] + foreign_cum5[si, max(d - 1, 0)] * 0.3)),
    ("trust_cum5 (D-2)", lambda si, d: float(trust_cum5[si, max(d - 2, 0)])),  # use D-2 to avoid lookahead
]

print(f"\n{'=' * 70}")
print(f"  TIEBREAKER COMPARISON (89.90 scoring, only tiebreaker differs)")
print(f"{'=' * 70}")
print(f"\n  {'Tiebreaker':<25} {'N':>4} {'Total':>7} {'WR':>6} {'Avg':>6} {'Ties':>5} {'Diff':>5}  Yearly WR")
print(f"  {'-' * 25} {'-' * 4} {'-' * 7} {'-' * 6} {'-' * 6} {'-' * 5} {'-' * 5}  {'-' * 50}")

baseline_wr = 0
for name, tb_fn in tiebreakers:
    trades, ties, diffs = replay(tb_fn)
    nt = len(trades)
    if nt == 0:
        print(f"  {name:<25}   NO TRADES"); continue
    rets = [t["return"] for t in trades]
    total = sum(rets)
    wr = sum(t["win"] for t in trades) / nt * 100
    avg = total / nt
    if name.startswith("vol_ratio"):
        baseline_wr = wr

    by_yr = defaultdict(list)
    for t in trades:
        by_yr[t["year"]].append(t["return"])
    yr_str = " ".join(f"{y}:{sum(1 for r in rs if r > 0) / len(rs) * 100:.0f}%"
                      for y, rs in sorted(by_yr.items()))

    diff_flag = ""
    if wr > baseline_wr + 0.5: diff_flag = " *** WIN"
    elif wr < baseline_wr - 0.5: diff_flag = " --- LOSE"

    print(f"  {name:<25} {nt:>4} {total:>7.0f} {wr:>5.1f}% {avg:>5.1f}% {ties:>5} {diffs:>5}  {yr_str}{diff_flag}")

# Detailed: show which stocks the tiebreakers disagree on
print(f"\n{'=' * 70}")
print(f"  DETAILED: Cases where trust_cum5 picks DIFFERENT stock than vol_ratio")
print(f"{'=' * 70}")

# Replay both and compare selections
vr_trades, _, _ = replay(lambda si, d: float(vol_ratio[si, d]))
tr_trades, _, _ = replay(lambda si, d: float(trust_cum5[si, max(d - 1, 0)]))

vr_dict = {(t["buy_day"], t["ticker"]): t["return"] for t in vr_trades}
tr_dict = {(t["buy_day"], t["ticker"]): t["return"] for t in tr_trades}

# Find trades unique to trust tiebreaker (not in vol_ratio)
only_trust = [(k, tr_dict[k]) for k in tr_dict if k not in vr_dict]
only_vr = [(k, vr_dict[k]) for k in vr_dict if k not in tr_dict]

print(f"\n  Trades only in trust_cum5 (not vol_ratio): {len(only_trust)}")
if only_trust:
    trust_rets = [r for _, r in only_trust]
    print(f"    Avg return: {sum(trust_rets) / len(trust_rets):+.1f}%")
    print(f"    Win rate: {sum(1 for r in trust_rets if r > 0) / len(trust_rets) * 100:.1f}%")

print(f"\n  Trades only in vol_ratio (not trust_cum5): {len(only_vr)}")
if only_vr:
    vr_rets = [r for _, r in only_vr]
    print(f"    Avg return: {sum(vr_rets) / len(vr_rets):+.1f}%")
    print(f"    Win rate: {sum(1 for r in vr_rets if r > 0) / len(vr_rets) * 100:.1f}%")

print(f"\n  Common trades: {len(set(vr_dict.keys()) & set(tr_dict.keys()))}")
print(f"\n{'=' * 70}")
