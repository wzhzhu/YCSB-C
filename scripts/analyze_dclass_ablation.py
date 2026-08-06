#!/usr/bin/env python3
"""Paired analysis for the D-class re-validation ablations.

Compares (on vs off) arms cell-by-cell using per-repeat medians, so a small
throughput effect is read against run-to-run noise.

  lazy:       lazy-on-0804  vs  lazy-off-0804        (wlC/D, low threads)
  creditband: creditband-on-0804 vs creditband-off-0804 (wlA, t32/t64)

Verdict heuristic: if the median on/off throughput delta is inside the
per-arm repeat spread (max-min over repeats), the optimization buys nothing
measurable on the clean harness.
"""
import csv, glob, os, statistics as st

ROOT = "results/rocksdb-matrix"


def load(run_id):
    rows = []
    for f in glob.glob(f"{ROOT}/{run_id}/**/summary.csv", recursive=True):
        rows += list(csv.DictReader(open(f)))
    return rows


def tput(r):
    return float(r["read_attempt_kops"]) + float(r["write_attempt_kops"])


def fg(r):
    try:
        return float(r["cache_fg_hit_ratio"])
    except Exception:
        return float("nan")


def index(rows):
    """(cache_bytes, threads, workload) -> list of rows (repeats)."""
    d = {}
    for r in rows:
        k = (int(r["cache_bytes"]), int(r["threads"]), r["workload"])
        d.setdefault(k, []).append(r)
    return d


def compare(name, on_id, off_id):
    on, off = index(load(on_id)), index(load(off_id))
    keys = sorted(set(on) & set(off))
    if not keys:
        print(f"\n## {name}: no overlapping cells yet "
              f"(on={sum(len(v) for v in on.values())} rows, "
              f"off={sum(len(v) for v in off.values())} rows)")
        return
    print(f"\n## {name}   (ON={on_id}  vs  OFF={off_id})")
    print("| wl | cache | thr | n | ON kops (spread) | OFF kops (spread) | "
          "d_tput% | ON fg | OFF fg | d_fg pt | verdict |")
    print("|----|------:|----:|--:|-----------------:|------------------:|"
          "-------:|------:|-------:|--------:|---------|")
    gm = []
    for cb, thr, wl in keys:
        o, f = on[(cb, thr, wl)], off[(cb, thr, wl)]
        ot = [tput(x) for x in o]
        ft = [tput(x) for x in f]
        on_med, off_med = st.median(ot), st.median(ft)
        on_spread = max(ot) - min(ot)
        off_spread = max(ft) - min(ft)
        d_tput = (on_med / off_med - 1) * 100 if off_med else 0.0
        on_fg, off_fg = st.median([fg(x) for x in o]), st.median([fg(x) for x in f])
        # noise band: the larger repeat spread as a fraction of off median
        noise_pct = (max(on_spread, off_spread) / off_med * 100) if off_med else 0.0
        verdict = "noise" if abs(d_tput) <= noise_pct else (
            "ON wins" if d_tput > 0 else "OFF wins")
        gm.append(on_med / off_med if off_med else 1.0)
        print(f"| {wl} | {cb//(1<<30)}G | {thr} | {len(o)} | "
              f"{on_med:.0f} (±{on_spread:.0f}) | {off_med:.0f} (±{off_spread:.0f}) | "
              f"{d_tput:+.1f} | {on_fg:.3f} | {off_fg:.3f} | "
              f"{(on_fg-off_fg)*100:+.2f} | {verdict} |")
    if gm:
        import math
        g = math.exp(sum(math.log(x) for x in gm) / len(gm))
        print(f"\n  geomean ON/OFF throughput = {g:.4f} ({(g-1)*100:+.2f}%) "
              f"over {len(gm)} cells")


if __name__ == "__main__":
    os.chdir("/users/wzhzhu/YCSB-C-master")
    compare("LAZY MODE", "lazy-on-0804", "lazy-off-0804")
    compare("REALIZATION CREDIT BAND (wlA 2G)", "creditband-on-0804",
            "creditband-off-0804")
    compare("REALIZATION CREDIT BAND (wider: wlA/F 4-8G t64)",
            "creditband2-on-0805", "creditband2-off-0805")
