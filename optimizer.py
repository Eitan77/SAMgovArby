"""Grid-search optimizer: find best parameter combination from backtest results.

Two modes:
  1. Re-simulate from cached awards (fast — no API calls after first run)
  2. Run full backtest for each param combo (slow — use sparingly)

Usage:
    python optimizer.py --start 2023-01-01 --end 2023-12-31 [--quiet] [--verbose]
    python optimizer.py --from-cache backtest_results.csv [--quiet]   # re-score existing results
"""
import argparse
import csv
import itertools
import logging
import os
import sys

from config_logging import setup_logging, add_verbosity_flags
from price_sim import simulate_ratchet, simulate_ratchet_from_row, simulate_eod_from_row, simulate_asymmetric_from_row
from scoring_engine import score_contract
from filter_engine_bt import apply_filters_bt_from_training

log = logging.getLogger("optimizer")


def normalize_date(date_str: str) -> str:
    """Convert M/D/YYYY to YYYY-MM-DD for consistent date comparison."""
    if not date_str:
        return ""
    date_str = date_str.strip()
    if len(date_str) >= 10 and date_str[4] == '-':
        return date_str[:10]  # Already YYYY-MM-DD
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            m, d, y = parts
            return f"{y}-{int(m):02d}-{int(d):02d}"
    except (ValueError, IndexError):
        pass
    return date_str[:10]

OPT_RESULTS_FILE = os.path.join(os.path.dirname(__file__), "optimizer_results.csv")

# Parameter grid - optimizes: score threshold, EOD take profit, stop loss, hold days
# Market cap is swept separately inside Phase 2 at MCAP_STEP resolution (not in cartesian product)
PARAM_GRID = {
    "score_threshold": list(range(1, 51)),               # 1–50 in 1-point steps
    "tp_pct":          [i / 100 for i in range(2, 16)], # 2%–15% take profit (EOD close)
    "sl_pct":          [i / 100 for i in range(1, 9)],  # 1%–8% stop loss (EOD close)
    "max_hold_days":   [1, 2, 3, 4, 5],                 # days before time exit
}
MCAP_STEP = 10_000_000          # 10M resolution for market cap sweep
MCAP_MAX  = 5_000_000_000       # upper bound (matches global MAX_MARKET_CAP)


def optimize_from_cache(cache_file: str):
    """Re-run scoring/simulation on cached backtest results with different params.

    This avoids re-fetching from SAM.gov and yfinance for each combo.
    """
    log.info(f"Loading cached results from {cache_file}")
    rows = _load_csv(cache_file)

    # Only use rows that had a ticker resolved (i.e. made it past filter + ticker step)
    eligible = [r for r in rows if r.get("ticker") and r.get("award_date")]
    log.info(f"Eligible rows for re-simulation: {len(eligible)}")

    if not eligible:
        log.error("No eligible rows found. Run backtest.py first.")
        return

    # Legacy ratchet grid — from-cache mode still uses trailing stop + yfinance
    _legacy_grid = list(itertools.product(
        PARAM_GRID["score_threshold"],
        [i / 100 for i in range(1, 11)],  # gap_pct 1%–10%
        PARAM_GRID["max_hold_days"],
    ))
    log.info(f"Testing {len(_legacy_grid)} parameter combinations")

    best_score = -999
    best_combo = None
    opt_rows = []

    for threshold, gap, hold in _legacy_grid:
        trades = []
        for row in eligible:
            score = row.get("score")
            try:
                score = float(score) if score else 0
            except (ValueError, TypeError):
                continue
            if score < threshold:
                continue

            ticker = row["ticker"]
            award_date = row["award_date"][:10]
            sim = simulate_ratchet(ticker, award_date, gap, hold)
            if sim:
                trades.append({"pnl": sim["pnl_pct"], "peak": sim.get("peak_pnl_pct", 0)})

        # Use tp_pct=gap, sl_pct=gap for legacy ratchet display compatibility
        stats = _stats(trades, gap, gap, threshold, hold, max_market_cap=None)
        opt_rows.append(stats)

        combo_score = _rank_score(stats)
        if combo_score > best_score:
            best_score = combo_score
            best_combo = stats

        log.debug(f"  threshold={threshold} gap={gap*100:.0f}% hold={hold}d "
                  f"-> {stats['trades']} trades "
                  f"wr={stats['win_rate']}% exp={stats['expectancy']:.3f}")

    # Write optimizer results
    _write_opt_results(opt_rows)
    _print_top10(opt_rows, best_combo)
    return best_combo


def optimize_from_training_csv(csv_path: str, start_date: str = None, end_date: str = None):
    """Fully offline optimizer using the OHLC-enriched training CSV.

    No API calls. Requires enrich_ohlc.py to have been run first so that
    open_tN/high_tN/low_tN/close_tN columns are present.

    For each parameter combo:
      - Re-applies historical filters (market cap, 8-K, dilutive)
      - Re-scores with actual press release signal
      - Simulates TP/SL/hold from stored OHLC data
    """
    log.info(f"Loading training CSV: {csv_path}")
    rows = _load_csv(csv_path)
    log.info(f"Loaded {len(rows)} rows")

    # Optional date filter
    if start_date or end_date:
        before = len(rows)
        rows = [r for r in rows
                if (not start_date or normalize_date(r.get("posted_date", "")) >= start_date)
                and (not end_date   or normalize_date(r.get("posted_date", "")) <= end_date)]
        log.info(f"Date filter {start_date} -> {end_date}: {before} -> {len(rows)} rows")

    # Filter for rows with OHLC data (tickers that were enriched)
    rows = [r for r in rows if r.get("open_t0", "").strip() and r.get("ticker", "").strip()]
    log.info(f"Rows with OHLC data: {len(rows)}")

    if not rows:
        log.error("No rows with OHLC data. Run: enrich_ohlc.py datasets/training_set_final.csv")
        return None

    # Pre-compute filter results once per row (filters are combo-independent)
    row_filter_cache = []
    for row in rows:
        passed, _, extra = apply_filters_bt_from_training(row)
        row_filter_cache.append((passed, extra))

    # Pre-compute raw score once per row (threshold comparison happens per combo)
    row_score_cache = []
    for row_idx, row in enumerate(rows):
        passed, extra = row_filter_cache[row_idx]
        if not passed:
            row_score_cache.append(None)
            continue
        market_cap = extra.get("market_cap", 0)
        sole_source_raw = row.get("sole_source", "")
        sole_source = sole_source_raw.strip().lower() in ("true", "1", "yes")
        contract = {
            "awardee_name": row.get("awardee_name", ""),
            "award_amount": float(row.get("award_amount", 0) or 0),
            "sole_source": sole_source,
            "agency": row.get("agency", ""),
            "naics": row.get("naics", ""),
        }
        has_pr = extra.get("has_press_release", False)
        prior_wins = extra.get("agency_prior_win_count", 0)
        is_first_agency = (prior_wins == 0)
        raw_score, _ = score_contract(contract, market_cap,
                                      threshold=0, has_press_release=has_pr,
                                      is_first_agency_win=is_first_agency)
        row_score_cache.append((raw_score, extra, row))

    # Compute date range for trades_per_week / avg_pnl_per_week
    all_dates = [normalize_date(r.get("posted_date", "")) for r in rows]
    valid_dates = [d for d in all_dates if d]
    if len(valid_dates) >= 2:
        from datetime import datetime as _dt
        date_range_days = (_dt.strptime(max(valid_dates), "%Y-%m-%d")
                           - _dt.strptime(min(valid_dates), "%Y-%m-%d")).days
        date_range_days = max(date_range_days, 1)
    else:
        date_range_days = None

    # ── Phase 1: pre-compute simulation results for every unique (tp, sl, hold) ──
    # simulate_eod_from_row depends only on (row OHLC, tp_pct, sl_pct, hold) — NOT on
    # score_threshold. Pre-computing eliminates ~50x redundant simulation work.
    valid_tp_sl_hold = [
        (tp, sl, hold)
        for tp   in PARAM_GRID["tp_pct"]
        for sl   in PARAM_GRID["sl_pct"]
        for hold in PARAM_GRID["max_hold_days"]
    ]
    n_tsh = len(valid_tp_sl_hold)
    log.info(f"Pre-computing simulations: {n_tsh} (tp,sl,hold) sets × {len(row_score_cache)} rows")

    # sim_cache[(tp, sl, hold)] = list of (pnl, peak) or None, indexed by row position
    sim_cache: dict = {}
    for i, (tp, sl, hold) in enumerate(valid_tp_sl_hold):
        results = []
        for cached in row_score_cache:
            if cached is None:
                results.append(None)
                continue
            _, _, row = cached
            sim = simulate_asymmetric_from_row(row, tp, sl, hold)
            if sim:
                results.append((sim["pnl_pct"], sim.get("peak_pnl_pct", 0)))
            else:
                results.append(None)
        sim_cache[(tp, sl, hold)] = results
        if (i + 1) % 50 == 0:
            log.info(f"  [{i+1}/{n_tsh}] (tp,sl,hold) sets pre-computed")

    log.info("Simulation pre-computation complete. Running combo sweep...")

    # ── Phase 2: sweep all (threshold, tp, sl, hold) combos ──
    # For each combo, market cap is swept by sorting eligible trades by mcap and
    # walking through them cumulatively at MCAP_STEP resolution.  This is O(rows log rows)
    # per combo instead of O(rows × mcap_steps) — same result, ~500x faster.
    base_combos = list(itertools.product(
        PARAM_GRID["score_threshold"],
        PARAM_GRID["tp_pct"],
        PARAM_GRID["sl_pct"],
        PARAM_GRID["max_hold_days"],
    ))
    n_mcap_steps = MCAP_MAX // MCAP_STEP
    log.info(f"Testing {len(base_combos):,} combos × up to {n_mcap_steps} mcap steps "
             f"(sorted-walk, not cartesian)")

    best_score = -999
    best_combo = None
    opt_rows = []

    for combo_idx, (threshold, tp, sl, hold) in enumerate(base_combos):
        sim_results = sim_cache[(tp, sl, hold)]

        # Collect eligible trades with their market caps (one pass)
        eligible = []   # list of (mcap, pnl, peak)
        seen_trades: set = set()
        for row_idx, cached in enumerate(row_score_cache):
            if cached is None:
                continue
            raw_score, extra, row = cached
            if raw_score < threshold:
                continue
            key = (row.get("ticker", ""), row.get("posted_date", "")[:10])
            if key in seen_trades:
                continue
            seen_trades.add(key)
            sim_result = sim_results[row_idx]
            if sim_result:
                eligible.append((extra.get("market_cap", 0), sim_result[0], sim_result[1]))

        if not eligible:
            stats = _stats([], tp, sl, threshold, hold, MCAP_MAX, date_range_days)
            opt_rows.append(stats)
            continue

        # Sort by market cap ascending — enables single cumulative walk
        eligible.sort()

        # Walk through trades, emitting stats at each 10M boundary where trades change
        ei = 0          # index into eligible
        n_el = len(eligible)
        pnls: list  = []
        peaks: list = []
        last_emitted_ei = -1  # track whether trade set changed since last emit

        # Build list of mcap thresholds to evaluate: each unique 10M bucket that
        # contains at least one trade, plus MCAP_MAX as the final bucket
        buckets = set()
        for mcap, _, _ in eligible:
            buckets.add(((int(mcap) // MCAP_STEP) + 1) * MCAP_STEP)
        buckets.add(MCAP_MAX)
        mcap_thresholds = sorted(b for b in buckets if b <= MCAP_MAX)

        for max_mcap in mcap_thresholds:
            # Add all trades whose mcap falls within this threshold
            while ei < n_el and eligible[ei][0] <= max_mcap:
                pnls.append(eligible[ei][1])
                peaks.append(eligible[ei][2])
                ei += 1

            if not pnls:
                continue

            # Only recompute stats if the trade set actually changed
            if ei == last_emitted_ei:
                continue
            last_emitted_ei = ei

            trades = [{"pnl": p, "peak": pk} for p, pk in zip(pnls, peaks)]
            stats = _stats(trades, tp, sl, threshold, hold, max_mcap, date_range_days)
            opt_rows.append(stats)

            combo_score = _rank_score(stats)
            if combo_score > best_score:
                best_score = combo_score
                best_combo = stats

        if (combo_idx + 1) % 500 == 0:
            log.info(f"  [{combo_idx+1}/{len(base_combos)}] combos swept, "
                     f"best SQN so far: {best_score:.3f}")

    _write_opt_results(opt_rows)
    _print_top10(opt_rows, best_combo)
    return best_combo


def optimize_from_api(start_date: str, end_date: str, max_records: int = 1000):
    """Full end-to-end optimization — fetches from SAM.gov and runs all combos.
    Slow. Use optimize_from_cache instead after first backtest run.
    """
    from backtest import run_backtest

    # First run a base backtest to populate the cache
    cache_file = os.path.join(os.path.dirname(__file__), "backtest_results.csv")
    log.info("Running base backtest to build cache...")
    run_backtest(start_date, end_date, max_records=max_records)

    # Then optimize from the cache
    return optimize_from_cache(cache_file)


def _rank_score(stats) -> float:
    """SQN (Van Tharp System Quality Number): (expectancy / std_dev) * sqrt(min(n, 100)).

    Requires >= 15 trades for statistical validity. Higher is better.
    """
    import math
    n = stats.get("trades", 0)
    if n < 15:
        return -999
    exp = stats.get("expectancy", 0)
    std = stats.get("std_dev_pnl", 0)
    if std <= 0:
        return -999
    return (exp / std) * math.sqrt(min(n, 100))


def _stats(trades, tp, sl, threshold, hold, max_market_cap=None, date_range_days=None):
    """Calculate stats from trades list (dicts with pnl/peak keys)."""
    import statistics as _stats_mod
    n = len(trades)
    base = {"trades": 0, "win_rate": 0, "avg_pnl_pct": 0,
            "total_pnl_pct": 0, "expectancy": -999, "std_dev_pnl": 0,
            "profit_factor": 0, "avg_win": 0, "avg_loss": 0,
            "avg_peak_pnl": 0, "peak_pnl_pct": 0, "max_drawdown_pct": 0,
            "trades_per_week": 0, "avg_pnl_per_week": 0,
            "tp_pct": round(tp * 100, 1), "sl_pct": round(sl * 100, 1),
            "score_threshold": threshold, "max_hold_days": hold,
            "max_market_cap_m": round(max_market_cap / 1_000_000) if max_market_cap else None}
    if n == 0:
        return base

    pnls  = [t if isinstance(t, (int, float)) else t["pnl"] for t in trades]
    peaks = [t.get("peak", 0) if isinstance(t, dict) else 0 for t in trades]

    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    avg    = sum(pnls) / n
    win_rate = len(wins) / n

    avg_win  = sum(wins) / len(wins) if wins else 0
    avg_loss = abs(sum(losses) / len(losses)) if losses else 0
    avg_peak = sum(peaks) / n if peaks else 0

    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
    std_dev    = _stats_mod.stdev(pnls) if n >= 2 else 0

    gross_wins   = sum(wins) if wins else 0
    gross_losses = abs(sum(losses)) if losses else 0
    if gross_losses > 0:
        profit_factor = min(gross_wins / gross_losses, 99.0)
    elif gross_wins > 0:
        profit_factor = 99.0
    else:
        profit_factor = 0.0

    # Max drawdown on cumulative P&L stream
    cumulative, peak_cum, max_dd = 0, 0, 0
    for p in pnls:
        cumulative += p
        if cumulative > peak_cum:
            peak_cum = cumulative
        dd = peak_cum - cumulative
        if dd > max_dd:
            max_dd = dd

    peak_pnl = max(pnls) if pnls else 0

    # Temporal metrics
    weeks = (date_range_days / 7) if date_range_days and date_range_days > 0 else None
    trades_per_week  = round(n / weeks, 2) if weeks else 0
    avg_pnl_per_week = round(sum(pnls) / weeks, 2) if weeks else 0

    base.update({
        "trades": n,
        "win_rate": round(win_rate * 100, 1),
        "avg_pnl_pct": round(avg, 3),
        "total_pnl_pct": round(sum(pnls), 2),
        "expectancy": round(expectancy, 4),
        "std_dev_pnl": round(std_dev, 4),
        "profit_factor": round(profit_factor, 3),
        "avg_win": round(avg_win, 3),
        "avg_loss": round(avg_loss, 3),
        "avg_peak_pnl": round(avg_peak, 2),
        "peak_pnl_pct": round(peak_pnl, 2),
        "max_drawdown_pct": round(max_dd, 2),
        "trades_per_week": trades_per_week,
        "avg_pnl_per_week": avg_pnl_per_week,
    })
    return base


def _print_top10(opt_rows, best_combo):
    print("\n" + "=" * 135)
    print("  OPTIMIZER RESULTS — TOP 10 BY TOTAL % RETURN")
    print("=" * 135)
    sorted_rows = sorted(opt_rows, key=lambda r: r.get("total_pnl_pct", -999), reverse=True)
    print(f"  {'Threshold':>9} {'TP%':>5} {'SL%':>5} {'Hold':>5} {'MaxMcap':>8} "
          f"{'Trades':>7} {'Total%':>8} {'Trd/Wk':>8} {'PnL/Wk':>8} {'Expect':>8} {'SQN':>7}")
    print("-" * 120)
    for r in sorted_rows[:10]:
        sqn = _rank_score(r)
        sqn_str = f"{sqn:.2f}" if sqn > -999 else "n/a"
        mcap = r.get("max_market_cap_m")
        mcap_str = f"${mcap}M" if mcap else "n/a"
        print(f"  {r['score_threshold']:>9} {r['tp_pct']:>4.1f}% {r['sl_pct']:>4.1f}% "
              f"{r['max_hold_days']:>5} {mcap_str:>8} {r['trades']:>7} "
              f"{r['total_pnl_pct']:>+7.2f}% {r.get('trades_per_week', 0):>7.2f} "
              f"{r.get('avg_pnl_per_week', 0):>+7.2f}% {r['expectancy']:>+7.3f}% {sqn_str:>7}")
    if best_combo:
        sqn = _rank_score(best_combo)
        mcap = best_combo.get("max_market_cap_m")
        print(f"\n  >>> BEST COMBO (highest SQN score):")
        print(f"      Score Threshold  : {best_combo['score_threshold']}")
        print(f"      Take Profit      : {best_combo['tp_pct']:.1f}%")
        print(f"      Stop Loss        : {best_combo['sl_pct']:.1f}%")
        print(f"      Hold Days        : {best_combo['max_hold_days']}")
        print(f"      Max Market Cap   : ${mcap}M" if mcap else "      Max Market Cap   : n/a")
        print(f"      Trades           : {best_combo['trades']}")
        print(f"      Win Rate         : {best_combo['win_rate']}%")
        print(f"      Total Return     : {best_combo['total_pnl_pct']:+.2f}%")
        print(f"      Avg P&L/Trade    : {best_combo['avg_pnl_pct']:+.3f}%")
        print(f"      Avg Win / Loss   : +{best_combo['avg_win']:.3f}% / -{best_combo['avg_loss']:.3f}%")
        print(f"      Expectancy       : {best_combo['expectancy']:+.4f}% per trade")
        print(f"      Std Dev PnL      : {best_combo.get('std_dev_pnl', 0):.4f}%")
        print(f"      SQN Score        : {sqn:.3f}")
        print(f"      Trades / Week    : {best_combo.get('trades_per_week', 0):.2f}")
        print(f"      Avg PnL / Week   : {best_combo.get('avg_pnl_per_week', 0):+.2f}%")
        print(f"      Profit Factor    : {best_combo['profit_factor']:.2f}x")
        print(f"      Max Drawdown     : -{best_combo.get('max_drawdown_pct', 0):.2f}%")
        print(f"      Avg Peak Intraday: {best_combo.get('avg_peak_pnl', 0):+.2f}%")
    else:
        print("\n  (No combos produced any trades — check data or thresholds)")
    print("=" * 135)
    print(f"  Full results -> {OPT_RESULTS_FILE}\n")


def _load_csv(filepath):
    with open(filepath, "r") as f:
        return list(csv.DictReader(f))


def _write_opt_results(rows):
    if not rows:
        return
    rows_sorted = sorted(rows, key=lambda r: _rank_score(r), reverse=True)
    fields = list(rows_sorted[0].keys())
    with open(OPT_RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows_sorted)
    log.info(f"Optimizer results written to {OPT_RESULTS_FILE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAMgovArby Parameter Optimizer")
    subparsers = parser.add_subparsers(dest="mode")

    # Mode 1: from training CSV (fully offline — recommended)
    training_parser = subparsers.add_parser(
        "from-training-csv",
        help="Fully offline optimize from OHLC-enriched training CSV (run enrich_ohlc.py first)"
    )
    training_parser.add_argument("file", help="Path to training CSV with OHLC columns")
    training_parser.add_argument("--start", default=None, help="Filter start date YYYY-MM-DD")
    training_parser.add_argument("--end",   default=None, help="Filter end date YYYY-MM-DD")

    # Mode 2: from cache (uses yfinance for price re-simulation)
    cache_parser = subparsers.add_parser("from-cache", help="Optimize from existing backtest CSV (calls yfinance)")
    cache_parser.add_argument("file", nargs="?", default="backtest_results.csv")

    # Mode 3: from API
    api_parser = subparsers.add_parser("from-api", help="Fetch from SAM.gov then optimize")
    api_parser.add_argument("--start", required=True)
    api_parser.add_argument("--end", required=True)
    api_parser.add_argument("--max-records", type=int, default=1000)

    # Add verbosity flags to main parser
    add_verbosity_flags(parser)

    args = parser.parse_args()

    # Initialize logger with user's verbosity preference
    log = setup_logging("optimizer", quiet=args.quiet, verbose=args.verbose, json_format=args.json)

    if args.mode == "from-training-csv":
        optimize_from_training_csv(args.file,
                                   start_date=args.start,
                                   end_date=args.end)
    elif args.mode == "from-cache":
        optimize_from_cache(args.file)
    elif args.mode == "from-api":
        optimize_from_api(args.start, args.end, args.max_records)
    else:
        parser.print_help()
