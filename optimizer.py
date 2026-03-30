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
from price_sim import simulate_trade, simulate_trade_from_row
from scoring_engine import score_contract
from filter_engine_bt import apply_filters_bt_from_training

log = logging.getLogger("optimizer")

OPT_RESULTS_FILE = os.path.join(os.path.dirname(__file__), "optimizer_results.csv")

# Parameter grid - optimizes: score threshold, TP/SL/hold, and market cap filter
PARAM_GRID = {
    "score_threshold": [5, 10, 15, 20, 25, 30, 35, 40, 50],  # Score threshold
    "take_profit_pct": [0.05, 0.08, 0.10, 0.12, 0.15, 0.20],  # TP targets
    "stop_loss_pct":   [0.02, 0.03, 0.04, 0.05, 0.06],  # SL targets (all < max TP of 0.20)
    "max_hold_days":   [1, 2, 3, 4, 5, 7],  # Hold period
    "max_market_cap":  [100_000_000, 150_000_000, 200_000_000, 300_000_000, 500_000_000, 1_000_000_000],  # Market cap filter
}


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

    combos = list(itertools.product(
        PARAM_GRID["score_threshold"],
        PARAM_GRID["take_profit_pct"],
        PARAM_GRID["stop_loss_pct"],
        PARAM_GRID["max_hold_days"],
    ))
    log.info(f"Testing {len(combos)} parameter combinations")

    best_score = -999
    best_combo = None
    opt_rows = []

    for threshold, tp, sl, hold in combos:
        if sl >= tp:
            log.debug(f"  Skipping combo threshold={threshold} tp={tp*100:.0f}% sl={sl*100:.0f}% hold={hold}d: SL >= TP")
            continue

        trades = []
        for row in eligible:
            # Re-check score threshold
            score = row.get("score")
            try:
                score = float(score) if score else 0
            except (ValueError, TypeError):
                continue
            if score < threshold:
                continue

            # Re-simulate with new TP/SL/hold
            ticker = row["ticker"]
            award_date = row["award_date"][:10]
            sim = simulate_trade(ticker, award_date, tp, sl, hold)
            if sim:
                trades.append(sim["pnl_pct"])

        stats = _stats(trades, tp, sl, threshold, hold, mcap_limit=None)
        opt_rows.append(stats)

        combo_score = _rank_score(stats)
        if combo_score > best_score:
            best_score = combo_score
            best_combo = stats

        log.debug(f"  threshold={threshold} tp={tp*100:.0f}% sl={sl*100:.0f}% "
                  f"hold={hold}d -> {stats['trades']} trades "
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
                if (not start_date or r.get("posted_date", "")[:10] >= start_date)
                and (not end_date   or r.get("posted_date", "")[:10] <= end_date)]
        log.info(f"Date filter {start_date} -> {end_date}: {before} -> {len(rows)} rows")

    # Filter for rows with OHLC data (tickers that were enriched)
    rows = [r for r in rows if r.get("open_t0", "").strip() and r.get("ticker", "").strip()]
    log.info(f"Rows with OHLC data: {len(rows)}")

    if not rows:
        log.error("No rows with OHLC data. Run: enrich_ohlc.py datasets/training_set_final.csv")
        return None

    combos = list(itertools.product(
        PARAM_GRID["score_threshold"],
        PARAM_GRID["take_profit_pct"],
        PARAM_GRID["stop_loss_pct"],
        PARAM_GRID["max_hold_days"],
        PARAM_GRID["max_market_cap"],
    ))
    log.info(f"Testing {len(combos)} parameter combinations (fully offline)")

    best_score = -999
    best_combo = None
    opt_rows = []

    # Pre-parse filter results for each row at max market cap ($5B) to avoid redundant work.
    # Then apply per-combo market cap cutoff cheaply.
    # This is safe because filters are stateless with respect to combo parameters.
    row_filter_cache = []
    for row in rows:
        passed, _, extra = apply_filters_bt_from_training(row)
        row_filter_cache.append((passed, extra))

    for combo_idx, (threshold, tp, sl, hold, mcap_limit) in enumerate(combos):
        if sl >= tp:
            log.debug(f"  Skipping combo threshold={threshold} tp={tp*100:.0f}% sl={sl*100:.0f}% hold={hold}d: SL >= TP")
            continue

        trades = []
        seen_trades = set()  # dedup by (ticker, date)
        for row_idx, row in enumerate(rows):
            passed, extra = row_filter_cache[row_idx]
            if not passed:
                continue

            # Apply market cap filter with this combo's limit
            market_cap = extra.get("market_cap", 0)
            if market_cap > mcap_limit:
                continue

            # Re-score with stored signals
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

            score, _ = score_contract(contract, market_cap,
                                      threshold=threshold, has_press_release=has_pr)
            if score < threshold:
                continue

            # Dedup by ticker+date
            ticker = row.get("ticker", "")
            award_date = row.get("posted_date", "")[:10]
            key = (ticker, award_date)
            if key in seen_trades:
                continue
            seen_trades.add(key)

            # Simulate from stored OHLC — zero API calls
            sim = simulate_trade_from_row(row, tp, sl, hold)
            if sim:
                trades.append({
                    "pnl": sim["pnl_pct"],
                    "peak": sim.get("peak_pnl_pct", 0),
                    "return_t7": sim.get("return_t7", 0)
                })

        stats = _stats(trades, tp, sl, threshold, hold, mcap_limit)
        opt_rows.append(stats)

        combo_score = _rank_score(stats)
        if combo_score > best_score:
            best_score = combo_score
            best_combo = stats

        if (combo_idx + 1) % 50 == 0:
            log.info(f"  [{combo_idx+1}/{len(combos)}] combos tested, "
                     f"best rank-score so far: {best_score:.3f}")

    _write_opt_results(opt_rows)
    _print_top10(opt_rows, best_combo)
    return best_combo


def optimize_from_api(start_date: str, end_date: str, max_records: int = 1000):
    """Full end-to-end optimization — fetches from SAM.gov and runs all combos.
    Slow. Use optimize_from_cache instead after first backtest run.
    """
    from backtest import run_backtest
    import tempfile

    # First run a base backtest to populate the cache
    cache_file = os.path.join(os.path.dirname(__file__), "backtest_results.csv")
    log.info("Running base backtest to build cache...")
    run_backtest(start_date, end_date, max_records=max_records)

    # Then optimize from the cache
    return optimize_from_cache(cache_file)


def _rank_score(stats) -> float:
    """Rank by highest total % return (total_pnl_pct).

    Higher is better.
    """
    n = stats.get("trades", 0)
    if n < 1:
        return -999
    # Rank by total % return
    return stats.get("total_pnl_pct", -999)


def _stats(trades, tp, sl, threshold, hold, mcap_limit=None):
    """Calculate stats from trades list (now dicts with pnl/peak/return_t7)."""
    import math
    n = len(trades)
    base = {"trades": 0, "win_rate": 0, "avg_pnl_pct": 0,
            "total_pnl_pct": 0, "expectancy": -999,
            "profit_factor": 0, "avg_win": 0, "avg_loss": 0,
            "avg_peak_pnl": 0, "avg_return_t7": 0,
            "peak_pnl_pct": 0, "sharpe": 0, "max_drawdown_pct": 0,
            "tp_pct": tp*100, "sl_pct": sl*100,
            "score_threshold": threshold, "max_hold_days": hold,
            "max_mcap_M": round(mcap_limit / 1e6) if mcap_limit else "N/A"}
    if n == 0:
        return base

    # Extract PnL values (backward compatible with old float format)
    pnls = [t if isinstance(t, (int, float)) else t["pnl"] for t in trades]
    peaks = [t.get("peak", 0) if isinstance(t, dict) else 0 for t in trades]
    returns_t7 = [t.get("return_t7", 0) if isinstance(t, dict) else 0 for t in trades]

    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    avg = sum(pnls) / n
    win_rate = len(wins) / n

    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = abs(sum(losses) / len(losses)) if losses else 0
    avg_peak = sum(peaks) / n if peaks else 0
    avg_t7 = sum(returns_t7) / n if returns_t7 else 0

    # Expectancy: (win_rate × avg_win) - (loss_rate × avg_loss)
    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

    # Profit factor: cap at 99 instead of inf so CSV output stays numeric
    gross_wins = sum(wins) if wins else 0
    gross_losses = abs(sum(losses)) if losses else 0
    if gross_losses > 0:
        profit_factor = min(gross_wins / gross_losses, 99.0)
    elif gross_wins > 0:
        profit_factor = 99.0  # all wins — cap at 99 for CSV compatibility
    else:
        profit_factor = 0.0

    # Sharpe (annualised, approximate)
    if n > 1:
        variance = sum((p - avg) ** 2 for p in pnls) / n
        std = math.sqrt(variance)
        sharpe = (avg / std * math.sqrt(252 / n)) if std > 0 else 0
    else:
        sharpe = 0

    # Max drawdown on cumulative P&L stream
    cumulative = 0
    peak = 0
    max_dd = 0
    for p in pnls:
        cumulative += p
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

    # Peak PnL from individual trades
    peak_pnl = max(pnls) if pnls else 0

    base.update({
        "trades": n,
        "win_rate": round(win_rate * 100, 1),
        "avg_pnl_pct": round(avg, 3),
        "total_pnl_pct": round(sum(pnls), 2),
        "expectancy": round(expectancy, 4),
        "profit_factor": round(profit_factor, 3),
        "avg_win": round(avg_win, 3),
        "avg_loss": round(avg_loss, 3),
        "avg_peak_pnl": round(avg_peak, 2),
        "avg_return_t7": round(avg_t7, 2),
        "peak_pnl_pct": round(peak_pnl, 2),
        "sharpe": round(sharpe, 3),
        "max_drawdown_pct": round(max_dd, 2),
    })
    return base


def _print_top10(opt_rows, best_combo):
    print("\n" + "=" * 110)
    print("  OPTIMIZER RESULTS — TOP 10 BY TOTAL % RETURN")
    print("=" * 110)
    # Sort by total return % (highest first)
    sorted_rows = sorted(opt_rows, key=lambda r: r.get("total_pnl_pct", -999), reverse=True)
    print(f"  {'Threshold':>9} {'MaxMcap':>8} {'TP%':>5} {'SL%':>5} {'Hold':>5} "
          f"{'Trades':>7} {'Total%':>8} {'AvgPeak%':>9} {'Avg7d%':>8} {'Expect':>8} {'Sharpe':>7}")
    print("-" * 110)
    for r in sorted_rows[:10]:
        mcap_str = f"${r['max_mcap_M']}M" if isinstance(r['max_mcap_M'], int) else r['max_mcap_M']
        print(f"  {r['score_threshold']:>9} {mcap_str:>8} {r['tp_pct']:>5.1f} {r['sl_pct']:>5.1f} "
              f"{r['max_hold_days']:>5} {r['trades']:>7} "
              f"{r['total_pnl_pct']:>+7.2f}% {r.get('avg_peak_pnl', 0):>+8.2f}% {r.get('avg_return_t7', 0):>+7.2f}% "
              f"{r['expectancy']:>+7.3f}% {r.get('sharpe', 0):>6.3f}")
    if best_combo:
        mcap_str = f"${best_combo['max_mcap_M']}M" if isinstance(best_combo['max_mcap_M'], int) else best_combo['max_mcap_M']
        print(f"\n  >>> BEST COMBO (highest total % return):")
        print(f"      Score Threshold : {best_combo['score_threshold']}")
        print(f"      Max Market Cap  : {mcap_str}")
        print(f"      Take Profit     : {best_combo['tp_pct']:.1f}%")
        print(f"      Stop Loss       : {best_combo['sl_pct']:.1f}%")
        print(f"      Hold Days       : {best_combo['max_hold_days']}")
        print(f"      Trades          : {best_combo['trades']}")
        print(f"      Win Rate        : {best_combo['win_rate']}%")
        print(f"      Total Return    : {best_combo['total_pnl_pct']:+.2f}%")
        print(f"      Avg Peak Intraday: {best_combo.get('avg_peak_pnl', 0):+.2f}%")
        print(f"      Avg 7-Day Return : {best_combo.get('avg_return_t7', 0):+.2f}%")
        print(f"      Peak Single Trade : {best_combo.get('peak_pnl_pct', 0):+.2f}%")
        print(f"      Avg P&L         : {best_combo['avg_pnl_pct']:+.2f}%")
        print(f"      Expectancy      : {best_combo['expectancy']:+.3f}% per trade")
        print(f"      Sharpe (approx) : {best_combo.get('sharpe', 0):.3f}")
        print(f"      Max Drawdown    : -{best_combo.get('max_drawdown_pct', 0):.2f}%")
        print(f"      Profit Factor   : {best_combo['profit_factor']:.2f}x")
        print(f"      Avg Win / Loss  : +{best_combo['avg_win']:.2f}% / -{best_combo['avg_loss']:.2f}%")
    else:
        print("\n  (No combos produced any trades — check data or thresholds)")
    print("=" * 110)
    print(f"  Full results -> {OPT_RESULTS_FILE}\n")


def _load_csv(filepath):
    with open(filepath, "r") as f:
        return list(csv.DictReader(f))


def _write_opt_results(rows):
    if not rows:
        return
    fields = list(rows[0].keys())
    with open(OPT_RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
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
