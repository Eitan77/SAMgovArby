"""Backtest runner: replay historical SAM.gov awards through the full pipeline.

Usage:
    python backtest.py --start 2023-01-01 --end 2023-12-31 [--quiet] [--verbose]
    python backtest.py --start 2022-01-01 --end 2024-01-01 --max-records 2000 [--quiet]
"""
import argparse
import csv
import glob
import json as _json
import logging
import os
import re
import sys
import time
from datetime import datetime

import polars as pl

from config_logging import setup_logging, add_verbosity_flags
from usaspending_poller import fetch_awards_range
from historical_poller import date_range_chunks
from watchlist_poller import fetch_awards_for_watchlist
from filter_engine_bt import apply_filters_bt, apply_filters_bt_from_training
from scoring_engine import score_contract
from ticker_resolver import resolve_ticker
from price_sim import simulate_trade, simulate_trade_from_row, get_historical_market_cap
from award_cache import load_from_cache, save_to_cache
from config import SCORE_THRESHOLD, TAKE_PROFIT_PCT, STOP_LOSS_PCT, MAX_HOLD_DAYS

log = logging.getLogger("backtest")

RESULTS_FILE = os.path.join(os.path.dirname(__file__), "backtest_results.csv")  # Overridden by run_backtest with year-specific name
RESULTS_DETAILED_FILE = os.path.join(os.path.dirname(__file__), "backtest_results_detailed.csv")
RESULTS_FIELDS = [
    "award_date", "awardee_name", "agency", "award_amount", "naics",
    "sole_source", "filter_result", "filter_reason",
    "first_8k_date", "first_pr_date", "last_dilutive_filing_date",
    "dilutive_filing_type", "agency_prior_win_count", "ticker_confidence",
    "score", "ticker", "market_cap", "value_to_mcap_pct",
    "entry_date", "entry_price", "exit_date", "exit_price",
    "exit_reason", "pnl_pct", "hit_tp", "hit_sl", "timed_out",
    "tp_target", "sl_target", "return_t7", "peak_pnl_pct",
]

MIN_CONTRACT_VALUE = 1_000_000
MAX_AWARD_AMOUNT = 10_000_000_000


def _build_funnel_breakdown(all_results, training_csv=None):
    """Build a complete funnel breakdown from Stage 1 (raw CSV) through backtest.

    Reads stage1/stage2 checkpoint files (fast) instead of re-scanning the FY CSV.
    Returns dict with counts at each filtering stage.
    """
    breakdown = {
        # Stage 1 — raw load & filter
        "raw_rows_read": 0,
        "after_dedup_amount": 0,
        "stage1_top20": 0,
        "stage1_idiq": 0,
        "stage1_total": 0,        # final count after all stage1 filters
        # Stage 2 — ticker resolution
        "stage2_ticker_resolved": 0,
        "stage2_ticker_failed": 0,
        # Stage 3 — enriched training CSV
        "stage3_after_enrich": 0,
        # Backtest filters
        "backtest_market_cap": 0,
        "backtest_8k": 0,
        "backtest_dilutive": 0,
        "backtest_low_score": 0,
        "backtest_no_ticker": 0,
        "backtest_no_price": 0,
        "backtest_duplicate": 0,
        "traded": 0,
    }

    script_dir = os.path.dirname(os.path.abspath(__file__))
    cp_dir = os.path.join(script_dir, "datasets", "checkpoints")

    # Stage 1 — read from checkpoint (instant, no FY CSV needed)
    try:
        cp1_path = os.path.join(cp_dir, "stage1_filter.json")
        if os.path.exists(cp1_path):
            with open(cp1_path) as f:
                cp1 = _json.load(f)
            breakdown["raw_rows_read"]      = cp1.get("total_rows_read", 0)
            breakdown["after_dedup_amount"] = cp1.get("unique_after_dedup_and_amount_filter", 0)
            breakdown["stage1_top20"]       = cp1.get("dropped_top20", 0)
            breakdown["stage1_idiq"]        = cp1.get("dropped_idiq", 0)
            breakdown["stage1_total"]       = cp1.get("final_count", 0)
            log.debug(f"Stage 1 checkpoint loaded: {breakdown['stage1_total']:,} contracts after filters")
    except Exception as e:
        log.debug(f"Could not load stage1 checkpoint: {e}")

    # Stage 2 — count resolved vs unresolved from checkpoint
    try:
        cp2_path = os.path.join(cp_dir, "stage2_tickers.json")
        if os.path.exists(cp2_path):
            with open(cp2_path) as f:
                cp2 = _json.load(f)
            resolved   = sum(1 for v in cp2.values() if isinstance(v, dict) and v.get("ticker"))
            unresolved = sum(1 for v in cp2.values() if isinstance(v, dict) and not v.get("ticker"))
            breakdown["stage2_ticker_resolved"] = resolved
            breakdown["stage2_ticker_failed"]   = unresolved
            log.debug(f"Stage 2 checkpoint loaded: {resolved:,} resolved, {unresolved:,} unresolved")
    except Exception as e:
        log.debug(f"Could not load stage2 checkpoint: {e}")

    # Stage 3 — count rows in training CSV
    if training_csv and os.path.exists(training_csv):
        try:
            with open(training_csv) as f:
                training_rows = sum(1 for _ in f) - 1  # Exclude header
            breakdown["stage3_after_enrich"] = training_rows
        except Exception as e:
            log.debug(f"Could not count training CSV rows: {e}")

    # Count backtest filter removals from all_results
    for result in all_results:
        reason = result.get("filter_reason", "")
        fr = result.get("filter_result", "")

        if fr == "pass":
            breakdown["traded"] += 1
        elif fr == "low_score":
            breakdown["backtest_low_score"] += 1
        elif fr == "no_ticker":
            breakdown["backtest_no_ticker"] += 1
        elif fr == "no_price_data":
            breakdown["backtest_no_price"] += 1
        elif fr == "duplicate":
            breakdown["backtest_duplicate"] += 1
        elif fr == "fail":
            if re.search(r"market cap.*exceeds", reason, re.I):
                breakdown["backtest_market_cap"] += 1
            elif re.search(r"8-K filed", reason, re.I):
                breakdown["backtest_8k"] += 1
            elif re.search(r"dilutive", reason, re.I):
                breakdown["backtest_dilutive"] += 1

    return breakdown

def run_backtest(start_date: str, end_date: str, max_records: int = 5000,
                 tp: float = TAKE_PROFIT_PCT, sl: float = STOP_LOSS_PCT,
                 hold: int = MAX_HOLD_DAYS, threshold: int = SCORE_THRESHOLD,
                 output_file: str = RESULTS_FILE, use_cache: bool = True,
                 watchlist_mode: bool = False,
                 dataset_file: str = None,
                 training_csv: str = None,
                 max_market_cap: int = None):
    """Run a full backtest and write results CSV. Returns summary stats dict.

    If training_csv is provided, uses the pre-built training CSV with historical
    signals (8-K, press release, historical market cap) for accurate backtesting.
    If max_market_cap is provided, overrides config.MAX_MARKET_CAP for this run.
    """
    import config as config_module

    # Override max_market_cap if provided
    old_max_market_cap = None
    if max_market_cap is not None:
        old_max_market_cap = config_module.MAX_MARKET_CAP
        config_module.MAX_MARKET_CAP = max_market_cap

    mcap_str = f" | MaxMCap=${max_market_cap/1e9:.1f}B" if max_market_cap else ""
    log.info(f"Backtest: {start_date} -> {end_date} | "
             f"TP={tp*100:.0f}% SL={sl*100:.0f}% Hold={hold}d Threshold={threshold}{mcap_str}")

    # Training CSV mode: use historical data for all signals
    if training_csv:
        result = _run_backtest_from_training(
            training_csv, start_date, end_date, max_records,
            tp, sl, hold, threshold, output_file, max_market_cap
        )
        if old_max_market_cap is not None:
            config_module.MAX_MARKET_CAP = old_max_market_cap
        return result

    all_results = []
    total_fetched = 0

    import json as _json
    awards = []

    if dataset_file:
        # Fastest path: pre-built dataset, no API calls
        log.info(f"Loading pre-built dataset: {dataset_file}")
        with open(dataset_file) as f:
            awards = _json.load(f)
        log.info(f"Loaded {len(awards)} awards from dataset")
    elif use_cache:
        cached = load_from_cache(start_date, end_date)
        if cached:
            awards = cached
            total_fetched = len(awards)

    if not awards and not dataset_file:
        if watchlist_mode:
            log.info("Watchlist mode: fetching awards for known small-cap defense stocks")
            awards = fetch_awards_for_watchlist(start_date, end_date)
        else:
            log.info(f"Fetching {start_date} -> {end_date} from USASpending.gov")
            awards = fetch_awards_range(start_date, end_date, max_records=max_records)
        total_fetched = len(awards)
        if use_cache:
            save_to_cache(awards, start_date, end_date)

    # Process awards (runs whether from cache or fresh fetch)
    ticker_cache = {}  # in-memory cache: company_name -> (ticker, market_cap, edgar_results)
    total_to_process = min(len(awards), max_records)
    signals = 0
    filtered = 0
    seen_trades = set()  # (ticker, date) dedup
    t_start = time.time()

    log.info(f"Processing {total_to_process} awards... (updates every 10)")
    for i, contract in enumerate(awards[:max_records]):
        result = _process_contract(contract, tp, sl, hold, threshold, ticker_cache)

        # Deduplicate: only one trade per ticker per day
        if result.get("filter_result") == "pass" and result.get("ticker"):
            key = (result["ticker"], result.get("award_date", "")[:10])
            if key in seen_trades:
                result["filter_result"] = "duplicate"
                result["filter_reason"] = f"Duplicate trade for {key[0]} on {key[1]}"
                for fld in ("entry_price", "exit_price", "pnl_pct", "entry_date",
                            "exit_date", "exit_reason", "hit_tp", "hit_sl", "timed_out"):
                    result.pop(fld, None)
            else:
                seen_trades.add(key)

        all_results.append(result)

        fr = result.get("filter_result", "")
        if fr == "pass":
            signals += 1
        else:
            filtered += 1

        if (i + 1) % 10 == 0:
            elapsed = time.time() - t_start
            rate = elapsed / (i + 1)
            remaining = rate * (total_to_process - i - 1)
            mins, secs = divmod(int(remaining), 60)
            log.info(
                f"  [{i+1}/{total_to_process}] "
                f"signals={signals} filtered={filtered} | "
                f"~{mins}m{secs:02d}s remaining | "
                f"last: {contract['awardee_name'][:35]} -> {fr}"
            )

    # Write CSV
    _write_results(all_results, output_file)

    # Compute stats on traded contracts only
    traded = [r for r in all_results if r.get("entry_price")]
    stats = _compute_stats(traded, tp, sl)
    _print_report(stats, all_results, traded, start_date, end_date)

    # Build funnel breakdown and save to JSON for GUI
    breakdown = _build_funnel_breakdown(all_results, training_csv=None)
    breakdown_file = os.path.join(os.path.dirname(__file__), "backtest_breakdown_2023.json")
    with open(breakdown_file, "w") as f:
        _json.dump(breakdown, f)

    # Restore original max_market_cap if it was overridden
    if old_max_market_cap is not None:
        config_module.MAX_MARKET_CAP = old_max_market_cap

    return stats, breakdown, all_results


def _process_contract(contract, tp, sl, hold, threshold, ticker_cache=None):
    """Run one contract through filter -> score -> resolve -> simulate."""
    base = {
        "award_date": contract.get("posted_date", ""),
        "awardee_name": contract["awardee_name"],
        "agency": contract["agency"],
        "award_amount": contract["award_amount"],
        "naics": contract["naics"],
        "sole_source": contract["sole_source"],
    }

    # Filter (backtest-safe version — no news/8-K checks)
    passed, reason, extra = apply_filters_bt(contract, ticker_cache=ticker_cache)
    base["filter_result"] = "pass" if passed else "fail"
    base["filter_reason"] = reason

    if not passed:
        return base

    # Score — use historical market cap when available
    market_cap = extra.get("market_cap", 0)
    if not market_cap and extra.get("ticker"):
        market_cap = get_historical_market_cap(
            extra["ticker"], contract.get("posted_date", ""))
        extra["market_cap"] = market_cap

    # agency_prior_win_count is not available in non-training mode; 0 → first win
    # but we don't have full history here so leave is_first_agency_win=None
    # (scores 0 pts conservatively — consistent with live mode)
    score, breakdown = score_contract(contract, market_cap, threshold=threshold)
    base["score"] = score
    base["market_cap"] = round(market_cap)
    base["value_to_mcap_pct"] = breakdown.get("value_to_mcap", {}).get("ratio", 0)

    log.info(f"Score {score}/100 | mcap=${market_cap:,.0f} | {contract['awardee_name'][:40]} | Pass={score >= threshold}")

    if score < threshold:
        base["filter_result"] = "low_score"
        base["filter_reason"] = f"Score {score} < threshold {threshold}"
        return base

    # Resolve ticker
    ticker = extra.get("ticker")
    if not ticker:
        ticker, confidence = resolve_ticker(
            contract["awardee_name"],
            edgar_results=extra.get("edgar_results"),
        )

    base["ticker"] = ticker or ""
    if not ticker:
        base["filter_result"] = "no_ticker"
        base["filter_reason"] = "Ticker resolution failed"
        return base

    # Simulate price action
    award_date = contract.get("posted_date", "")[:10]
    sim = simulate_trade(ticker, award_date, tp, sl, hold)
    if sim:
        base.update({k: sim[k] for k in sim if k != "ticker"})
    else:
        base["filter_result"] = "no_price_data"
        base["filter_reason"] = "Price data unavailable"

    return base


def _run_backtest_from_training(csv_path, start_date, end_date, max_records,
                                tp, sl, hold, threshold, output_file, max_market_cap=None):
    """Run backtest using pre-built training CSV with historical signals."""
    import csv as _csv

    log.info(f"Loading training CSV: {csv_path}")
    with open(csv_path, encoding="utf-8") as f:
        reader = _csv.DictReader(f)
        rows = list(reader)
    log.info(f"Loaded {len(rows)} rows from training CSV")

    # Filter by date range
    rows = [r for r in rows if start_date <= r.get("posted_date", "")[:10] <= end_date]
    log.info(f"{len(rows)} rows within date range {start_date} -> {end_date}")

    all_results = []
    total_to_process = min(len(rows), max_records)
    signals = 0
    filtered = 0
    seen_trades = set()  # (ticker, date) dedup
    t_start = time.time()

    log.info(f"Processing {total_to_process} awards from training data...")
    for i, row in enumerate(rows[:max_records]):
        result = _process_training_row(row, tp, sl, hold, threshold)

        # Deduplicate: only one trade per ticker per day
        if result.get("filter_result") == "pass" and result.get("ticker"):
            key = (result["ticker"], result.get("award_date", "")[:10])
            if key in seen_trades:
                result["filter_result"] = "duplicate"
                result["filter_reason"] = f"Duplicate trade for {key[0]} on {key[1]}"
                # Clear trade fields so it doesn't count as traded
                for fld in ("entry_price", "exit_price", "pnl_pct", "entry_date",
                            "exit_date", "exit_reason", "hit_tp", "hit_sl", "timed_out"):
                    result.pop(fld, None)
            else:
                seen_trades.add(key)

        all_results.append(result)

        fr = result.get("filter_result", "")
        if fr == "pass":
            signals += 1
        else:
            filtered += 1

        if (i + 1) % 10 == 0:
            elapsed = time.time() - t_start
            rate = elapsed / (i + 1)
            remaining = rate * (total_to_process - i - 1)
            mins, secs = divmod(int(remaining), 60)
            log.info(
                f"  [{i+1}/{total_to_process}] "
                f"signals={signals} filtered={filtered} | "
                f"~{mins}m{secs:02d}s remaining | "
                f"last: {row.get('awardee_name', '')[:35]} -> {fr}"
            )

    _write_results(all_results, output_file)
    _write_detailed_results(all_results, RESULTS_DETAILED_FILE)

    traded = [r for r in all_results if r.get("entry_price")]
    stats = _compute_stats(traded, tp, sl)
    _print_report(stats, all_results, traded, start_date, end_date)

    # Build funnel breakdown and save to JSON for GUI
    breakdown = _build_funnel_breakdown(all_results, training_csv=csv_path)
    breakdown_file = os.path.join(os.path.dirname(__file__), "backtest_breakdown_2023.json")
    with open(breakdown_file, "w") as f:
        _json.dump(breakdown, f)

    return stats, breakdown, all_results


def _process_training_row(row, tp, sl, hold, threshold):
    """Process one row from the training CSV through filter -> score -> simulate."""
    sole_source = row.get("sole_source", "")
    if isinstance(sole_source, str):
        sole_source = sole_source.strip().lower() in ("true", "1", "yes")

    base = {
        "award_date": row.get("posted_date", "")[:10],
        "awardee_name": row.get("awardee_name", ""),
        "agency": row.get("agency", ""),
        "award_amount": float(row.get("award_amount", 0)),
        "naics": row.get("naics", ""),
        "sole_source": sole_source,
    }

    # Filter using historical data
    passed, reason, extra = apply_filters_bt_from_training(row)
    base["filter_result"] = "pass" if passed else "fail"
    base["filter_reason"] = reason
    base["first_8k_date"] = extra.get("first_8k_date", "")
    base["first_pr_date"] = extra.get("first_pr_date", "")
    base["last_dilutive_filing_date"] = extra.get("last_dilutive_filing_date", "")
    base["dilutive_filing_type"] = extra.get("dilutive_filing_type", "")
    base["agency_prior_win_count"] = extra.get("agency_prior_win_count", 0)
    base["ticker_confidence"] = row.get("ticker_confidence", "")

    if not passed:
        return base

    # Score using historical market cap and actual press release signal
    market_cap = extra.get("market_cap", 0)
    has_pr = extra.get("has_press_release", False)

    # Parse agency_prior_win_count from training CSV (0 = first win)
    prior_wins = extra.get("agency_prior_win_count", 0)
    is_first_agency = (prior_wins == 0)

    # Build contract dict for scoring engine
    contract_for_scoring = {
        "awardee_name": row.get("awardee_name", ""),
        "award_amount": float(row.get("award_amount", 0)),
        "sole_source": sole_source,
        "agency": row.get("agency", ""),
        "naics": row.get("naics", ""),
    }

    score, breakdown = score_contract(
        contract_for_scoring, market_cap,
        threshold=threshold, has_press_release=has_pr,
        is_first_agency_win=is_first_agency
    )
    base["score"] = score
    base["market_cap"] = round(market_cap)
    base["value_to_mcap_pct"] = breakdown.get("value_to_mcap", {}).get("ratio", 0)

    log.info(f"Score {score}/100 | mcap=${market_cap:,.0f} | {row.get('awardee_name', 'N/A')[:40]} | Pass={score >= threshold}")

    if score < threshold:
        base["filter_result"] = "low_score"
        base["filter_reason"] = f"Score {score} < threshold {threshold}"
        return base

    # Ticker from training data (already resolved)
    ticker = row.get("ticker", "")
    base["ticker"] = ticker
    if not ticker:
        base["filter_result"] = "no_ticker"
        base["filter_reason"] = "No ticker in training data"
        log.info(f"Filter TICKER | {row.get('awardee_name', 'N/A')[:40]}")
        return base

    # Simulate price action using stored OHLC (no API call)
    sim = simulate_trade_from_row(row, tp, sl, hold)
    if sim:
        base.update({k: sim[k] for k in sim if k != "ticker"})
        # Add 7-day return from original row
        base["return_t7"] = row.get("return_t7", "")
        return_7d = row.get('return_t7', 'N/A')
        return_7d_display = f"{float(return_7d):.2f}%" if return_7d and return_7d not in ('', 'None') else 'N/A'
        log.info(f"TRADE SIGNAL | {ticker} | {row.get('awardee_name', 'N/A')[:40]} | PnL: {sim.get('pnl_pct')}% | 7d: {return_7d_display}")
    else:
        base["filter_result"] = "no_price_data"
        base["filter_reason"] = "No stored OHLC data — run enrich_ohlc.py first"
        log.info(f"Filter PRICE | {ticker} | {row.get('awardee_name', 'N/A')[:40]}")

    return base


def _compute_stats(traded, tp, sl):
    """Compute performance statistics."""
    if not traded:
        return {"trades": 0}

    pnls = [float(t["pnl_pct"]) for t in traded if t.get("pnl_pct") != ""]
    peak_pnls = [float(t["peak_pnl_pct"]) for t in traded if t.get("peak_pnl_pct") not in ("", None, "None")]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    tp_hits = sum(1 for t in traded if t.get("hit_tp") in [True, "True"])
    sl_hits = sum(1 for t in traded if t.get("hit_sl") in [True, "True"])
    timeouts = sum(1 for t in traded if t.get("timed_out") in [True, "True"])

    avg_pnl = sum(pnls) / len(pnls) if pnls else 0
    win_rate = len(wins) / len(pnls) * 100 if pnls else 0

    import math
    if len(pnls) > 1:
        mean = avg_pnl
        variance = sum((p - mean) ** 2 for p in pnls) / len(pnls)
        std = math.sqrt(variance)
        sharpe = (mean / std * math.sqrt(252 / len(pnls))) if std > 0 else 0
    else:
        sharpe = 0
        std = 0

    # Max drawdown (simple, on pnl_pct stream)
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

    return {
        "trades": len(pnls),
        "win_rate": round(win_rate, 1),
        "avg_pnl_pct": round(avg_pnl, 3),
        "total_pnl_pct": round(sum(pnls), 2),
        "std_pnl": round(std, 3),
        "sharpe": round(sharpe, 3),
        "max_drawdown_pct": round(max_dd, 2),
        "tp_hits": tp_hits,
        "sl_hits": sl_hits,
        "timeouts": timeouts,
        "best_trade": round(max(pnls), 2) if pnls else 0,
        "worst_trade": round(min(pnls), 2) if pnls else 0,
        "avg_peak_pnl_pct": round(sum(peak_pnls) / len(peak_pnls), 3) if peak_pnls else 0,
        "best_peak_pnl_pct": round(max(peak_pnls), 2) if peak_pnls else 0,
        "tp_pct": tp * 100,
        "sl_pct": sl * 100,
    }


def _print_report(stats, all_results, traded, start_date, end_date):
    """Print a formatted summary report."""
    total = len(all_results)
    passed_filter = sum(1 for r in all_results if r.get("filter_result") == "pass")
    scored_out = sum(1 for r in all_results if r.get("filter_result") == "low_score")
    no_ticker = sum(1 for r in all_results if r.get("filter_result") == "no_ticker")
    no_price = sum(1 for r in all_results if r.get("filter_result") == "no_price_data")
    duplicates = sum(1 for r in all_results if r.get("filter_result") == "duplicate")

    print("\n" + "=" * 60)
    print(f"  BACKTEST RESULTS  {start_date} -> {end_date}")
    print("=" * 60)
    print(f"  Total awards processed : {total:,}")
    print(f"  Passed all filters     : {passed_filter:,}")
    print(f"  Filtered out           : {total - passed_filter:,}")
    print(f"    +--- Low score         : {scored_out:,}")
    print(f"    +--- No ticker         : {no_ticker:,}")
    print(f"    +--- No price data     : {no_price:,}")
    print(f"    +--- Duplicate ticker  : {duplicates:,}")
    print(f"  Trades simulated       : {stats.get('trades', 0):,}")
    print("-" * 60)
    if stats.get("trades", 0) > 0:
        print(f"  Win Rate               : {stats['win_rate']}%")
        print(f"  Avg P&L per trade      : {stats['avg_pnl_pct']:+.2f}%")
        print(f"  Total P&L (sum)        : {stats['total_pnl_pct']:+.2f}%")
        print(f"  Sharpe Ratio           : {stats['sharpe']:.3f}")
        print(f"  Max Drawdown           : -{stats['max_drawdown_pct']:.2f}%")
        print(f"  Best / Worst trade     : {stats['best_trade']:+.2f}% / {stats['worst_trade']:+.2f}%")
        print(f"  TP hits / SL hits / TO : {stats['tp_hits']} / {stats['sl_hits']} / {stats['timeouts']}")
        print(f"  Avg Peak Return (MFE)  : {stats['avg_peak_pnl_pct']:+.2f}%")
        print(f"  Best Peak Return       : {stats['best_peak_pnl_pct']:+.2f}%")

        # Print individual trade details
        print("\n" + "=" * 100)
        print(f"  INDIVIDUAL TRADES (7-day returns)")
        print("=" * 100)
        traded = [r for r in all_results if r.get("filter_result") == "pass"]
        for i, t in enumerate(traded, 1):
            ticker = t.get("ticker", "?")
            awardee = t.get("awardee_name", "?")[:40]
            entry_price = t.get("entry_price", 0)
            exit_price = t.get("exit_price", 0)
            pnl = t.get("pnl_pct", 0)

            # Calculate 7-day return from stored data or use return_t7
            return_7d_str = t.get("return_t7", "")
            if return_7d_str and return_7d_str not in ("", "None"):
                try:
                    return_7d = f"{float(return_7d_str):+.2f}%"
                except:
                    return_7d = "N/A"
            else:
                return_7d = "N/A"

            exit_reason = t.get("exit_reason", "?")
            entry_date = t.get("entry_date", "?")

            # Peak return (MFE) — best possible exit
            peak_pnl = t.get("peak_pnl_pct", "")
            if peak_pnl not in ("", None, "None"):
                try:
                    peak_str = f"{float(peak_pnl):+.2f}%"
                except:
                    peak_str = "N/A"
            else:
                peak_str = "N/A"

            print(f"  [{i:2}] {ticker:8} | {awardee:40} | Entry: ${entry_price:>7.2f} | Exit: ${exit_price:>7.2f} | PnL: {pnl:+7.2f}% | Peak: {peak_str:>8} | 7d: {return_7d:>8} | {exit_reason:12} | {entry_date}")
        print("=" * 100)
    print(f"  Full results -> backtest_results.csv\n")


def _write_results(results, filepath):
    """Write results to CSV."""
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULTS_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
    log.info(f"Results written to {filepath}")


def _write_detailed_results(results, filepath):
    """Write detailed trade results to CSV (for detailed analysis)."""
    if not results:
        return
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULTS_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
    log.info(f"Detailed results written to {filepath}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAMgovArby Backtester")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--max-records", type=int, default=2000)
    parser.add_argument("--no-cache", action="store_true", help="Skip cache, fetch fresh")
    parser.add_argument("--tp", type=float, default=TAKE_PROFIT_PCT, help="Take profit fraction (e.g. 0.15)")
    parser.add_argument("--sl", type=float, default=STOP_LOSS_PCT, help="Stop loss fraction (e.g. 0.07)")
    parser.add_argument("--hold", type=int, default=MAX_HOLD_DAYS, help="Max hold days")
    parser.add_argument("--threshold", type=int, default=SCORE_THRESHOLD, help="Score threshold")
    parser.add_argument("--watchlist", action="store_true", help="Use watchlist mode (known small-cap defense stocks)")
    parser.add_argument("--dataset", type=str, default=None, help="Path to pre-built dataset JSON from bulk_builder.py")
    parser.add_argument("--training-csv", type=str, default=None,
                        help="Path to training CSV from build_training_set.py (uses historical 8-K, PR, market cap)")
    parser.add_argument("--max-market-cap", type=int, default=None,
                        help="Override MAX_MARKET_CAP for this run (in dollars)")
    add_verbosity_flags(parser)
    args = parser.parse_args()

    # Initialize logger with user's verbosity preference
    log = setup_logging("backtest", quiet=args.quiet, verbose=args.verbose, json_format=args.json)

    # Extract year from start_date and use year-specific output filename
    year = args.start.split("-")[0]
    year_specific_output = os.path.join(os.path.dirname(__file__), f"backtest_results_{year}.csv")

    run_backtest(
        start_date=args.start,
        end_date=args.end,
        max_records=args.max_records,
        tp=args.tp,
        sl=args.sl,
        hold=args.hold,
        threshold=args.threshold,
        use_cache=not args.no_cache,
        watchlist_mode=args.watchlist,
        dataset_file=args.dataset,
        training_csv=args.training_csv,
        output_file=year_specific_output,
        max_market_cap=args.max_market_cap,
    )
