"""Patch an existing training CSV to add OHLC columns (open/high/low/close per day T0-T7).

Run this once against your existing training CSV. It adds:
    open_t0..open_t7, high_t0..high_t7, low_t0..low_t7, close_t0..close_t7

Rows that already have open_t0 populated are skipped (resumable).

Usage:
    python enrich_ohlc.py datasets/training_qualified_2023.csv [--quiet] [--verbose]
    python enrich_ohlc.py datasets/training_qualified_2023.csv --output datasets/training_qualified_2023_ohlc.csv [--quiet]
"""
import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime, timedelta

import polars as pl
import yfinance as yf

from config_logging import setup_logging, add_verbosity_flags

log = logging.getLogger("enrich_ohlc")

N_DAYS = 7  # T0 through T7
LOOKBACK_DAYS = 15  # Fetch ±15 days around award date (instead of full year)


def _fetch_date_range_ohlc(ticker: str, target_date: str):
    """Fetch OHLC for ±15 days around target_date. Much faster than full year!

    Args:
        ticker: Stock ticker symbol
        target_date: Target date as YYYY-MM-DD string

    Returns:
        DataFrame with OHLC data or None on error
    """
    try:
        target = datetime.strptime(target_date[:10], "%Y-%m-%d")
        start_date = (target - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        end_date = (target + timedelta(days=LOOKBACK_DAYS + N_DAYS)).strftime("%Y-%m-%d")

        hist = yf.Ticker(ticker).history(
            start=start_date,
            end=end_date,
            auto_adjust=True,
            timeout=30,
        )
        if hasattr(hist.index, "tz") and hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        if hist.empty:
            log.debug(f"  {ticker} on {target_date}: no history")
            return None
        return hist
    except Exception as e:
        log.debug(f"  {ticker} on {target_date}: fetch failed — {e}")
        return None


def _slice_ohlc(hist, date_str: str, n_days: int = N_DAYS) -> dict:
    """Extract T0..T{n_days} OHLC from a full-year DataFrame."""
    if hist is None or hist.empty:
        return {}
    try:
        import pandas as pd
        target = datetime.strptime(date_str[:10], "%Y-%m-%d")
        future = hist[hist.index >= pd.Timestamp(target)]
        if future.empty:
            return {}

        result = {}
        for i, (_, row) in enumerate(future.iloc[:n_days + 1].iterrows()):
            result[f"open_t{i}"]  = round(float(row["Open"]),  4)
            result[f"high_t{i}"]  = round(float(row["High"]),  4)
            result[f"low_t{i}"]   = round(float(row["Low"]),   4)
            result[f"close_t{i}"] = round(float(row["Close"]), 4)
        return result
    except Exception as e:
        log.debug(f"Slice error on {date_str}: {e}")
        return {}


def enrich_csv(input_path: str, output_path: str):
    log.info(f"Loading {input_path}")
    with open(input_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        original_fields = reader.fieldnames or []

    log.info(f"Loaded {len(rows)} rows")

    # Determine new columns
    new_cols = []
    for i in range(N_DAYS + 1):
        new_cols += [f"open_t{i}", f"high_t{i}", f"low_t{i}", f"close_t{i}"]

    # Build full fieldnames: original + new cols (deduplicated, preserving order)
    existing_set = set(original_fields)
    extra_cols = [c for c in new_cols if c not in existing_set]
    all_fields = original_fields + extra_cols

    # Count how many rows already have OHLC (for resume)
    already_done = sum(1 for r in rows if r.get("open_t0", "").strip() not in ("", "None"))
    log.info(f"Rows already enriched (will skip): {already_done}")

    # Cache historical data by (ticker, date_key) to reuse across similar dates
    history_cache = {}  # (ticker, date_key) -> DataFrame
    processed = 0
    t_start = time.time()

    for row_idx, row in enumerate(rows):
        ticker = row.get("ticker", "").strip()
        date_str = row.get("posted_date", "")[:10]

        # Skip if already enriched
        if row.get("open_t0", "").strip() not in ("", "None"):
            continue

        # Skip if missing required fields
        if not ticker or not date_str:
            continue

        # Fetch history with caching (use date's month as cache key to minimize API calls)
        cache_key = (ticker, date_str[:7])  # YYYY-MM as cache key
        if cache_key not in history_cache:
            hist = _fetch_date_range_ohlc(ticker, date_str)
            history_cache[cache_key] = hist
            time.sleep(0.05)  # polite rate limit (20 req/sec)
        else:
            hist = history_cache[cache_key]

        # Extract OHLC for this specific date
        ohlc = _slice_ohlc(hist, date_str) if hist is not None else {}
        for col in new_cols:
            row[col] = ohlc.get(col, "")

        processed += 1
        if processed % 50 == 0 or processed == sum(1 for r in rows if r.get("ticker", "").strip() and r.get("posted_date", "")):
            elapsed = time.time() - t_start
            rate = elapsed / processed if processed > 0 else 0
            log.info(
                f"  [{processed}] {ticker} {date_str} "
                f"| cache_size={len(history_cache)} | rate={rate:.2f}s/row"
            )

            # Checkpoint: write after every 50 rows
            if processed % 50 == 0:
                _write_csv(output_path, all_fields, rows)
                log.debug(f"  Checkpoint saved -> {output_path}")

    # Final write
    _write_csv(output_path, all_fields, rows)
    log.info(f"Done. Enriched {processed} rows, cache_size={len(history_cache)}")
    log.info(f"Output written to {output_path}")


def _write_csv(path: str, fields: list, rows: list):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add OHLC columns to existing training CSV")
    parser.add_argument("input", help="Path to existing training CSV")
    parser.add_argument("--output", default=None,
                        help="Output path (default: overwrites input)")
    add_verbosity_flags(parser)
    args = parser.parse_args()

    # Initialize logger with user's verbosity preference
    log = setup_logging("enrich_ohlc", quiet=args.quiet, verbose=args.verbose, json_format=args.json)

    output = args.output or args.input
    enrich_csv(args.input, output)
