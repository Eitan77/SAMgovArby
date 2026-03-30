"""SAM.gov Contract Award -> Stock Execution Pipeline.

Runs hourly: poll SAM.gov -> filter -> score -> resolve ticker -> execute trade.
"""
import json
import logging
import csv
import os
import sys
import time
from datetime import datetime

import pytz
import schedule

from config import SCORE_THRESHOLD, TZ, POLL_INTERVAL_HOURS
from sam_poller import fetch_recent_awards
from filter_engine import apply_filters
from scoring_engine import score_contract
from ticker_resolver import resolve_ticker
from trade_executor import execute_trade, check_and_exit_expired_positions

# --- Logging setup ---
LOG_DIR = os.path.dirname(__file__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOG_DIR, "pipeline.log")),
    ],
)
log = logging.getLogger("main")

SIGNAL_LOG = os.path.join(LOG_DIR, "signal_log.csv")
PROCESSED_AWARDS_FILE = os.path.join(LOG_DIR, "processed_awards.json")
SIGNAL_FIELDS = [
    "timestamp", "awardee_name", "agency", "award_amount", "naics",
    "sole_source", "filter_result", "filter_reason", "score", "score_breakdown",
    "ticker", "ticker_confidence", "trade_placed",
]


def run_pipeline():
    """Execute one full pipeline cycle."""
    tz = pytz.timezone(TZ)
    now = datetime.now(tz)
    log.info(f"=== Pipeline run at {now.strftime('%Y-%m-%d %H:%M %Z')} ===")

    # Step 0: Check for expired positions to exit
    try:
        check_and_exit_expired_positions()
    except Exception as e:
        log.error(f"Position exit check failed: {e}")

    # Step 1: Fetch
    try:
        awards = fetch_recent_awards(hours_back=POLL_INTERVAL_HOURS)
    except Exception as e:
        log.error(f"SAM.gov fetch failed: {e}")
        return

    if not awards:
        log.info("No new awards found")
        return

    log.info(f"Processing {len(awards)} awards")

    processed_awards = _load_processed_awards()

    for contract in awards:
        key = _award_key(contract)
        if key in processed_awards:
            log.info(f"SKIP (already processed): {contract['awardee_name']}")
            continue
        signal = {
            "timestamp": now.strftime("%Y-%m-%d %H:%M"),
            "awardee_name": contract["awardee_name"],
            "agency": contract["agency"],
            "award_amount": contract["award_amount"],
            "naics": contract["naics"],
            "sole_source": contract["sole_source"],
        }

        # Step 2: Filter
        try:
            passed, reason, extra = apply_filters(contract)
        except Exception as e:
            log.error(f"Filter error for {contract['awardee_name']}: {e}")
            signal.update({"filter_result": "error", "filter_reason": str(e),
                           "score": "", "ticker": "", "trade_placed": False})
            _log_signal(signal)
            continue

        signal["filter_result"] = "pass" if passed else "fail"
        signal["filter_reason"] = reason

        if not passed:
            log.info(f"FILTERED: {contract['awardee_name']} - {reason}")
            signal.update({"score": "", "ticker": "", "trade_placed": False})
            _log_signal(signal)
            continue

        # Step 3: Score
        market_cap = extra.get("market_cap", 0)
        try:
            total_score, breakdown = score_contract(contract, market_cap)
        except Exception as e:
            log.error(f"Scoring error for {contract['awardee_name']}: {e}")
            signal.update({"score": "error", "ticker": "", "trade_placed": False})
            _log_signal(signal)
            continue

        signal["score"] = total_score
        signal["score_breakdown"] = str(breakdown)

        if total_score < SCORE_THRESHOLD:
            log.info(f"LOW SCORE: {contract['awardee_name']} scored {total_score}/{SCORE_THRESHOLD}")
            signal.update({"ticker": "", "trade_placed": False})
            _log_signal(signal)
            continue

        # Step 4: Resolve ticker
        ticker = extra.get("ticker")  # may already have from filter step
        confidence = 1.0

        if not ticker:
            try:
                ticker, confidence = resolve_ticker(
                    contract["awardee_name"],
                    edgar_results=extra.get("edgar_results"),
                )
            except Exception as e:
                log.error(f"Ticker resolution error for {contract['awardee_name']}: {e}")
                signal.update({"ticker": "error", "trade_placed": False})
                _log_signal(signal)
                continue

        signal["ticker"] = ticker or ""
        signal["ticker_confidence"] = confidence

        if not ticker:
            log.warning(f"NO TICKER: Could not resolve {contract['awardee_name']}")
            signal["trade_placed"] = False
            _log_signal(signal)
            continue

        # Step 5: Execute
        log.info(f">>> SIGNAL: {ticker} | Score: {total_score} | "
                 f"${contract['award_amount']:,.0f} | {contract['awardee_name']}")

        try:
            result = execute_trade(ticker, total_score, contract)
            signal["trade_placed"] = result is not None
        except Exception as e:
            log.error(f"Trade execution error for {ticker}: {e}")
            signal["trade_placed"] = False

        processed_awards.add(key)
        _log_signal(signal)

    _save_processed_awards(processed_awards)
    log.info("=== Pipeline run complete ===\n")


def _award_key(contract):
    """Stable dedup key for a contract award."""
    return f"{contract['awardee_name']}|{contract['award_amount']}|{contract.get('posted_date', '')}"


def _load_processed_awards():
    if not os.path.exists(PROCESSED_AWARDS_FILE):
        return set()
    try:
        with open(PROCESSED_AWARDS_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()


def _save_processed_awards(seen: set):
    try:
        with open(PROCESSED_AWARDS_FILE, "w") as f:
            json.dump(sorted(seen), f)
    except Exception as e:
        log.warning(f"Could not save processed awards ledger: {e}")


def _log_signal(signal):
    """Append signal to the signal log CSV."""
    exists = os.path.exists(SIGNAL_LOG)
    with open(SIGNAL_LOG, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SIGNAL_FIELDS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow(signal)


def main():
    """Entry point. Run once immediately, then schedule hourly."""
    log.info("SAMgovArby pipeline starting")
    log.info(f"Schedule: every {POLL_INTERVAL_HOURS} hour(s)")

    # Run immediately on start
    run_pipeline()

    # Schedule hourly runs
    schedule.every(POLL_INTERVAL_HOURS).hours.do(run_pipeline)

    log.info("Scheduler running. Press Ctrl+C to stop.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Pipeline stopped by user")


if __name__ == "__main__":
    main()
