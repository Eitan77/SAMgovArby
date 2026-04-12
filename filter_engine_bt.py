"""Backtest filter engine. Supports both live-lookup and training-CSV modes."""
from __future__ import annotations
import logging
from datetime import datetime
from config import (MIN_CONTRACT_VALUE, MAX_MARKET_CAP,
                    MIN_VALUE_TO_MCAP_PCT, MAX_VALUE_TO_MCAP_PCT,
                    MAX_8K_WINDOW_DAYS, MAX_DILUTIVE_WINDOW_DAYS,
                    MAX_PR_WINDOW_DAYS, MIN_TICKER_CONFIDENCE)

log = logging.getLogger(__name__)

# Confidence levels ordered from lowest to highest
_CONFIDENCE_LEVELS = ["none", "low", "medium", "medium_high", "high"]


def _confidence_meets_minimum(confidence: str, minimum: str) -> bool:
    """Check if a confidence level meets the minimum threshold."""
    try:
        return _CONFIDENCE_LEVELS.index(confidence) >= _CONFIDENCE_LEVELS.index(minimum)
    except ValueError:
        return False


def _days_signed(earlier_str, later_str) -> int | None:
    """Return (later - earlier).days as a signed int.

    Positive → later_str is after earlier_str.
    Negative → later_str is before earlier_str.
    None     → either date is missing, None, or unparseable.
    """
    # Normalise: CSV DictReader may give "" or None; str(None) == "None" guard
    if not earlier_str or not later_str:
        return None
    earlier_str = str(earlier_str).strip()
    later_str = str(later_str).strip()
    if earlier_str in ("None", "nan", "") or later_str in ("None", "nan", ""):
        return None
    try:
        d1 = datetime.strptime(earlier_str[:10], "%Y-%m-%d")
        d2 = datetime.strptime(later_str[:10], "%Y-%m-%d")
        return (d2 - d1).days
    except (ValueError, TypeError):
        return None


def apply_filters_bt_from_training(row):
    """Filter using pre-computed historical data from the training CSV.

    Uses date-based columns for tunable window filtering.
    Returns (passed: bool, reason: str, extra: dict).
    """
    extra = {}

    # Filter 1: Minimum contract value
    award_amount = float(row.get("award_amount", 0))
    if award_amount < MIN_CONTRACT_VALUE:
        return False, f"Contract ${award_amount:,.0f} below minimum", extra

    # Filter 2: Ticker confidence check
    confidence = row.get("ticker_confidence", "none")
    if confidence in (None, "", "None"):
        confidence = "none"
    if not _confidence_meets_minimum(confidence, MIN_TICKER_CONFIDENCE):
        return False, f"Ticker confidence '{confidence}' below minimum '{MIN_TICKER_CONFIDENCE}'", extra

    # Filter 3: Historical market cap check
    hist_mcap = row.get("historical_market_cap_approx")
    if hist_mcap in (None, "", "None"):
        hist_mcap = 0
    else:
        hist_mcap = float(hist_mcap)

    extra["market_cap"] = hist_mcap
    extra["ticker"] = row.get("ticker", "")

    if hist_mcap <= 0:
        return False, "No historical market cap data", extra

    if hist_mcap > MAX_MARKET_CAP:
        return False, f"Historical market cap ${hist_mcap:,.0f} exceeds ${MAX_MARKET_CAP:,.0f} limit", extra

    # Filter 3b: Value-to-market-cap ratio — contract must be material but not a distress signal
    award_amount = float(row.get("award_amount", 0))
    value_to_mcap = award_amount / hist_mcap
    if value_to_mcap < MIN_VALUE_TO_MCAP_PCT:
        return False, f"Contract {value_to_mcap*100:.2f}% of mcap below {MIN_VALUE_TO_MCAP_PCT*100:.0f}% minimum (too immaterial)", extra
    if value_to_mcap > MAX_VALUE_TO_MCAP_PCT:
        return False, f"Contract {value_to_mcap*100:.2f}% of mcap above {MAX_VALUE_TO_MCAP_PCT*100:.0f}% maximum (distress signal)", extra

    # Filter 4: 8-K check — reject if 8-K filed ON OR AFTER award within window
    award_date = row.get("posted_date", "")
    first_8k = row.get("first_8k_date", "")
    days_to_8k = _days_signed(award_date, first_8k)  # positive = 8-K after award
    extra["first_8k_date"] = first_8k
    if days_to_8k is not None and 0 <= days_to_8k <= MAX_8K_WINDOW_DAYS:
        return False, f"8-K filed {days_to_8k}d after award (within {MAX_8K_WINDOW_DAYS}d window)", extra

    # Filter 5: Dilutive offering — reject if dilutive filing is BEFORE award within window
    last_dilutive = row.get("last_dilutive_filing_date", "")
    days_since_dilutive = _days_signed(last_dilutive, award_date)  # positive = dilutive before award
    extra["last_dilutive_filing_date"] = last_dilutive
    extra["dilutive_filing_type"] = row.get("dilutive_filing_type", "")
    if days_since_dilutive is not None and 0 <= days_since_dilutive <= MAX_DILUTIVE_WINDOW_DAYS:
        return False, f"Dilutive offering {days_since_dilutive}d before award (within {MAX_DILUTIVE_WINDOW_DAYS}d window)", extra

    # Pass press release date through for scoring (not a hard filter here)
    first_pr = row.get("first_pr_date", "")
    has_pr_value = row.get("has_pr", "")
    days_to_pr = _days_signed(award_date, first_pr)  # positive = PR after award
    extra["first_pr_date"] = first_pr

    # Normalise has_pr_value: CSV may contain None, "", "None", or "unknown"
    has_pr_str = str(has_pr_value).strip() if has_pr_value is not None else ""
    if has_pr_str in ("unknown", "None", "nan", ""):
        # Data was never collected — let scoring handle conservatively (0 pts)
        extra["has_press_release"] = None
    elif days_to_pr is not None and 0 <= days_to_pr <= MAX_PR_WINDOW_DAYS:
        # PR confirmed within the window → information already public
        extra["has_press_release"] = True
    elif days_to_pr is None:
        # PR date is missing even though has_pr says something — treat as unknown
        extra["has_press_release"] = None
    else:
        extra["has_press_release"] = False

    # Pass agency win count through for scoring (safe int conversion)
    raw_wins = row.get("agency_prior_win_count", 0)
    try:
        extra["agency_prior_win_count"] = int(float(raw_wins)) if raw_wins not in (None, "", "None") else 0
    except (ValueError, TypeError):
        extra["agency_prior_win_count"] = 0

    return True, "Passed filters (training-data mode)", extra


