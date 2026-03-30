"""Step 3: Score filtered contracts on a 0-100 scale."""
import logging
from config import HOT_SECTOR_NAICS, GENERAL_DEFENSE_NAICS_PREFIX, SCORE_THRESHOLD

log = logging.getLogger(__name__)


def score_contract(contract, market_cap, agency_history=None, threshold=None,
                    has_press_release=None, is_first_agency_win=None):
    """Score a contract. Returns (total_score: int, breakdown: dict).

    Args:
        has_press_release: If provided (True/False), uses the actual historical
            press release signal instead of assuming no PR exists.
    """
    breakdown = {}

    # Factor 1: Contract value as % of market cap (max 30 pts)
    if market_cap > 0:
        ratio = contract["award_amount"] / market_cap
        if ratio >= 0.10:
            pts = 30
        elif ratio >= 0.05:
            pts = 20
        elif ratio >= 0.02:
            pts = 12
        elif ratio >= 0.01:
            pts = 6
        else:
            pts = 2
    else:
        pts = 0
        ratio = 0
    breakdown["value_to_mcap"] = {"points": pts, "max": 30, "ratio": round(ratio * 100, 2)}

    # Factor 2: Sole-source (max 25 pts)
    pts = 25 if contract.get("sole_source") else 0
    breakdown["sole_source"] = {"points": pts, "max": 25}

    # Factor 3: First-time agency win (max 15 pts)
    # agency_history is a set of agencies this company has won from before
    agency = contract.get("agency", "")
    if is_first_agency_win is not None:
        pts = 15 if is_first_agency_win else 0
    elif agency_history is not None:
        pts = 15 if agency not in agency_history else 0
    else:
        pts = 8  # unknown, give partial credit
    breakdown["first_time_agency"] = {"points": pts, "max": 15}

    # Factor 4: Hot sector (max 15 pts)
    naics = contract.get("naics", "")
    if naics in HOT_SECTOR_NAICS:
        pts = 15
    elif naics.startswith(GENERAL_DEFENSE_NAICS_PREFIX):
        pts = 8
    else:
        pts = 0
    breakdown["hot_sector"] = {"points": pts, "max": 15, "naics": naics}

    # Factor 5: No simultaneous press release (max 15 pts)
    if has_press_release is True:
        pts = 0   # confirmed PR exists — no information edge
    elif has_press_release is False:
        pts = 15  # confirmed no PR — full points
    else:
        pts = 0   # unknown/None — conservative: don't award points for missing data
    breakdown["no_press_release"] = {"points": pts, "max": 15}

    total = sum(f["points"] for f in breakdown.values())
    effective_threshold = threshold if threshold is not None else SCORE_THRESHOLD
    breakdown["total"] = total
    breakdown["threshold"] = effective_threshold
    breakdown["passed"] = total >= effective_threshold

    log.info(f"Score for {contract['awardee_name']}: {total}/100 "
             f"(threshold={effective_threshold} -> {'PASS' if total >= effective_threshold else 'FAIL'})")

    return total, breakdown
