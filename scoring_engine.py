"""Step 3: Score filtered contracts on a 0-100 scale."""
import logging
from config import HOT_SECTOR_NAICS, GENERAL_DEFENSE_NAICS_PREFIX, SCORE_THRESHOLD, SCORE_WEIGHTS

log = logging.getLogger(__name__)


def score_contract(contract, market_cap, agency_history=None, threshold=None,
                    has_press_release=None, is_first_agency_win=None):
    """Score a contract. Returns (total_score: int, breakdown: dict).

    Args:
        market_cap: Historical market cap at award time (float, dollars).
        agency_history: Set of agency names this company has won from before.
            Used only if is_first_agency_win is not provided.
        is_first_agency_win: Explicit bool from training data. Takes precedence
            over agency_history. None means unknown (live mode without history).
        has_press_release: True/False/None. None = unknown (conservative: 0 pts).
    """
    breakdown = {}
    w = SCORE_WEIGHTS

    # Factor 1: Contract value as % of market cap (max w["value_to_mcap"] pts)
    award_amount = float(contract["award_amount"])  # cast: may be str from CSV
    market_cap = float(market_cap)
    if market_cap > 0:
        ratio = award_amount / market_cap
        max_pts = w["value_to_mcap"]
        if ratio >= 0.10:
            pts = max_pts
        elif ratio >= 0.05:
            pts = int(max_pts * 0.67)   # 20 of 30
        elif ratio >= 0.02:
            pts = int(max_pts * 0.40)   # 12 of 30
        elif ratio >= 0.01:
            pts = int(max_pts * 0.20)   # 6 of 30
        else:
            pts = int(max_pts * 0.07)   # 2 of 30
    else:
        pts = 0
        ratio = 0
    breakdown["value_to_mcap"] = {"points": pts, "max": w["value_to_mcap"], "ratio": round(ratio * 100, 2)}

    # Factor 2: Sole-source (max w["sole_source"] pts)
    pts = w["sole_source"] if contract.get("sole_source") else 0
    breakdown["sole_source"] = {"points": pts, "max": w["sole_source"]}

    # Factor 3: First-time agency win (max w["first_agency"] pts)
    # Priority: explicit bool > set-based lookup > unknown (0 pts, not partial)
    # NOTE: live mode provides neither — scores 0 to be conservative. Use
    # training CSV mode (is_first_agency_win from history) for accurate backtests.
    agency = contract.get("agency", "")
    if is_first_agency_win is not None:
        pts = w["first_agency"] if is_first_agency_win else 0
    elif agency_history is not None:
        pts = w["first_agency"] if agency not in agency_history else 0
    else:
        pts = 0  # unknown in live mode — conservative
    breakdown["first_time_agency"] = {"points": pts, "max": w["first_agency"]}

    # Factor 4: Hot sector (max w["hot_sector"] pts)
    naics = contract.get("naics", "")
    if naics in HOT_SECTOR_NAICS:
        pts = w["hot_sector"]
    elif naics.startswith(GENERAL_DEFENSE_NAICS_PREFIX):
        pts = int(w["hot_sector"] * 0.53)  # 8 of 15
    else:
        pts = 0
    breakdown["hot_sector"] = {"points": pts, "max": w["hot_sector"], "naics": naics}

    # Factor 5: No simultaneous press release (max w["no_pr"] pts)
    if has_press_release is True:
        pts = 0   # confirmed PR exists — no information edge
    elif has_press_release is False:
        pts = w["no_pr"]  # confirmed no PR — full points
    else:
        pts = 0   # unknown/None — conservative: don't award points for missing data
    breakdown["no_press_release"] = {"points": pts, "max": w["no_pr"]}

    total = sum(f["points"] for f in breakdown.values())
    effective_threshold = threshold if threshold is not None else SCORE_THRESHOLD
    breakdown["total"] = total
    breakdown["threshold"] = effective_threshold
    breakdown["passed"] = total >= effective_threshold

    log.info(f"Score for {contract['awardee_name']}: {total}/100 "
             f"(threshold={effective_threshold} -> {'PASS' if total >= effective_threshold else 'FAIL'})")

    return total, breakdown
