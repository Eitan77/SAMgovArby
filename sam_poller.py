"""Step 1: Fetch new Award Notices from SAM.gov API."""
import logging
import re
import requests
from datetime import datetime, timedelta
import pytz
from config import SAM_API_KEY, TZ

log = logging.getLogger(__name__)

SAM_API_URL = "https://api.sam.gov/opportunities/v2/search"


def fetch_recent_awards(hours_back=1):
    """Fetch Award Notices posted in the last `hours_back` hours."""
    tz = pytz.timezone(TZ)
    now = datetime.now(tz)
    since = now - timedelta(hours=hours_back)

    params = {
        "api_key": SAM_API_KEY,
        "postedFrom": since.strftime("%m/%d/%Y"),
        "postedTo": now.strftime("%m/%d/%Y"),
        "ptype": "a",  # Award Notice
        "limit": 100,
        "offset": 0,
    }

    all_awards = []
    while True:
        resp = requests.get(SAM_API_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        opportunities = data.get("opportunitiesData", [])
        if not opportunities:
            break

        for opp in opportunities:
            award = _parse_award(opp)
            if award:
                all_awards.append(award)

        if len(opportunities) < params["limit"]:
            break
        params["offset"] += params["limit"]

    log.info(f"Fetched {len(all_awards)} award notices from SAM.gov")
    return all_awards


def _parse_award(opp):
    """Extract relevant fields from a SAM.gov opportunity."""
    try:
        # Get award details
        award_data = opp.get("award", {}) or {}
        awardee = award_data.get("awardee", {}) or {}

        # Build the parsed contract dict
        contract = {
            "title": opp.get("title", ""),
            "solicitation_number": opp.get("solicitationNumber", ""),
            "posted_date": opp.get("postedDate", ""),
            "awardee_name": _clean_awardee_name(awardee.get("name", "")),
            "awardee_name_raw": awardee.get("name", ""),
            "awardee_duns": awardee.get("ueiSAM", ""),
            "award_amount": _parse_amount(award_data.get("amount", 0)),
            "agency": opp.get("fullParentPathName", ""),
            "office": opp.get("officeAddress", {}).get("name", "") if opp.get("officeAddress") else "",
            "naics": opp.get("naicsCode", ""),
            "set_aside": opp.get("typeOfSetAside", ""),
            "sole_source": _is_sole_source(opp),
            "is_idiq": _is_idiq(opp),
            "description": opp.get("description", ""),
            "sam_url": f"https://sam.gov/opp/{opp.get('noticeId', '')}/view",
        }

        if not contract["awardee_name"] or contract["award_amount"] <= 0:
            return None

        return contract
    except Exception as e:
        log.warning(f"Failed to parse award: {e}")
        return None


def _clean_awardee_name(raw_name):
    """Strip embedded addresses from SAM.gov awardee names.

    Input:  'DATASOFT TECHNOLOGIES INC 34 PARKWAY COMMONS WAY GREER SC USA 29650-5213'
    Output: 'DATASOFT TECHNOLOGIES INC'
    """
    if not raw_name:
        return ""

    name = raw_name.strip()

    # Pattern: company name followed by a street number (digits starting an address)
    # e.g., "COMPANY NAME 1234 STREET NAME CITY ST ZIP"
    match = re.match(r'^(.+?)\s+\d+\s+[A-Z]', name)
    if match:
        candidate = match.group(1).strip()
        # Only accept if we got at least 2 words (avoid stripping too much)
        if len(candidate.split()) >= 2:
            return candidate

    # Fallback: look for state abbreviation + ZIP pattern
    match = re.match(r'^(.+?)\s+[A-Z]{2}\s+USA\s+\d{5}', name)
    if match:
        # Walk back to find where the address starts
        parts = name.split()
        for i, part in enumerate(parts):
            if re.match(r'^\d+$', part) and i >= 2:
                return " ".join(parts[:i]).strip()

    # Fallback: look for common address words
    address_markers = [' PO BOX ', ' SUITE ', ' STE ', ' STE.', ' BLDG ',
                       ' FLOOR ', ' FL ']
    name_upper = name.upper()
    for marker in address_markers:
        idx = name_upper.find(marker)
        if idx > 10:  # make sure there's a company name before the marker
            return name[:idx].strip()

    # If nothing matched, return as-is (might be a clean name already)
    return name.rstrip()


def _parse_amount(val):
    """Parse dollar amount from various formats."""
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        cleaned = val.replace("$", "").replace(",", "").strip()
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    return 0.0


def _is_sole_source(opp):
    """Check if the contract was sole-source."""
    set_aside = (opp.get("typeOfSetAsideDescription") or "").lower()
    sol_type = (opp.get("solicitationNumber") or "").lower()
    title = (opp.get("title") or "").lower()
    desc = (opp.get("description") or "").lower()

    indicators = ["sole source", "sole-source", "only one responsible source",
                  "justification for other than full and open"]
    text = f"{set_aside} {sol_type} {title} {desc}"
    return any(ind in text for ind in indicators)


def _is_idiq(opp):
    """Check if this is an IDIQ contract."""
    title = (opp.get("title") or "").lower()
    desc = (opp.get("description") or "").lower()
    contract_type = (opp.get("archiveType") or "").lower()

    indicators = ["idiq", "indefinite delivery", "indefinite quantity",
                  "id/iq", "indefinite-delivery", "indefinite-quantity"]
    text = f"{title} {desc} {contract_type}"
    return any(ind in text for ind in indicators)
