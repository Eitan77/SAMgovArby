"""Fetch USASpending contract awards for each company in the watchlist.

Much more targeted than pulling random awards — we know these are public companies.
"""
import logging
import time
import requests
import yfinance as yf
from watchlist import WATCHLIST

log = logging.getLogger(__name__)

BASE_URL = "https://api.usaspending.gov/api/v2"
HEADERS = {"Content-Type": "application/json"}


def fetch_awards_for_watchlist(start_date: str, end_date: str, max_per_company: int = 50):
    """Fetch all contract awards for every company in the watchlist.

    Returns list of contract dicts (same schema as usaspending_poller).
    """
    all_awards = []

    for ticker, company_name, aliases in WATCHLIST:
        # Get current market cap to check if it was small cap back then
        # (approximation — we use current market cap as a proxy)
        market_cap = _get_market_cap(ticker)

        # Fetch awards for this company name + aliases
        search_names = [company_name] + aliases
        company_awards = []

        for name in search_names:
            awards = _fetch_for_company(name, start_date, end_date, max_per_company)
            for a in awards:
                # Tag with ticker and market cap
                a["ticker"] = ticker
                a["market_cap"] = market_cap
                company_awards.append(a)

        # Deduplicate by award ID
        seen = set()
        for a in company_awards:
            aid = a.get("solicitation_number", "")
            if aid and aid not in seen:
                seen.add(aid)
                all_awards.append(a)
            elif not aid:
                all_awards.append(a)

        if company_awards:
            log.info(f"  {ticker} ({company_name}): {len(set(a.get('solicitation_number','') for a in company_awards))} awards")

        time.sleep(0.3)  # polite pause

    log.info(f"Total watchlist awards: {len(all_awards)}")
    return all_awards


def _fetch_for_company(company_name: str, start_date: str, end_date: str, limit: int):
    """Fetch awards from USASpending for a specific recipient name."""
    payload = {
        "filters": {
            "time_period": [{"start_date": start_date, "end_date": end_date}],
            "award_type_codes": ["A", "B", "C", "D"],
            "recipient_search_text": [company_name],
        },
        "fields": [
            "Award ID", "Recipient Name", "Award Amount",
            "Start Date", "End Date",
            "Awarding Agency Name", "Awarding Sub Agency Name",
            "NAICS Code", "NAICS Description",
            "Contract Award Type", "Type of Set Aside",
            "Period of Performance Start Date",
        ],
        "page": 1,
        "limit": min(limit, 100),
        "sort": "Award Amount",
        "order": "desc",
    }

    try:
        resp = requests.post(
            f"{BASE_URL}/search/spending_by_award/",
            json=payload,
            headers=HEADERS,
            timeout=30,
        )
        if resp.status_code == 429:
            log.warning("Rate limited, waiting 15s")
            time.sleep(15)
            return []
        if resp.status_code != 200:
            log.warning(f"USASpending error {resp.status_code} for '{company_name}'")
            return []

        results = resp.json().get("results", [])
        return [_parse(r) for r in results if _parse(r)]

    except Exception as e:
        log.warning(f"Fetch error for '{company_name}': {e}")
        return []


def _parse(r):
    """Map USASpending result to internal contract schema."""
    try:
        name = (r.get("Recipient Name") or "").strip()
        amount = r.get("Award Amount") or 0
        if float(amount) <= 0:
            return None

        set_aside = (r.get("Type of Set Aside") or "").lower()
        contract_type = (r.get("Contract Award Type") or "").lower()
        description = f"{set_aside} {contract_type}"

        posted = r.get("Start Date") or r.get("Period of Performance Start Date") or ""

        sole_source_indicators = ["sole source", "sole-source", "only one source",
                                   "other than full", "8(a) sole"]
        idiq_indicators = ["idiq", "indefinite delivery", "indefinite quantity"]

        return {
            "title": r.get("NAICS Description") or "",
            "solicitation_number": r.get("Award ID") or "",
            "posted_date": posted,
            "awardee_name": name,
            "awardee_name_raw": name,
            "awardee_duns": "",
            "award_amount": float(amount),
            "agency": r.get("Awarding Agency Name") or "",
            "office": r.get("Awarding Sub Agency Name") or "",
            "naics": str(r.get("NAICS Code") or ""),
            "set_aside": r.get("Type of Set Aside") or "",
            "sole_source": any(i in description for i in sole_source_indicators),
            "is_idiq": any(i in description for i in idiq_indicators),
            "description": description,
            "sam_url": "",
        }
    except Exception as e:
        log.warning(f"Parse error: {e}")
        return None


def _get_market_cap(ticker: str) -> float:
    """Get current market cap from yfinance."""
    try:
        info = yf.Ticker(ticker).info
        return float(info.get("marketCap") or 0)
    except Exception:
        return 0.0
