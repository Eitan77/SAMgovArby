"""Fetch historical contract awards from USASpending.gov API.

No API key required. Much higher rate limits than SAM.gov.
Designed for bulk historical queries — perfect for backtesting.

Docs: https://api.usaspending.gov/
"""
import logging
import time
import requests
from datetime import datetime

log = logging.getLogger(__name__)

BASE_URL = "https://api.usaspending.gov/api/v2"
HEADERS = {"Content-Type": "application/json"}
PAGE_SIZE = 100


def fetch_awards_range(start_date: str, end_date: str, max_records: int = 5000):
    """Fetch contract awards between start_date and end_date (YYYY-MM-DD).

    Returns list of dicts matching our internal contract schema.
    """
    start = _normalize(start_date)
    end = _normalize(end_date)

    awards = []
    page = 1
    last_id = None
    last_sort_value = None

    while len(awards) < max_records:
        payload = {
            "filters": {
                "time_period": [{"start_date": start, "end_date": end}],
                "award_type_codes": ["A", "B", "C", "D"],
                # Focus on defense/tech NAICS codes where small caps get contracts
                "naics_codes": [
                    "336411", "336412", "336413", "336414", "336415", "336419",
                    "334511", "334512", "334513", "334514", "334515", "334516",
                    "334519", "334220", "334290", "334310", "334411", "334412",
                    "541330", "541511", "541512", "541519", "541611", "541715",
                    "518210", "927110", "928110", "928120",
                ],
                # Only contracts under $500M to focus on small caps
                "award_amounts": [{"lower_bound": 1000000, "upper_bound": 500000000}],
            },
            "fields": [
                "Award ID", "Recipient Name", "Award Amount",
                "Start Date", "End Date",
                "Awarding Agency Name", "Awarding Sub Agency Name",
                "NAICS Code", "NAICS Description",
                "Contract Award Type", "Type of Set Aside",
                "Period of Performance Start Date",
            ],
            "page": page,
            "limit": PAGE_SIZE,
            "sort": "Award Amount",
            "order": "asc",  # ascending = smaller contracts first = more small caps
        }

        # Use cursor-based pagination after first page
        if last_id and last_sort_value:
            payload["last_record_unique_id"] = last_id
            payload["last_record_sort_value"] = last_sort_value

        try:
            resp = requests.post(
                f"{BASE_URL}/search/spending_by_award/",
                json=payload,
                headers=HEADERS,
                timeout=30,
            )
            if resp.status_code == 429:
                log.warning("USASpending rate limited — waiting 30s")
                time.sleep(30)
                continue
            if resp.status_code != 200:
                log.error(f"USASpending error {resp.status_code}: {resp.text[:500]}")
                break
            data = resp.json()
        except Exception as e:
            log.error(f"USASpending fetch error: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        for r in results:
            award = _parse_award(r)
            if award:
                awards.append(award)

        # Pagination info
        page_meta = data.get("page_metadata", {})
        has_next = page_meta.get("hasNext", False)
        last_id = page_meta.get("last_record_unique_id")
        last_sort_value = page_meta.get("last_record_sort_value")

        log.info(f"  USASpending: {len(awards)} awards fetched (page {page})")

        if not has_next:
            break

        page += 1
        time.sleep(0.5)

    log.info(f"Total awards fetched ({start} -> {end}): {len(awards)}")
    return awards


def _parse_award(r):
    """Map USASpending result to our internal contract schema."""
    try:
        name = (r.get("Recipient Name") or "").strip()
        amount = r.get("Award Amount") or 0

        if not name or float(amount) <= 0:
            return None

        set_aside = (r.get("Type of Set Aside") or "").lower()
        contract_type = (r.get("Contract Award Type") or "").lower()

        sole_source_indicators = [
            "sole source", "sole-source", "only one source",
            "other than full", "8(a) sole"
        ]
        idiq_indicators = ["idiq", "indefinite delivery", "indefinite quantity"]

        description = f"{set_aside} {contract_type}"

        # Use Start Date or Period of Performance Start Date
        posted = r.get("Start Date") or r.get("Period of Performance Start Date") or ""

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
        log.warning(f"Failed to parse USASpending award: {e}")
        return None


def _normalize(d: str) -> str:
    """Ensure date is YYYY-MM-DD."""
    if "/" in d:
        dt = datetime.strptime(d, "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    return d[:10]
