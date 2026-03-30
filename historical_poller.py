"""Fetch historical SAM.gov award notices for a date range."""
import logging
import time
import requests
from datetime import datetime, timedelta
from config import SAM_API_KEY
from sam_poller import _parse_award

log = logging.getLogger(__name__)
SAM_API_URL = "https://api.sam.gov/opportunities/v2/search"

MAX_RETRIES = 5
BASE_WAIT = 10  # seconds


def fetch_awards_range(start_date: str, end_date: str, max_records=1000):
    """Fetch Award Notices between start_date and end_date (MM/DD/YYYY or YYYY-MM-DD).

    Returns list of parsed contract dicts.
    """
    # Normalize to MM/DD/YYYY
    start = _normalize_date(start_date)
    end = _normalize_date(end_date)

    params = {
        "api_key": SAM_API_KEY,
        "postedFrom": start,
        "postedTo": end,
        "ptype": "a",
        "limit": 100,
        "offset": 0,
    }

    all_awards = []
    while len(all_awards) < max_records:
        data = _fetch_with_retry(params)
        if data is None:
            break

        opportunities = data.get("opportunitiesData", [])
        if not opportunities:
            break

        for opp in opportunities:
            award = _parse_award(opp)
            if award:
                all_awards.append(award)

        log.info(f"  Fetched {len(all_awards)} awards so far (offset={params['offset']})")

        if len(opportunities) < params["limit"]:
            break

        params["offset"] += params["limit"]
        time.sleep(1)  # polite pause between pages

    log.info(f"Total awards fetched ({start} -> {end}): {len(all_awards)}")
    return all_awards


def _fetch_with_retry(params):
    """Fetch from SAM.gov with exponential backoff on 429 errors."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(SAM_API_URL, params=params, timeout=30)
            if resp.status_code == 429:
                wait = BASE_WAIT * (2 ** attempt)
                log.warning(f"Rate limited (429). Waiting {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if "429" in str(e):
                wait = BASE_WAIT * (2 ** attempt)
                log.warning(f"Rate limited. Waiting {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            log.error(f"SAM.gov fetch error at offset {params['offset']}: {e}")
            return None
        except Exception as e:
            log.error(f"SAM.gov fetch error at offset {params['offset']}: {e}")
            return None

    log.error(f"SAM.gov fetch failed after {MAX_RETRIES} retries")
    return None


def _normalize_date(d: str) -> str:
    """Convert YYYY-MM-DD to MM/DD/YYYY if needed."""
    if "-" in d and d.index("-") == 4:
        dt = datetime.strptime(d, "%Y-%m-%d")
        return dt.strftime("%m/%d/%Y")
    return d


def date_range_chunks(start_date: str, end_date: str, chunk_days=14):
    """Yield (start, end) pairs in 14-day chunks to stay under rate limits."""
    start = datetime.strptime(_normalize_date(start_date), "%m/%d/%Y")
    end = datetime.strptime(_normalize_date(end_date), "%m/%d/%Y")

    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        yield current.strftime("%m/%d/%Y"), chunk_end.strftime("%m/%d/%Y")
        current = chunk_end + timedelta(days=1)
