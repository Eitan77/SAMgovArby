"""SEC EDGAR API wrapper for company search and filing checks."""
import logging
import time
import requests
from config import EDGAR_RATE_LIMIT, EDGAR_USER_AGENT

log = logging.getLogger(__name__)

HEADERS = {"User-Agent": EDGAR_USER_AGENT, "Accept": "application/json"}
COMPANY_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index/company-search"
FULL_TEXT_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index/company-search"
EDGAR_COMPANY_TICKERS = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"

_last_request = 0


def _rate_limit():
    global _last_request
    elapsed = time.time() - _last_request
    if elapsed < EDGAR_RATE_LIMIT:
        time.sleep(EDGAR_RATE_LIMIT - elapsed)
    _last_request = time.time()


def search_company(name):
    """Search EDGAR for companies matching a name. Returns list of {cik, name, ticker}."""
    _rate_limit()
    # Use the EDGAR full-text search endpoint
    url = "https://efts.sec.gov/LATEST/search-index/company-search"
    params = {"q": name, "dateRange": "custom", "startdt": "2020-01-01"}

    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            # Fallback: try company tickers JSON
            return _search_company_tickers(name)
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        results = []
        for hit in hits[:10]:
            src = hit.get("_source", {})
            results.append({
                "cik": str(src.get("entity_id", "")),
                "name": src.get("entity", ""),
                "ticker": src.get("tickers", [""])[0] if src.get("tickers") else "",
            })
        return results
    except Exception as e:
        log.warning(f"EDGAR company search failed: {e}, trying fallback")
        return _search_company_tickers(name)


def _search_company_tickers(name):
    """Fallback: search the static company_tickers.json file."""
    _rate_limit()
    try:
        resp = requests.get(EDGAR_COMPANY_TICKERS, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        name_lower = name.lower()
        results = []
        for entry in data.values():
            company_name = entry.get("title", "").lower()
            if name_lower in company_name or company_name in name_lower:
                results.append({
                    "cik": str(entry.get("cik_str", "")),
                    "name": entry.get("title", ""),
                    "ticker": entry.get("ticker", ""),
                })
        return results[:10]
    except Exception as e:
        log.error(f"EDGAR company tickers fallback failed: {e}")
        return []


def get_recent_filings(cik, form_types=None, days_back=30):
    """Get recent filings for a CIK. Optionally filter by form type (e.g., '8-K', 'S-3')."""
    _rate_limit()
    padded_cik = cik.zfill(10)
    url = SUBMISSIONS_URL.format(cik=padded_cik)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        descriptions = recent.get("primaryDocDescription", [])

        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        filings = []
        for i, (form, date) in enumerate(zip(forms, dates)):
            if date < cutoff:
                continue
            if form_types and form not in form_types:
                continue
            filings.append({
                "form": form,
                "date": date,
                "description": descriptions[i] if i < len(descriptions) else "",
            })

        return filings
    except Exception as e:
        log.error(f"Failed to get filings for CIK {cik}: {e}")
        return []


def has_recent_8k(cik, days_back=7):
    """Check if company filed an 8-K in the last N days."""
    filings = get_recent_filings(cik, form_types=["8-K"], days_back=days_back)
    return len(filings) > 0


def has_dilutive_offering(cik, days_back=60):
    """Check if company has a recent S-1 or S-3 filing (equity offering)."""
    filings = get_recent_filings(cik, form_types=["S-1", "S-3", "S-1/A", "S-3/A"], days_back=days_back)
    return len(filings) > 0
