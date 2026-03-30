"""Check PRNewswire and Business Wire for existing press releases about a contract."""
import logging
import time
import re
import requests
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime

log = logging.getLogger(__name__)

_last_request = 0.0


def _rate_limit():
    global _last_request
    elapsed = time.time() - _last_request
    if elapsed < 1.0:
        time.sleep(1.0 - elapsed)
    _last_request = time.time()


def has_press_release(company_name, contract_title="", days_back=2):
    """Check if a press release exists for this company + contract."""
    queries = [
        f'"{company_name}" contract award',
        f'"{company_name}" government contract',
    ]
    for query in queries:
        if _search_google_news(query, days_back):
            log.info(f"Found press release for {company_name}")
            return True
    return False


def find_pr_date(company_name: str, contract_date: str, days_window: int = 7) -> str:
    """Find the date of the earliest press release about a contract award.

    Args:
        company_name: Company name to search for.
        contract_date: ISO date string (YYYY-MM-DD) of the contract award.
        days_window: Search window in days around the contract date.

    Returns:
        ISO date string of the first PR found, or "" if none found.
    """
    queries = [
        f'"{company_name}" contract award',
        f'"{company_name}" government contract',
    ]
    for query in queries:
        date = _search_google_news_date(query, contract_date, days_window)
        if date:
            return date
    return ""


def _search_google_news(query, days_back):
    """Search Google News RSS for recent articles matching query."""
    _rate_limit()
    try:
        url = "https://news.google.com/rss/search"
        params = {
            "q": f"{query} when:{days_back}d",
            "hl": "en-US",
            "gl": "US",
            "ceid": "US:en",
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            return False

        content = resp.text.lower()
        pr_sources = ["prnewswire", "businesswire", "globenewswire", "accesswire"]
        for source in pr_sources:
            if source in content:
                return True

        return False
    except Exception as e:
        log.warning(f"News search failed for '{query}': {e}")
        return False


def _search_google_news_date(query: str, contract_date: str, days_window: int) -> str:
    """Search Google News RSS and return the date of the first matching PR.

    Returns ISO date string or "".
    """
    _rate_limit()
    try:
        url = "https://news.google.com/rss/search"
        params = {
            "q": f"{query} when:{days_window * 2}d",
            "hl": "en-US",
            "gl": "US",
            "ceid": "US:en",
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            return ""

        content = resp.text
        content_lower = content.lower()
        pr_sources = ["prnewswire", "businesswire", "globenewswire", "accesswire"]

        # Check if any PR source is mentioned
        has_pr_source = any(s in content_lower for s in pr_sources)
        if not has_pr_source:
            return ""

        # Parse pubDate from RSS items
        # RSS items look like: <item>...<pubDate>Thu, 01 Jun 2023 12:00:00 GMT</pubDate>...</item>
        items = re.findall(r'<item>(.*?)</item>', content, re.DOTALL)
        contract_dt = datetime.strptime(contract_date, "%Y-%m-%d")
        lo = contract_dt - timedelta(days=1)
        hi = contract_dt + timedelta(days=days_window)

        earliest_date = None
        for item in items:
            item_lower = item.lower()
            # Check if this item mentions a PR source
            if not any(s in item_lower for s in pr_sources):
                continue
            # Extract pubDate
            pub_match = re.search(r'<pubDate>(.*?)</pubDate>', item)
            if not pub_match:
                continue
            try:
                pub_dt = parsedate_to_datetime(pub_match.group(1)).replace(tzinfo=None)
                if lo <= pub_dt <= hi:
                    pub_date_str = pub_dt.strftime("%Y-%m-%d")
                    if earliest_date is None or pub_date_str < earliest_date:
                        earliest_date = pub_date_str
            except Exception:
                continue

        return earliest_date or ""
    except Exception as e:
        log.warning(f"News date search failed for '{query}': {e}")
        return ""
