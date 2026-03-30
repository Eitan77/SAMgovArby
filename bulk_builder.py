"""Build a compact training dataset from USASpending bulk data.

Steps:
  1. Page through USASpending API for all defense/tech NAICS awards in date range
  2. Resolve each unique company to a ticker + market cap (cached to disk)
  3. Keep only public companies under MAX_MARKET_CAP_FILTER
  4. Save a clean JSON dataset for fast offline backtesting

Usage:
    python bulk_builder.py --start 2022-01-01 --end 2023-12-31
    python bulk_builder.py --start 2021-01-01 --end 2023-12-31 --max-cap 1000000000
"""
import argparse
import json
import logging
import os
import sys
import time

import yfinance as yf

from edgar_client import search_company
from usaspending_poller import _parse_award

log = logging.getLogger("bulk_builder")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

DATASET_DIR = os.path.join(os.path.dirname(__file__), "datasets")
TICKER_CACHE_FILE = os.path.join(os.path.dirname(__file__), ".ticker_cache.json")

import requests
from rapidfuzz import fuzz, process
BASE_URL = "https://api.usaspending.gov/api/v2"
HEADERS = {"Content-Type": "application/json"}
PAGE_SIZE = 100
EDGAR_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
EDGAR_HEADERS = {"User-Agent": "SAMgovArby research@example.com"}

NAICS_CODES = [
    "336411", "336412", "336413", "336414", "336415", "336419",
    "334511", "334512", "334513", "334514", "334515", "334516",
    "334519", "334220", "334290", "334310", "334411", "334412",
    "541330", "541511", "541512", "541519", "541611", "541715",
    "518210", "927110", "928110", "928120",
]


def build_dataset(start_date: str, end_date: str,
                  max_cap: int = 1_000_000_000,
                  min_contract: int = 1_000_000):
    """Fetch, resolve, filter and save the training dataset."""
    os.makedirs(DATASET_DIR, exist_ok=True)

    # Load persistent ticker cache
    ticker_cache = _load_ticker_cache()
    log.info(f"Loaded {len(ticker_cache)} cached ticker lookups")

    # Step 1: Fetch all awards from USASpending
    log.info(f"Fetching awards {start_date} -> {end_date} from USASpending...")
    awards = _fetch_all_awards(start_date, end_date, min_contract)
    log.info(f"Fetched {len(awards)} raw awards")

    # Step 2: Load EDGAR public company list (one download, all public tickers)
    log.info("Loading EDGAR public company list...")
    edgar_map = _load_edgar_tickers()
    log.info(f"EDGAR has {len(edgar_map)} public companies")

    # Step 3: Get unique company names and match against EDGAR first
    companies = list({a["awardee_name"] for a in awards if a["awardee_name"]})
    log.info(f"Matching {len(companies)} unique companies against EDGAR...")

    resolved = 0
    private = 0
    too_large = 0

    for i, name in enumerate(companies):
        if name in ticker_cache:
            entry = ticker_cache[name]
            mc = entry.get("market_cap_current", 0)
            if entry.get("ticker") and mc > 0 and mc <= max_cap:
                resolved += 1
            elif mc > max_cap:
                too_large += 1
            else:
                private += 1
            continue

        # Fast local fuzzy match against EDGAR list first
        ticker = _fuzzy_match_edgar(name, edgar_map)
        if not ticker:
            # Not in EDGAR = private company, skip yfinance entirely
            ticker_cache[name] = {"ticker": None, "market_cap_current": 0}
            private += 1
        else:
            # Only hit yfinance for confirmed public companies
            market_cap_current = _get_market_cap(ticker)
            ticker_cache[name] = {"ticker": ticker, "market_cap_current": market_cap_current}
            if market_cap_current and market_cap_current <= max_cap:
                resolved += 1
            elif market_cap_current > max_cap:
                too_large += 1
            else:
                private += 1

        if (i + 1) % 100 == 0:
            pct = (i + 1) / len(companies) * 100
            log.info(f"  [{i+1}/{len(companies)} {pct:.0f}%] "
                     f"resolved={resolved} private={private} too_large={too_large}")
            _save_ticker_cache(ticker_cache)

    _save_ticker_cache(ticker_cache)
    log.info(f"Resolution complete: {resolved} tradeable under ${max_cap/1e6:.0f}M, "
             f"{private} private/no match, {too_large} over cap")

    # Step 4: Filter awards to public companies only.
    # NOTE: market_cap_current is today's cap — NOT the historical cap at award time.
    # We keep all public companies here; historical cap filtering happens downstream
    # in build_training_set.py which computes historical_market_cap_approx per award.
    filtered_awards = []
    for award in awards:
        name = award["awardee_name"]
        info = ticker_cache.get(name, {})
        ticker = info.get("ticker")
        market_cap_current = info.get("market_cap_current", 0)

        if not ticker:
            continue

        award["ticker"] = ticker
        award["market_cap_current"] = market_cap_current  # snapshot only — use for identity, not historical filter
        filtered_awards.append(award)

    log.info(f"Filtered to {len(filtered_awards)} awards from public companies under ${max_cap/1e6:.0f}M cap")

    # Step 5: Save dataset
    start_clean = start_date.replace("-", "")
    end_clean = end_date.replace("-", "")
    filename = f"awards_{start_clean}_{end_clean}_under{max_cap//1_000_000}M.json"
    filepath = os.path.join(DATASET_DIR, filename)

    with open(filepath, "w") as f:
        json.dump(filtered_awards, f, indent=2)

    log.info(f"Dataset saved: {filepath} ({len(filtered_awards)} awards, "
             f"{os.path.getsize(filepath) / 1024:.0f} KB)")

    # Print summary
    print("\n" + "=" * 55)
    print(f"  DATASET BUILD COMPLETE")
    print("=" * 55)
    print(f"  Date range      : {start_date} -> {end_date}")
    print(f"  Raw awards      : {len(awards):,}")
    print(f"  Companies found : {len(companies):,}")
    print(f"  Public <${max_cap/1e6:.0f}M : {resolved:,}")
    print(f"  Private         : {private:,}")
    print(f"  Over cap        : {too_large:,}")
    print(f"  Final awards    : {len(filtered_awards):,}")
    print(f"  Saved to        : {filepath}")
    print("=" * 55)

    return filepath


def _fetch_all_awards(start_date, end_date, min_contract):
    """Page through USASpending and return all matching awards."""
    awards = []
    page = 1
    last_id = None
    last_sort_value = None

    while True:
        payload = {
            "filters": {
                "time_period": [{"start_date": start_date, "end_date": end_date}],
                "award_type_codes": ["A", "B", "C", "D"],
                "naics_codes": NAICS_CODES,
                "award_amounts": [{"lower_bound": min_contract, "upper_bound": 500_000_000}],
            },
            "fields": [
                "Award ID", "Recipient Name", "Award Amount",
                "Start Date", "Awarding Agency Name",
                "Awarding Sub Agency Name", "NAICS Code",
                "NAICS Description", "Contract Award Type",
                "Type of Set Aside", "Period of Performance Start Date",
            ],
            "page": page,
            "limit": PAGE_SIZE,
            "sort": "Award Amount",
            "order": "asc",
        }

        if last_id and last_sort_value:
            payload["last_record_unique_id"] = last_id
            payload["last_record_sort_value"] = last_sort_value

        try:
            resp = requests.post(f"{BASE_URL}/search/spending_by_award/",
                                 json=payload, headers=HEADERS, timeout=30)
            if resp.status_code == 429:
                log.warning("Rate limited, waiting 15s")
                time.sleep(15)
                continue
            if resp.status_code != 200:
                log.error(f"API error {resp.status_code}: {resp.text[:200]}")
                break

            data = resp.json()
            results = data.get("results", [])
            if not results:
                break

            for r in results:
                award = _parse_award(r)
                if award:
                    awards.append(award)

            meta = data.get("page_metadata", {})
            last_id = meta.get("last_record_unique_id")
            last_sort_value = meta.get("last_record_sort_value")

            if (page % 10) == 0:
                log.info(f"  Fetched {len(awards)} awards so far (page {page})")

            if not meta.get("hasNext"):
                break

            page += 1
            time.sleep(0.3)

        except Exception as e:
            log.error(f"Fetch error: {e}")
            break

    return awards


def _load_edgar_tickers():
    """Download the SEC EDGAR company tickers JSON once.
    Returns dict: cleaned_name -> ticker
    """
    cache_file = os.path.join(os.path.dirname(__file__), ".edgar_tickers.json")

    # Use local copy if recent (less than 7 days old)
    if os.path.exists(cache_file):
        age_days = (time.time() - os.path.getmtime(cache_file)) / 86400
        if age_days < 7:
            with open(cache_file) as f:
                return json.load(f)

    log.info("Downloading EDGAR company tickers from SEC...")
    try:
        resp = requests.get(EDGAR_TICKERS_URL, headers=EDGAR_HEADERS, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        log.error(f"Failed to download EDGAR tickers: {e}")
        return {}

    # Build name -> ticker map with cleaned names
    edgar_map = {}
    for entry in raw.values():
        name = entry.get("title", "").strip().upper()
        ticker = entry.get("ticker", "").strip().upper()
        if name and ticker:
            edgar_map[name] = ticker

    with open(cache_file, "w") as f:
        json.dump(edgar_map, f)

    log.info(f"EDGAR ticker list saved ({len(edgar_map)} companies)")
    return edgar_map


def _fuzzy_match_edgar(company_name, edgar_map, threshold=75):
    """Fuzzy match a company name against the EDGAR public company list.
    Returns ticker string or None if no match above threshold.
    """
    name_upper = company_name.strip().upper()

    # Exact match first
    if name_upper in edgar_map:
        return edgar_map[name_upper]

    # Try stripping common suffixes
    for suffix in [", INC.", " INC", " CORP.", " CORP", " LLC", " LTD",
                   " LIMITED", " INCORPORATED", " CO.", " CO"]:
        stripped = name_upper.replace(suffix, "").strip()
        if stripped in edgar_map:
            return edgar_map[stripped]

    # Fuzzy match using rapidfuzz C extension (much faster than Python loop)
    result = process.extractOne(name_upper, edgar_map.keys(), scorer=fuzz.token_sort_ratio)
    if result and result[1] >= threshold:
        return edgar_map[result[0]]
    return None


def _get_market_cap(ticker):
    """Get CURRENT market cap from yfinance (used for identity check, not historical filtering)."""
    try:
        info = yf.Ticker(ticker).info
        return float(info.get("marketCap") or 0)
    except Exception:
        return 0.0


def _resolve(company_name):
    """Resolve company name to (ticker, market_cap). Returns (None, 0) if not found."""
    try:
        results = search_company(company_name)
        if not results:
            return None, 0

        for result in results:
            ticker = result.get("ticker", "")
            if not ticker:
                continue
            try:
                info = yf.Ticker(ticker).info
                mc = info.get("marketCap")
                if mc and mc > 0:
                    return ticker, float(mc)
            except Exception:
                continue

        return None, 0
    except Exception:
        return None, 0


def _load_ticker_cache():
    """Load persistent ticker cache from disk."""
    if os.path.exists(TICKER_CACHE_FILE):
        try:
            with open(TICKER_CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_ticker_cache(cache):
    """Save ticker cache to disk."""
    with open(TICKER_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build compact training dataset")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--max-cap", type=int, default=1_000_000_000,
                        help="Max market cap in dollars (default: $1B)")
    parser.add_argument("--min-contract", type=int, default=1_000_000,
                        help="Min contract value (default: $1M)")
    args = parser.parse_args()

    build_dataset(
        start_date=args.start,
        end_date=args.end,
        max_cap=args.max_cap,
        min_contract=args.min_contract,
    )
