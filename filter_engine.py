"""Step 2: Apply rejection filters to parsed contracts."""
import logging
import requests
import yfinance as yf
from config import MAX_MARKET_CAP, MIN_CONTRACT_VALUE
from edgar_client import search_company, has_recent_8k, has_dilutive_offering
from news_checker import has_press_release

log = logging.getLogger(__name__)

# In-process cache: company_name -> (ticker, market_cap, cik, edgar_results)
# Avoids redundant API calls when the same company appears in multiple awards.
_mcap_cache: dict = {}


def apply_filters(contract):
    """Run all 6 filters. Returns (passed: bool, reason: str, extra_data: dict).

    extra_data contains market_cap and cik if found, to avoid duplicate lookups.
    """
    extra = {}

    # Filter 1: IDIQ
    if contract.get("is_idiq"):
        return False, "IDIQ contract", extra

    # Filter 2: Minimum contract value
    award_amount = float(contract["award_amount"])
    if award_amount < MIN_CONTRACT_VALUE:
        return False, f"Contract value ${award_amount:,.0f} below ${MIN_CONTRACT_VALUE:,.0f} minimum", extra

    # Filter 3: Market cap check (requires ticker lookup)
    company_name = contract["awardee_name"]

    if company_name in _mcap_cache:
        ticker, market_cap, cik, edgar_results = _mcap_cache[company_name]
    else:
        ticker, market_cap, cik, edgar_results = _resolve_market_cap(company_name)
        _mcap_cache[company_name] = (ticker, market_cap, cik, edgar_results)

    if market_cap is None:
        return False, f"Could not determine market cap for '{company_name}'", extra

    extra["market_cap"] = market_cap
    extra["ticker"] = ticker
    extra["cik"] = cik
    extra["edgar_results"] = edgar_results

    if market_cap > MAX_MARKET_CAP:
        return False, f"Market cap ${market_cap:,.0f} exceeds ${MAX_MARKET_CAP:,.0f} limit", extra

    # Filter 4: Recent 8-K about this contract
    if cik and has_recent_8k(cik, days_back=7):
        return False, "Recent 8-K filing found (contract likely already disclosed)", extra

    # Filter 5: Simultaneous press release
    if has_press_release(company_name):
        return False, "Press release already exists", extra

    # Filter 6: Dilutive offering in progress
    if cik and has_dilutive_offering(cik, days_back=60):
        return False, "Dilutive equity offering (S-1/S-3) in progress", extra

    return True, "Passed all filters", extra


def _resolve_market_cap(company_name):
    """Return (ticker, market_cap, cik, edgar_results) or (None, None, None, []) on failure."""
    edgar_results = search_company(company_name)
    if not edgar_results:
        log.debug(f"No EDGAR match for '{company_name}'")
        return None, None, None, []

    for result in edgar_results:
        t = result.get("ticker", "")
        if not t:
            continue
        try:
            info = yf.Ticker(t).info
            mc = info.get("marketCap")
            if mc and mc > 0:
                return t, float(mc), result.get("cik"), edgar_results
        except requests.exceptions.ConnectionError as e:
            log.warning(f"Network error fetching market cap for {t}: {e}")
        except requests.exceptions.Timeout as e:
            log.warning(f"Timeout fetching market cap for {t}: {e}")
        except Exception as e:
            log.warning(f"Unexpected error fetching market cap for {t}: {type(e).__name__}: {e}")

    return None, None, None, edgar_results
