"""resolver/historical.py — Historical symbol resolution and security selection."""
from __future__ import annotations

import logging
import re
import time
from datetime import date, timedelta
from typing import Any

from resolver.models import (
    HistoricalSymbolDecision, SecuritySelectionDecision,
    EntityResolutionDecision, ReferenceHandles, ResolverConfig,
    PRIMARY_HISTORICAL_FORMS, SECONDARY_HISTORICAL_FORMS,
    US_EXCHANGE_CODES_ALLOWED, DISALLOWED_SECURITY_TYPES,
    DEFAULT_HTTP_HEADERS,
)
from resolver.normalize import normalize_cik, historical_symbol_coverage_bucket, parse_date

log = logging.getLogger(__name__)

_EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"
_edgar_last = 0.0

def _edgar_throttle(rate: float = 0.12) -> None:
    global _edgar_last
    elapsed = time.time() - _edgar_last
    if elapsed < rate:
        time.sleep(rate - elapsed)
    _edgar_last = time.time()

# ── Filing locator ────────────────────────────────────────────────────────────

def get_filing_locator_for_cik(cik: str, references: ReferenceHandles, config) -> list[dict]:
    """
    Return list of filing dicts {form, date, accessionNumber} for a CIK.
    Uses SEC live API if not in local reference data.
    """
    cik_norm = normalize_cik(cik) or cik
    # Check local reference first
    filings_data = references.sec_issuer_master.get(cik_norm, {}).get("filings", {})
    if filings_data:
        return _flatten_filings(filings_data)
    # Live fallback
    if not config.source_policies.allow_sec_live_fallback:
        return []
    import requests
    _edgar_throttle()
    try:
        url  = _EDGAR_SUBMISSIONS.format(cik=cik_norm)
        resp = requests.get(url, headers={"User-Agent": config.runtime.user_agent, "Accept": "application/json"},
                            timeout=config.runtime.http_timeout_seconds)
        if resp.status_code != 200:
            return []
        data = resp.json()
        return _flatten_filings(data.get("filings", {}).get("recent", {}))
    except Exception as e:
        log.debug(f"Filing locator fetch failed for CIK {cik}: {e}")
        return []

def _flatten_filings(filings_data: dict) -> list[dict]:
    """Convert SEC submissions recent filings dict (parallel arrays) → list of dicts."""
    if not filings_data:
        return []
    forms       = filings_data.get("form", [])
    dates       = filings_data.get("filingDate", [])
    accessions  = filings_data.get("accessionNumber", [])
    result = []
    for i, form in enumerate(forms):
        filing_date = dates[i]  if i < len(dates)      else ""
        accession   = accessions[i] if i < len(accessions) else ""
        result.append({"form": form, "date": filing_date, "accessionNumber": accession})
    return result

def find_candidate_filings_for_date(
    cik: str,
    award_date: date | None,
    filing_locator: list[dict],
    config,
) -> list[dict]:
    """Return filings near the award date in priority order."""
    if not award_date:
        return []
    primary_forms   = config.historical.primary_forms
    secondary_forms = config.historical.secondary_forms
    max_back        = config.historical.max_days_back_for_primary_lookup
    max_back_fb     = config.historical.max_days_back_for_fallback_lookup

    primary   = []
    secondary = []
    for filing in filing_locator:
        form        = filing.get("form", "")
        filing_date = parse_date(filing.get("date"))
        if not filing_date:
            continue
        # Prefer filing on or before award date
        if filing_date > award_date:
            continue
        age_days = (award_date - filing_date).days
        if form in primary_forms and age_days <= max_back:
            primary.append((age_days, filing))
        elif form in secondary_forms and age_days <= max_back_fb:
            secondary.append((age_days, filing))

    # Sort: nearest prior filing first
    primary.sort(key=lambda x: x[0])
    secondary.sort(key=lambda x: x[0])
    ordered = [f for _, f in primary] + [f for _, f in secondary]
    return ordered

def choose_primary_historical_filing(filings: list[dict], award_date: date | None, config) -> dict | None:
    """Return the best filing for historical symbol extraction."""
    if not filings:
        return None
    # Prefer 10-K/10-Q/20-F/40-F; take 8-K only if nothing else
    for preferred_form in config.historical.primary_forms:
        for f in filings:
            if f.get("form") == preferred_form:
                return f
    return filings[0] if filings else None

# ── Symbol extraction from filing ────────────────────────────────────────────

def extract_trading_symbol_from_filing(filing_record: dict, cik: str, config) -> dict | None:
    """
    Extract ticker symbol from an SEC filing's cover page / XBRL.
    Uses the SEC EDGAR XBRL API for structured data.
    """
    if not filing_record or not cik:
        return None
    accession = filing_record.get("accessionNumber", "")
    if not accession:
        return None
    accession_clean = accession.replace("-", "")
    # Try XBRL cover page facts
    symbol = _extract_symbol_from_cover_page_facts(cik, accession_clean, config)
    if symbol:
        return {"symbol": symbol, "source": "xbrl_cover_page", "accession": accession}
    # Fallback: parse filing index for DEF 14A / submission ticker
    return None

def _extract_symbol_from_cover_page_facts(cik: str, accession_clean: str, config) -> str | None:
    """Hit SEC XBRL API for TradingSymbol cover-page fact."""
    import requests
    cik_norm = normalize_cik(cik) or cik
    url = (
        f"https://data.sec.gov/api/xbrl/companyconcept/"
        f"CIK{cik_norm}/dei/TradingSymbol.json"
    )
    _edgar_throttle()
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": config.runtime.user_agent, "Accept": "application/json"},
            timeout=config.runtime.http_timeout_seconds,
        )
        if resp.status_code != 200:
            return None
        data  = resp.json()
        units = data.get("units", {})
        facts = units.get("USD") or units.get("pure") or next(iter(units.values()), [])
        if not facts:
            # TradingSymbol is a string fact, not numeric
            facts = data.get("facts", [])
        # TradingSymbol is under a different key
        # Try the direct string key
        if not isinstance(facts, list):
            return None
        # Sort by end date descending and pick the most recent
        dated_facts = [(f.get("end", ""), f.get("val")) for f in facts if f.get("val")]
        if not dated_facts:
            return None
        dated_facts.sort(reverse=True)
        return str(dated_facts[0][1]).upper().strip() or None
    except Exception as e:
        log.debug(f"XBRL TradingSymbol fetch failed for CIK {cik}: {e}")
        return None

def _extract_trading_symbol_from_dei_api(cik: str, config) -> str | None:
    """Alt approach: SEC DEI facts for TradingSymbol as string."""
    import requests
    cik_norm = normalize_cik(cik) or cik
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_norm}.json"
    _edgar_throttle()
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": config.runtime.user_agent, "Accept": "application/json"},
            timeout=config.runtime.http_timeout_seconds,
        )
        if resp.status_code != 200:
            return None
        data  = resp.json()
        facts = data.get("facts", {})
        dei   = facts.get("dei", {})
        ts    = dei.get("TradingSymbol", {})
        units = ts.get("units", {})
        # String values are under a str-typed key; try common ones
        for key in ("", "USD", "pure", "shares"):
            vals = units.get(key, [])
            if vals:
                vals.sort(key=lambda x: x.get("end", ""), reverse=True)
                sym = str(vals[0].get("val", "")).upper().strip()
                if sym:
                    return sym
        # Some filings put it directly
        if "value" in ts:
            return str(ts["value"]).upper().strip() or None
    except Exception as e:
        log.debug(f"DEI TradingSymbol fetch failed for CIK {cik}: {e}")
    return None

def extract_exchange_from_filing(filing_record: dict, cik: str, config) -> dict | None:
    """Extract exchange info from the filing (best-effort from XBRL or submissions)."""
    import requests
    cik_norm = normalize_cik(cik) or cik
    _edgar_throttle()
    try:
        url  = _EDGAR_SUBMISSIONS.format(cik=cik_norm)
        resp = requests.get(
            url,
            headers={"User-Agent": config.runtime.user_agent, "Accept": "application/json"},
            timeout=config.runtime.http_timeout_seconds,
        )
        if resp.status_code == 200:
            data      = resp.json()
            exchanges = data.get("exchanges", [])
            tickers   = data.get("tickers", [])
            if tickers and exchanges:
                return {"exchange": exchanges[0], "ticker": tickers[0], "source": "sec_submissions"}
    except Exception as e:
        log.debug(f"Exchange extraction failed for CIK {cik}: {e}")
    return None

def score_historical_symbol_evidence(symbol_record: dict, award_date: date | None, config) -> float:
    """Score confidence of a historical symbol based on source and date bucket."""
    if not symbol_record or not symbol_record.get("symbol"):
        return 0.0
    source = symbol_record.get("source", "")
    bucket = historical_symbol_coverage_bucket(award_date)
    base   = 50.0
    if source == "xbrl_cover_page":
        base = 70.0
    elif source == "sec_submissions":
        base = 60.0
    elif source == "dei_api":
        base = 65.0
    # Adjust by date bucket
    if bucket == "post_2021_06_15":
        base += 20.0
    elif bucket == "2019_06_15_to_2021_06_14":
        base += 5.0
    elif bucket == "pre_2019_06_15":
        base -= 20.0
    return min(100.0, base)

# ── Symbol timeline ───────────────────────────────────────────────────────────

def build_symbol_timeline_for_issuer(cik: str, references: ReferenceHandles, config) -> list[dict]:
    """Build a time-ordered list of {date, symbol, exchange, source} for an issuer."""
    filing_locator = get_filing_locator_for_cik(cik, references, config)
    timeline = []
    # Use SEC submissions tickers as the primary stable point
    issuer = references.sec_issuer_master.get(normalize_cik(cik) or cik, {})
    tickers   = issuer.get("tickers",   [])
    exchanges = issuer.get("exchanges", [])
    if tickers:
        timeline.append({
            "date":     "current",
            "symbol":   tickers[0].upper(),
            "exchange": exchanges[0] if exchanges else "",
            "source":   "sec_submissions_current",
        })
    return timeline

def lookup_symbol_on_date(
    cik: str,
    award_date: date | None,
    references: ReferenceHandles,
    config,
) -> HistoricalSymbolDecision:
    """Find the ticker symbol on the given award date for a CIK."""
    if not cik:
        return HistoricalSymbolDecision(status="historical_symbol_unavailable")

    bucket = historical_symbol_coverage_bucket(award_date)

    # Pre-2019 with auto-resolve disabled → return unavailable
    if (bucket == "pre_2019_06_15"
            and not config.historical.pre_2019_auto_resolve_allowed):
        return HistoricalSymbolDecision(
            status="historical_symbol_low_confidence",
            evidence_json={"reason": "pre_2019_auto_resolve_disabled"},
        )

    # Step 1: try XBRL DEI for current symbol (most accurate for modern filings)
    sym_from_dei = _extract_trading_symbol_from_dei_api(cik, config)
    if sym_from_dei:
        score = score_historical_symbol_evidence(
            {"symbol": sym_from_dei, "source": "dei_api"}, award_date, config
        )
        if score >= config.historical.historical_symbol_min_score if hasattr(config.historical, "historical_symbol_min_score") else 30.0:
            exch_info = extract_exchange_from_filing({}, cik, config)
            return HistoricalSymbolDecision(
                status    = "resolved_symbol",
                symbol    = sym_from_dei,
                exchange  = exch_info.get("exchange") if exch_info else None,
                source    = "dei_api",
                score     = score,
                evidence_json = {"cik": cik, "bucket": bucket},
            )

    # Step 2: Find the best filing for this award date
    filing_locator = get_filing_locator_for_cik(cik, references, config)
    candidate_filings = find_candidate_filings_for_date(cik, award_date, filing_locator, config)
    primary_filing    = choose_primary_historical_filing(candidate_filings, award_date, config)
    if primary_filing:
        sym_record = extract_trading_symbol_from_filing(primary_filing, cik, config)
        if sym_record and sym_record.get("symbol"):
            score = score_historical_symbol_evidence(sym_record, award_date, config)
            exch_info = extract_exchange_from_filing(primary_filing, cik, config)
            return HistoricalSymbolDecision(
                status    = "resolved_symbol",
                symbol    = sym_record["symbol"],
                exchange  = exch_info.get("exchange") if exch_info else None,
                source    = sym_record.get("source", "xbrl"),
                score     = score,
                evidence_json = {
                    "cik":       cik,
                    "accession": primary_filing.get("accessionNumber"),
                    "form":      primary_filing.get("form"),
                    "bucket":    bucket,
                },
            )

    # Step 3: Fallback to SEC submissions current ticker (may not be historical)
    issuer = references.sec_issuer_master.get(normalize_cik(cik) or cik, {})
    tickers = issuer.get("tickers", [])
    if tickers:
        sym = tickers[0].upper()
        score = score_historical_symbol_evidence(
            {"symbol": sym, "source": "sec_submissions"}, award_date, config
        )
        if score >= 40.0:
            exchanges = issuer.get("exchanges", [])
            return HistoricalSymbolDecision(
                status    = "resolved_symbol",
                symbol    = sym,
                exchange  = exchanges[0] if exchanges else None,
                source    = "sec_submissions_fallback",
                score     = score,
                evidence_json = {"cik": cik, "bucket": bucket, "note": "current_ticker_used_as_proxy"},
            )

    return HistoricalSymbolDecision(
        status="historical_symbol_unavailable",
        evidence_json={"cik": cik, "bucket": bucket},
    )

def resolve_historical_ticker(
    cik: str | None,
    lei: str | None,
    award_date: date | None,
    references: ReferenceHandles,
    config,
    known_alias_ticker: str | None = None,
) -> HistoricalSymbolDecision:
    """
    Top-level historical ticker resolution.
    Known-alias shortcuts skip all the filing lookups.
    """
    # If resolved via known alias, return immediately
    if known_alias_ticker:
        import yfinance as yf
        return HistoricalSymbolDecision(
            status   = "resolved_symbol",
            symbol   = known_alias_ticker,
            source   = "known_alias",
            score    = 100.0,
            evidence_json = {},
        )

    if not cik:
        # Try LEI→OpenFIGI path if we have a LEI
        if lei and config.source_policies.allow_live_openfigi:
            ticker = _resolve_via_lei(lei, config)
            if ticker:
                return HistoricalSymbolDecision(
                    status   = "resolved_symbol",
                    symbol   = ticker,
                    source   = "lei_openfigi",
                    score    = 60.0,
                )
        return HistoricalSymbolDecision(status="historical_symbol_unavailable",
                                        evidence_json={"reason": "no_cik"})

    return lookup_symbol_on_date(cik, award_date, references, config)

def _resolve_via_lei(lei: str, config) -> str | None:
    """LEI → OpenFIGI → US common stock ticker."""
    from resolver.ingest import openfigi_lei_to_ticker
    return openfigi_lei_to_ticker(lei, config)

# ── Security selection ────────────────────────────────────────────────────────

def select_us_tradable_security(
    issuer_decision:   EntityResolutionDecision,
    historical_decision: HistoricalSymbolDecision,
    references:        ReferenceHandles,
    config,
) -> SecuritySelectionDecision:
    """
    Final stage: pick one US-tradable security or return null decision.
    Sources in priority order:
      1. Known alias ticker (already in issuer_decision evidence)
      2. Historical symbol from filing
      3. Current SEC submissions ticker
      4. OpenFIGI LEI→ticker
    """
    # Known alias fast path
    if issuer_decision.resolution_path == "known_alias":
        ticker = issuer_decision.evidence_json.get("ticker")
        if ticker:
            return SecuritySelectionDecision(
                status="resolved_security",
                selected_ticker=ticker,
                selected_exchange=None,
                selected_security_type="Common Stock",
                is_adr=False,
                source="known_alias",
                score=100.0,
            )

    # Historical symbol from filing
    if historical_decision and historical_decision.status == "resolved_symbol":
        sym    = historical_decision.symbol
        exch   = historical_decision.exchange or ""
        is_valid, is_adr = _validate_us_security(sym, exch, config)
        if is_valid:
            return SecuritySelectionDecision(
                status="resolved_security",
                selected_ticker=sym,
                selected_exchange=exch or None,
                selected_security_type="ADR" if is_adr else "Common Stock",
                is_adr=is_adr,
                source=historical_decision.source,
                score=historical_decision.score or 60.0,
            )

    # Try current SEC submissions as a last resort
    cik = issuer_decision.matched_cik
    if cik:
        cik_norm = normalize_cik(cik) or cik
        issuer   = references.sec_issuer_master.get(cik_norm, {})
        tickers  = issuer.get("tickers", [])
        exchanges = issuer.get("exchanges", [])
        for i, ticker in enumerate(tickers):
            exch = exchanges[i] if i < len(exchanges) else ""
            is_valid, is_adr = _validate_us_security(ticker, exch, config)
            if is_valid:
                return SecuritySelectionDecision(
                    status="resolved_security",
                    selected_ticker=ticker.upper(),
                    selected_exchange=exch or None,
                    selected_security_type="ADR" if is_adr else "Common Stock",
                    is_adr=is_adr,
                    source="sec_submissions_current",
                    score=50.0,
                )

    # OpenFIGI via LEI
    lei = issuer_decision.matched_lei
    if lei and config.source_policies.allow_live_openfigi:
        ticker = _resolve_via_lei(lei, config)
        if ticker:
            return SecuritySelectionDecision(
                status="resolved_security",
                selected_ticker=ticker,
                selected_exchange=None,
                selected_security_type="Common Stock",
                is_adr=False,
                source="openfigi_lei",
                score=55.0,
            )

    # Try yfinance as absolute last resort to verify ticker exists
    if historical_decision and historical_decision.symbol:
        sym = historical_decision.symbol
        try:
            import yfinance as yf
            info = yf.Ticker(sym).fast_info
            mcap = info.market_cap or 0
            if mcap > 0:
                return SecuritySelectionDecision(
                    status="resolved_security",
                    selected_ticker=sym,
                    selected_exchange=getattr(info, "exchange", None),
                    selected_security_type="Common Stock",
                    is_adr=False,
                    source="yfinance_verification",
                    score=45.0,
                )
        except Exception:
            pass

    return SecuritySelectionDecision(
        status="no_us_tradable_security",
        evidence_json={
            "cik": issuer_decision.matched_cik,
            "lei": issuer_decision.matched_lei,
            "historical_status": historical_decision.status if historical_decision else None,
        },
    )

def _validate_us_security(ticker: str | None, exchange: str | None, config) -> tuple[bool, bool]:
    """Returns (is_valid_us_tradable, is_adr)."""
    if not ticker:
        return False, False
    ticker = ticker.upper().strip()
    # Reject special chars indicating non-standard security
    if any(c in ticker for c in ["/", "+", "."]):
        return False, False
    is_adr  = False
    is_valid = True
    # OTC exclusion
    if not config.security_selection.exclude_otc_default:
        is_valid = True
    else:
        # If exchange known, check whitelist
        if exchange and exchange not in config.security_selection.us_exchange_whitelist:
            # Don't reject if exchange is blank (unknown)
            if exchange in ("OTC", "OTCMKTS", "PINK", "OB"):
                is_valid = False
    return is_valid, is_adr

def filter_candidate_securities(securities: list[dict], config) -> list[dict]:
    """Filter a list of security dicts to US-tradable non-disallowed types."""
    result = []
    for sec in securities:
        exch     = sec.get("exchange", "")
        sec_type = sec.get("security_type", "")
        if sec_type in config.security_selection.disallowed_security_types:
            continue
        if exch in ("OTC", "PINK", "OB") and config.security_selection.exclude_otc_default:
            continue
        result.append(sec)
    return result

def rank_candidate_securities(securities: list[dict], config) -> list[dict]:
    """Rank: common stock first, then ADR, then others."""
    def _rank(sec):
        t = sec.get("security_type", "")
        if "Common" in t:
            return 0
        if "ADR" in t or "ADS" in t:
            return 1
        return 2
    return sorted(securities, key=_rank)

# ── Market cap (yfinance cache) ───────────────────────────────────────────────

_mcap_mem_cache: dict[str, float] = {}

def get_market_cap(ticker: str, mcap_cache: dict) -> float:
    """Fetch current market cap for a ticker, using persistent + memory cache."""
    if ticker in mcap_cache:
        return float(mcap_cache[ticker])
    if ticker in _mcap_mem_cache:
        return _mcap_mem_cache[ticker]
    try:
        import yfinance as yf
        mcap = float(yf.Ticker(ticker).fast_info.market_cap or 0)
        mcap_cache[ticker]      = mcap
        _mcap_mem_cache[ticker] = mcap
        return mcap
    except Exception as e:
        log.debug(f"Market cap fetch failed for {ticker}: {e}")
        return 0.0
