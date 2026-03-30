"""Multi-stage ticker resolver for SAM.gov awardee names.

Strategy:
  1. Exact / normalized match against local EDGAR company_tickers map
  2. Validate candidate via SEC submissions (current name + former names)
  3. Fuzzy match (≥85%) + validation
  4. Non-public entity detection (universities, gov agencies, non-profits)

Only accepts high-confidence matches. Unresolved entities are marked as such
rather than guessed.
"""
import json
import logging
import os
import re
import time
from datetime import datetime

import requests
import yfinance as yf
from rapidfuzz import fuzz, process

log = logging.getLogger(__name__)

# ─── Non-public entity patterns ──────────────────────────────────────────────
_NON_PUBLIC_PATTERNS = [
    r"\bUNIVERSIT",
    r"\bREGENTS\b",
    r"\bTRUSTEES\b",
    r"\bBOARD OF\b",
    r"\bNATIONAL LABORATOR",
    r"\bDEPARTMENT OF\b",
    r"\bBUREAU OF\b",
    r"\bFOUNDATION\b",
    r"\bINSTITUTE OF\b",
    r"\bAUTHORIT[YI]",
    r"\bTRIBAL\b",
    r"\bCOUNTY OF\b",
    r"\bCITY OF\b",
    r"\bSTATE OF\b",
    r"\bCOMMISSION\b",
    r"\bGOVERNMENT\b",
    r"\bMUNICIPAL",
    r"\bCOOPERATIVE\b",
    r"\bASSOCIATION OF\b",
    r"\bCONSORTIUM\b",
    r"\bJOINT VENTURE\b",
    r"\b[A-Z]+ JV\b",
    r"\bAJV\b",
    r"\bBATTELLE\b",
    r"\bSANDIA\b",
    r"\bBROOKHAVEN\b",
    r"\bFERMILAB\b",
]
_NON_PUBLIC_RE = [re.compile(p, re.IGNORECASE) for p in _NON_PUBLIC_PATTERNS]

# ─── Suffixes to strip ───────────────────────────────────────────────────────
_SUFFIX_WORDS = {
    "INC", "INCORPORATED", "CORP", "CORPORATION", "LLC", "LLP",
    "LTD", "LIMITED", "CO", "COMPANY", "LP", "HOLDINGS",
    "GROUP", "TECHNOLOGIES", "SOLUTIONS", "SYSTEMS", "SERVICES",
    "ENTERPRISES", "INTERNATIONAL", "GLOBAL", "USA", "US", "DBA",
}

# SEC EDGAR endpoints
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
EDGAR_HEADERS = {"User-Agent": "SAMgovArby research@example.com", "Accept": "application/json"}

_edgar_last = 0.0


def _edgar_throttle():
    global _edgar_last
    elapsed = time.time() - _edgar_last
    if elapsed < 0.12:
        time.sleep(0.12 - elapsed)
    _edgar_last = time.time()


# ─── Text normalization ──────────────────────────────────────────────────────

def _normalize(name: str) -> str:
    upper = name.strip().upper()
    upper = upper.replace("&", "AND")
    upper = re.sub(r'[^A-Z0-9 ]', '', upper)
    return re.sub(r' +', ' ', upper).strip()


def _strip_suffixes(name: str) -> str:
    words = name.split()
    while words and words[-1] in _SUFFIX_WORDS:
        words.pop()
    return " ".join(words)


# ─── SEC Submissions metadata ────────────────────────────────────────────────

def _fetch_submissions_metadata(cik: str) -> dict | None:
    """Fetch SEC submissions JSON and extract identity metadata."""
    if not cik:
        return None
    _edgar_throttle()
    try:
        url = EDGAR_SUBMISSIONS_URL.format(cik=str(cik).zfill(10))
        resp = requests.get(url, headers=EDGAR_HEADERS, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        return {
            "name": data.get("name", ""),
            "formerNames": data.get("formerNames", []),
            "tickers": data.get("tickers", []),
            "exchanges": data.get("exchanges", []),
            "entityType": data.get("entityType", ""),
            "sic": data.get("sic", ""),
            "sicDescription": data.get("sicDescription", ""),
        }
    except Exception as e:
        log.debug(f"Submissions fetch failed for CIK {cik}: {e}")
        return None


def _validate_candidate(candidate_cik: str, awardee_norm: str, awardee_stripped: str) -> tuple[bool, str, str]:
    """Validate a candidate CIK against SEC submissions.

    Returns (valid, confidence, evidence_type).
    """
    meta = _fetch_submissions_metadata(candidate_cik)
    if not meta:
        return False, "none", "validation_failed"

    sec_name_norm = _normalize(meta["name"])
    sec_name_stripped = _strip_suffixes(sec_name_norm)

    # Check current name (exact normalized)
    if awardee_norm == sec_name_norm or awardee_stripped == sec_name_stripped:
        return True, "high", "exact_sec_name"

    # Check current name (high fuzzy)
    score = fuzz.token_sort_ratio(awardee_stripped, sec_name_stripped)
    if score >= 90:
        return True, "high", "fuzzy_sec_name"

    # Check former names
    for fn in meta.get("formerNames", []):
        fn_norm = _normalize(fn.get("name", ""))
        fn_stripped = _strip_suffixes(fn_norm)
        if awardee_norm == fn_norm or awardee_stripped == fn_stripped:
            return True, "medium_high", "former_name_exact"
        score = fuzz.token_sort_ratio(awardee_stripped, fn_stripped)
        if score >= 85:
            return True, "medium_high", "former_name_fuzzy"

    # Has tickers at all?
    if not meta.get("tickers"):
        return False, "none", "no_tickers_on_file"

    return False, "low", "name_mismatch"


# ─── Main resolver ────────────────────────────────────────────────────────────

class TickerResolverV2:
    """Multi-stage resolver: exact → validate → fuzzy+validate → non-public detect."""

    def __init__(self, edgar_map: dict, cache_path: str = ".ticker_cache_v2.json",
                 mcap_cache_path: str = ".mcap_cache.json"):
        self.edgar_map = edgar_map  # {COMPANY_NAME -> {ticker, cik}}
        self.cache_path = cache_path
        self.mcap_cache_path = mcap_cache_path
        self.cache: dict = {}
        self.mcap_cache: dict = {}  # ticker -> market_cap (persistent)
        self._load_cache()
        self._load_mcap_cache()

        # Pre-build stripped lookup
        self._stripped_map: dict[str, tuple[str, dict]] = {}
        for ename, entry in edgar_map.items():
            s = _strip_suffixes(_normalize(ename))
            if s and s not in self._stripped_map:
                self._stripped_map[s] = (ename, entry)

        # Pre-build list for fuzzy matching
        self._edgar_names = list(edgar_map.keys())

    def _load_cache(self):
        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path) as f:
                    self.cache = json.load(f)
            except Exception:
                self.cache = {}

    def _load_mcap_cache(self):
        """Load market cap cache from disk."""
        if os.path.exists(self.mcap_cache_path):
            try:
                with open(self.mcap_cache_path) as f:
                    self.mcap_cache = json.load(f)
            except Exception:
                self.mcap_cache = {}

    def save_cache(self):
        with open(self.cache_path, "w") as f:
            json.dump(self.cache, f, indent=2)

    def save_mcap_cache(self):
        """Save market cap cache to disk."""
        with open(self.mcap_cache_path, "w") as f:
            json.dump(self.mcap_cache, f, indent=2)

    def resolve(self, awardee_name: str) -> dict:
        """Resolve an awardee name to ticker/CIK.

        Returns cache entry dict with keys: resolved_ticker, resolved_cik,
        evidence_type, confidence, rejection_reason, market_cap_current.
        """
        if awardee_name in self.cache:
            return self.cache[awardee_name]

        norm = _normalize(awardee_name)
        stripped = _strip_suffixes(norm)

        # Stage 4 first (cheap): non-public entity detection
        for pat in _NON_PUBLIC_RE:
            if pat.search(awardee_name):
                result = self._make_result(awardee_name, norm, None, None,
                                           "none", "unresolved", "non_public_entity")
                self.cache[awardee_name] = result
                return result

        # Stage 1: exact match against EDGAR map
        candidate = None
        for key in [awardee_name.strip().upper(), norm, stripped]:
            if key in self.edgar_map:
                candidate = self.edgar_map[key]
                break
        if not candidate and stripped in self._stripped_map:
            _, candidate = self._stripped_map[stripped]

        if candidate:
            # Stage 2: validate via SEC submissions
            cik = candidate.get("cik", "")
            if cik:
                valid, confidence, evidence = _validate_candidate(cik, norm, stripped)
                if valid:
                    mc = self._get_market_cap(candidate["ticker"])
                    result = self._make_result(awardee_name, norm, candidate["ticker"],
                                               cik, confidence, evidence, None, mc)
                    self.cache[awardee_name] = result
                    return result
            else:
                # No CIK in EDGAR map, do yfinance verification
                mc = self._get_market_cap(candidate["ticker"])
                if mc > 0:
                    result = self._make_result(awardee_name, norm, candidate["ticker"],
                                               "", "medium", "exact_edgar_map_unverified", None, mc)
                    self.cache[awardee_name] = result
                    return result

        # Stage 3: fuzzy match + validate
        results = process.extract(norm, self._edgar_names, scorer=fuzz.token_sort_ratio, limit=5)
        for match_name, score, _ in results:
            if score < 85:
                break
            entry = self.edgar_map[match_name]
            cik = entry.get("cik", "")

            if score >= 95:
                # Very high similarity — accept with medium confidence even without CIK validation
                mc = self._get_market_cap(entry["ticker"])
                if mc > 0:
                    result = self._make_result(awardee_name, norm, entry["ticker"],
                                               cik, "medium_high", "fuzzy_very_high", None, mc)
                    self.cache[awardee_name] = result
                    return result

            if cik:
                valid, confidence, evidence = _validate_candidate(cik, norm, stripped)
                if valid:
                    mc = self._get_market_cap(entry["ticker"])
                    result = self._make_result(awardee_name, norm, entry["ticker"],
                                               cik, confidence, f"fuzzy_{evidence}", None, mc)
                    self.cache[awardee_name] = result
                    return result

        # No match found
        result = self._make_result(awardee_name, norm, None, None, "none", "unresolved", "no_match")
        self.cache[awardee_name] = result
        return result

    def _get_market_cap(self, ticker: str) -> float:
        """Get market cap for ticker, using persistent cache to minimize API calls."""
        # Check cache first
        if ticker in self.mcap_cache:
            return float(self.mcap_cache[ticker])

        # Fetch from yfinance if not cached
        try:
            mcap = float(yf.Ticker(ticker).fast_info.market_cap or 0)
            self.mcap_cache[ticker] = mcap
            self.save_mcap_cache()  # Persist immediately
            return mcap
        except Exception:
            return 0.0

    @staticmethod
    def _make_result(original, normalized, ticker, cik, confidence,
                     evidence_type, rejection_reason=None, market_cap=0.0):
        return {
            "original_name": original,
            "normalized_name": normalized,
            "resolved_ticker": ticker,
            "resolved_cik": cik or "",
            "evidence_type": evidence_type,
            "confidence": confidence,
            "rejection_reason": rejection_reason,
            "market_cap_current": market_cap or 0.0,
            "last_verified": datetime.utcnow().isoformat(),
        }


# ─── Module-level singleton wrapper ──────────────────────────────────────────
_resolver_instance: "TickerResolverV2 | None" = None


def resolve_ticker(awardee_name: str, edgar_results=None,
                   resolver: "TickerResolverV2 | None" = None) -> "tuple[str | None, str]":
    """Resolve awardee name → (ticker_or_None, confidence_str).

    Wraps TickerResolverV2 as a module-level function. Resolver is cached as a
    module singleton so the EDGAR map and cache are loaded only once per process.
    """
    global _resolver_instance
    if resolver is None:
        if _resolver_instance is None:
            _resolver_instance = TickerResolverV2()
        resolver = _resolver_instance
    result = resolver.resolve(awardee_name)
    return result.get("resolved_ticker"), result.get("confidence", "none")
