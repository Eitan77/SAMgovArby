"""Multi-stage ticker resolver for SAM.gov awardee names.

Strategy:
  0. Non-public entity detection (universities, gov agencies, non-profits)
  1. Exact / normalized match against local EDGAR company_tickers map
  2. Validate candidate via SEC submissions (current name + former names)
  3. Substring match (catch subsidiaries like "NORTHROP GRUMMAN SYSTEMS CORP")
  4. Fuzzy match (≥80-85%) + validation
  5. Parent company escalation (use USASpending parent_recipient_name)

Uses sec-cik-mapper for broader EDGAR coverage (~10K+ tickers vs ~8K).
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
from config import EDGAR_RATE_LIMIT, EDGAR_USER_AGENT
from cage_resolver import CageResolver
from lei_resolver import LeiResolver

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
    "ENTERPRISES", "GLOBAL", "USA", "US", "DBA",
    # State of incorporation suffixes from SEC EDGAR names
    "DE", "MD", "NV", "NY", "VA", "CA", "TX", "FL", "PA", "OH",
    "WA", "GA", "MA", "IL", "NJ", "CT", "AZ", "CO", "MN",
}

# SEC EDGAR endpoints
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
EDGAR_HEADERS = {"User-Agent": EDGAR_USER_AGENT, "Accept": "application/json"}

_edgar_last = 0.0
_MCAP_CACHE_WRITE_BATCH = 50  # write mcap cache to disk every N new entries


def _edgar_throttle():
    global _edgar_last
    elapsed = time.time() - _edgar_last
    if elapsed < EDGAR_RATE_LIMIT:
        time.sleep(EDGAR_RATE_LIMIT - elapsed)
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


# ─── EDGAR map loader (used by singleton when no map provided) ────────────────

_EDGAR_MAP_FILE = os.path.join(os.path.dirname(__file__), ".edgar_tickers.json")
_EDGAR_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def _load_edgar_map_default() -> dict:
    """Load the EDGAR company→ticker map from local cache or sec-cik-mapper.

    Uses sec-cik-mapper (13K+ entries) as primary source, falls back to
    SEC company_tickers.json download if unavailable.
    """
    if os.path.exists(_EDGAR_MAP_FILE):
        age_days = (time.time() - os.path.getmtime(_EDGAR_MAP_FILE)) / 86400
        if age_days < 7:
            try:
                with open(_EDGAR_MAP_FILE) as f:
                    data = json.load(f)
                migrated = {}
                for name, val in data.items():
                    if isinstance(val, str):
                        migrated[name] = {"ticker": val, "cik": ""}
                    else:
                        migrated[name] = val
                return migrated
            except Exception as e:
                log.warning(f"Could not load EDGAR map from cache: {e}")

    # Primary: sec-cik-mapper (broader coverage)
    try:
        from sec_cik_mapper import StockMapper
        mapper = StockMapper()
        edgar_map = {}
        for ticker, name in mapper.ticker_to_company_name.items():
            cik = str(mapper.ticker_to_cik.get(ticker, ""))
            name_upper = name.strip().upper()
            ticker_upper = ticker.strip().upper()
            if name_upper and ticker_upper:
                edgar_map[name_upper] = {"ticker": ticker_upper, "cik": cik}
        with open(_EDGAR_MAP_FILE, "w") as f:
            json.dump(edgar_map, f)
        log.info(f"EDGAR map via sec-cik-mapper: {len(edgar_map):,} companies")
        return edgar_map
    except Exception as e:
        log.warning(f"sec-cik-mapper failed ({e}), falling back to SEC download")

    # Fallback: direct SEC download
    log.info("Downloading EDGAR company tickers from SEC...")
    try:
        resp = requests.get(_EDGAR_TICKERS_URL, headers=EDGAR_HEADERS, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        edgar_map = {}
        for entry in raw.values():
            name = entry.get("title", "").strip().upper()
            ticker = entry.get("ticker", "").strip().upper()
            cik = str(entry.get("cik_str", ""))
            if name and ticker:
                edgar_map[name] = {"ticker": ticker, "cik": cik}
        with open(_EDGAR_MAP_FILE, "w") as f:
            json.dump(edgar_map, f)
        log.info(f"EDGAR map downloaded: {len(edgar_map):,} companies")
        return edgar_map
    except Exception as e:
        log.error(f"Failed to download EDGAR tickers: {e}")
        return {}


# ─── Main resolver ────────────────────────────────────────────────────────────

class TickerResolverV3:
    """Multi-stage resolver: CAGE→LEI, exact, validate, fuzzy+validate, non-public detect."""

    def __init__(self, edgar_map: dict | None = None, cache_path: str = ".ticker_cache_v2.json",
                 mcap_cache_path: str = ".mcap_cache.json"):
        if edgar_map is None:
            # Lazy-load the EDGAR map when not provided (module-level singleton use)
            edgar_map = _load_edgar_map_default()
        self.edgar_map = edgar_map  # {COMPANY_NAME -> {ticker, cik}}
        self.cache_path = cache_path
        self.mcap_cache_path = mcap_cache_path
        self.cache: dict = {}
        self.mcap_cache: dict = {}  # ticker -> market_cap (persistent)
        self._mcap_unsaved = 0      # count of unsaved mcap entries
        self.cage_resolver = CageResolver()
        self.lei_resolver = LeiResolver()
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

        # Pre-build substring index: stripped name → (original_name, entry)
        # Only names with ≥2 words (to avoid false positives on "APPLE", "ORACLE" etc.)
        self._substr_candidates: list[tuple[str, str, dict]] = []
        for ename, entry in edgar_map.items():
            s = _strip_suffixes(_normalize(ename))
            if s and len(s.split()) >= 2:
                self._substr_candidates.append((s, ename, entry))

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
        # Also flush any pending mcap entries
        if self._mcap_unsaved > 0:
            self.save_mcap_cache()
            self._mcap_unsaved = 0

    def save_mcap_cache(self):
        """Save market cap cache to disk."""
        with open(self.mcap_cache_path, "w") as f:
            json.dump(self.mcap_cache, f, indent=2)

    def resolve(self, awardee_name: str, parent_name: str = "", cage_code: str = "") -> dict:
        """Resolve an awardee name to ticker/CIK.

        Args:
            awardee_name: Direct company name from award
            parent_name: Parent company name from USASpending (fallback)
            cage_code: Commercial and Government Entity code (optional, SAM.gov only)

        Returns cache entry dict with keys: resolved_ticker, resolved_cik,
        evidence_type, confidence, rejection_reason, market_cap_current, audit_trail.
        """
        if awardee_name in self.cache:
            return self.cache[awardee_name]

        # Tier 1: CAGE code → LEI → OpenFIGI (federal identifiers)
        if cage_code:
            cage_result = self._resolve_via_cage(awardee_name, cage_code)
            if cage_result.get("resolved_ticker"):
                self.cache[awardee_name] = cage_result
                return cage_result

        result = self._resolve_name(awardee_name)
        if result.get("resolved_ticker"):
            self.cache[awardee_name] = result
            return result

        # Stage 5: parent company escalation
        if parent_name and parent_name.strip().upper() != awardee_name.strip().upper():
            parent_result = self._resolve_name(parent_name)
            if parent_result.get("resolved_ticker"):
                # Re-tag evidence to show this came via parent escalation
                parent_result["evidence_type"] = f"parent_{parent_result['evidence_type']}"
                parent_result["original_name"] = awardee_name
                self.cache[awardee_name] = parent_result
                return parent_result

        # No match found
        norm = _normalize(awardee_name)
        result = self._make_result(awardee_name, norm, None, None, "none", "unresolved", "no_match")
        self.cache[awardee_name] = result
        return result

    def _resolve_via_cage(self, awardee_name: str, cage_code: str) -> dict:
        """Attempt resolution via CAGE → LEI → OpenFIGI."""
        # Step 1: CAGE → LEI
        cage_result = self.cage_resolver.resolve_cage(cage_code)
        if not cage_result.get("lei"):
            return {}

        lei = cage_result["lei"]

        # Step 2: LEI → ticker
        lei_result = self.lei_resolver.resolve_lei(lei)
        if not lei_result.get("ticker"):
            return {}

        ticker = lei_result["ticker"]
        cik = lei_result.get("cik", "")
        mc = self._get_market_cap(ticker)

        norm = _normalize(awardee_name)
        audit_trail = [
            {"path": "cage_to_lei", "source": "GLEIF", "lei": lei, "confidence": cage_result.get("confidence")},
            {"path": "lei_to_ticker", "source": "OpenFIGI", "ticker": ticker, "confidence": lei_result.get("confidence")}
        ]

        return self._make_result(awardee_name, norm, ticker, cik, "high",
                                "cage_lei_openfigi", None, mc, audit_trail)

    def _resolve_name(self, name: str) -> dict:
        """Core resolution logic for a single name (no parent escalation)."""
        norm = _normalize(name)
        stripped = _strip_suffixes(norm)

        # Stage 0 (cheap): non-public entity detection
        for pat in _NON_PUBLIC_RE:
            if pat.search(name):
                return self._make_result(name, norm, None, None,
                                         "none", "unresolved", "non_public_entity")

        # Stage 1: exact match against EDGAR map
        candidate = None
        for key in [name.strip().upper(), norm, stripped]:
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
                    return self._make_result(name, norm, candidate["ticker"],
                                             cik, confidence, evidence, None, mc)
            else:
                mc = self._get_market_cap(candidate["ticker"])
                if mc > 0:
                    return self._make_result(name, norm, candidate["ticker"],
                                             "", "medium", "exact_edgar_map_unverified", None, mc)

        # Stage 3: substring match (catch subsidiaries)
        sub_result = self._substring_match(name, norm, stripped)
        if sub_result:
            return sub_result

        # Stage 4: fuzzy match + validate
        # Lower threshold for short names (≤3 words are more sensitive)
        min_score = 80 if len(stripped.split()) <= 3 else 85
        results = process.extract(norm, self._edgar_names, scorer=fuzz.token_sort_ratio, limit=5)
        for match_name, score, _ in results:
            if score < min_score:
                break
            entry = self.edgar_map[match_name]
            cik = entry.get("cik", "")

            if score >= 95:
                mc = self._get_market_cap(entry["ticker"])
                if mc > 0:
                    return self._make_result(name, norm, entry["ticker"],
                                             cik, "medium_high", "fuzzy_very_high", None, mc)

            if cik:
                valid, confidence, evidence = _validate_candidate(cik, norm, stripped)
                if valid:
                    mc = self._get_market_cap(entry["ticker"])
                    return self._make_result(name, norm, entry["ticker"],
                                             cik, confidence, f"fuzzy_{evidence}", None, mc)

        # No match
        return self._make_result(name, norm, None, None, "none", "unresolved", "no_match")

    def _substring_match(self, original: str, norm: str, stripped: str) -> dict | None:
        """Check if an EDGAR company name is a significant substring of the awardee name.

        Catches cases like "NORTHROP GRUMMAN SYSTEMS CORP" → "NORTHROP GRUMMAN"
        """
        best_match = None
        best_len = 0
        for edgar_stripped, edgar_orig, entry in self._substr_candidates:
            # Check both directions:
            # 1. EDGAR name contained in awardee: "NORTHROP GRUMMAN" in "NORTHROP GRUMMAN SYSTEMS"
            # 2. Awardee contained in EDGAR name: "NORTHROP GRUMMAN" in "NORTHROP GRUMMAN CORP DE"
            match_len = 0
            if edgar_stripped in stripped:
                match_len = len(edgar_stripped)
            elif stripped in edgar_stripped:
                match_len = len(stripped)
            if match_len > best_len:
                best_match = (edgar_stripped, edgar_orig, entry)
                best_len = match_len

        if not best_match or best_len < 10:  # min 10 chars to avoid false positives
            return None

        edgar_stripped, edgar_orig, entry = best_match
        cik = entry.get("cik", "")

        # For substring matches, we validate that the ticker is real (has mcap)
        # rather than requiring the SEC name to match the awardee name exactly
        # (they won't match — that's the whole point of substring matching).
        # Extra safety: require the overlap to be ≥60% of the longer name.
        longer = max(len(stripped), len(edgar_stripped))
        overlap_pct = best_len / longer if longer else 0
        if overlap_pct < 0.6:
            return None

        mc = self._get_market_cap(entry["ticker"])
        if mc > 0:
            return self._make_result(original, norm, entry["ticker"],
                                     cik, "medium", "substring_match", None, mc)
        return None

    def _get_market_cap(self, ticker: str) -> float:
        """Get market cap for ticker, using persistent cache to minimize API calls."""
        if ticker in self.mcap_cache:
            return float(self.mcap_cache[ticker])

        try:
            mcap = float(yf.Ticker(ticker).fast_info.market_cap or 0)
            self.mcap_cache[ticker] = mcap
            self._mcap_unsaved += 1
            # Batch-write: only flush to disk every N new entries to reduce I/O
            if self._mcap_unsaved >= _MCAP_CACHE_WRITE_BATCH:
                self.save_mcap_cache()
                self._mcap_unsaved = 0
            return mcap
        except Exception as e:
            log.debug(f"Market cap fetch failed for {ticker}: {e}")
            return 0.0

    @staticmethod
    def _make_result(original, normalized, ticker, cik, confidence,
                     evidence_type, rejection_reason=None, market_cap=0.0, audit_trail=None):
        return {
            "original_name": original,
            "normalized_name": normalized,
            "resolved_ticker": ticker,
            "resolved_cik": cik or "",
            "evidence_type": evidence_type,
            "confidence": confidence,
            "rejection_reason": rejection_reason,
            "market_cap_current": market_cap or 0.0,
            "audit_trail": audit_trail or [],
            "last_verified": datetime.utcnow().isoformat(),
        }


# ─── Module-level singleton wrapper ──────────────────────────────────────────
_resolver_instance: "TickerResolverV3 | None" = None


def resolve_ticker(awardee_name: str, edgar_results=None, resolver: "TickerResolverV3 | None" = None, cage_code: str = "") -> "tuple[str | None, str]":
    """Resolve awardee name → (ticker_or_None, confidence_str).

    Wraps TickerResolverV3 as a module-level function. Resolver is cached as a
    module singleton so the EDGAR map and cache are loaded only once per process.
    """
    global _resolver_instance
    if resolver is None:
        if _resolver_instance is None:
            _resolver_instance = TickerResolverV3()
        resolver = _resolver_instance
    result = resolver.resolve(awardee_name, cage_code=cage_code)
    return result.get("resolved_ticker"), result.get("confidence", "none")
